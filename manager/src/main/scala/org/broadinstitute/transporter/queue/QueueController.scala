package org.broadinstitute.transporter.queue

import java.util.UUID

import cats.effect.IO
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.db.DbLogHandler
import org.broadinstitute.transporter.error.{
  InvalidQueueParameter,
  NoSuchQueue,
  QueueAlreadyExists
}
import org.broadinstitute.transporter.kafka.{KafkaAdminClient, TopicApi}
import org.broadinstitute.transporter.queue.api.{Queue, QueueParameters, QueueRequest}

/**
  * Component responsible for updating and retrieving queue-level information
  * for the Transporter system.
  *
  * @param dbClient client which can interact with Transporter's backing DB
  * @param kafkaClient client which can perform admin-level operations against
  *                    Transporter's Kafka cluster
  */
class QueueController(
  dbClient: Transactor[IO],
  kafkaClient: KafkaAdminClient,
  topicNames: UUID => (String, String, String) = TopicApi.queueTopics(_)
) {
  import org.broadinstitute.transporter.db.DoobieInstances._

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  private val queueColumns = List(
    "name",
    "request_topic",
    "progress_topic",
    "response_topic",
    "request_schema",
    "max_in_flight",
    "partition_count"
  )
  private val queueColsFragment = Fragment.const(queueColumns.mkString(", "))

  /** Fetch the unique ID of the queue resource with the given name, if one exists. */
  private def lookupQueueId(name: String): ConnectionIO[Option[UUID]] =
    sql"select id from queues where name = $name".query[UUID].option

  /**
    * Create and record a new queue resource matching the given request parameters.
    *
    * Queue creation touches both the DB and Kafka. To avoid getting the two systems
    * out-of-sync, this method:
    *
    *   1. Begins a DB transaction
    *   2. Creates a new row for the queue within the transaction
    *   3. Creates topics in Kafka for the queue, matching the DB info
    *   4. Completes the DB transaction.
    *
    * If an error occurs during 3, the transaction is rolled back.
    */
  def createQueue(request: QueueRequest): IO[Queue] =
    lookupQueueId(request.name).transact(dbClient).flatMap {
      case Some(existing) =>
        logger
          .warn(s"Caught attempt to double-create queue $existing")
          .flatMap(_ => IO.raiseError[Queue](QueueAlreadyExists(request.name)))
      case None =>
        val queueId = UUID.randomUUID()
        val (requestTopic, progressTopic, responseTopic) = topicNames(queueId)

        val insert = List(
          fr"insert into queues",
          Fragments.parentheses(fr"id," ++ queueColsFragment),
          fr"values",
          Fragments.parentheses {
            List(
              fr"$queueId",
              fr"${request.name}",
              fr"$requestTopic",
              fr"$progressTopic",
              fr"$responseTopic",
              fr"${request.schema}",
              fr"${request.maxConcurrentTransfers}",
              fr"${request.partitionCount}"
            ).intercalate(fr",")
          }
        ).combineAll

        val initTransaction = for {
          queue <- insert.update.withUniqueGeneratedKeys[Queue](queueColumns: _*)
          topicPartitions = List(requestTopic, progressTopic, responseTopic)
            .map(_ -> request.partitionCount)
          _ <- kafkaClient.createTopics(topicPartitions).to[ConnectionIO]
        } yield {
          queue
        }

        for {
          _ <- checkParameters(
            request.name,
            QueueParameters(
              schema = Some(request.schema),
              maxConcurrentTransfers = Some(request.maxConcurrentTransfers),
              partitionCount = Some(request.partitionCount)
            )
          )
          _ <- logger.info(s"Initializing resources for queue ${request.name}")
          newQueue <- initTransaction.transact(dbClient)
          _ <- logger.info(s"Successfully created queue ${request.name}")
        } yield {
          newQueue
        }
    }

  /** Check if user-defined parameters specified in a queue request are valid. */
  private def checkParameters(queueName: String, params: QueueParameters): IO[Unit] = {
    import QueueController._

    val checkConcurrency = params.maxConcurrentTransfers.fold(IO.unit) { concurrency =>
      IO.raiseError(
          InvalidQueueParameter(queueName, "Max concurrent requests must be non-negative")
        )
        .whenA(concurrency < 0)
    }

    val checkPartitions = params.partitionCount.fold(IO.unit) { partitions =>
      for {
        _ <- IO
          .raiseError(
            InvalidQueueParameter(
              queueName,
              s"Partition count must be between $MinPartitions and $MaxPartitions"
            )
          )
          .whenA(partitions < MinPartitions || partitions > MaxPartitions)
        existingPartitions <- sql"select partition_count from queues where name = $queueName"
          .query[Int]
          .option
          .transact(dbClient)
        _ <- existingPartitions.fold(IO.unit) { existing =>
          IO.raiseError(
              InvalidQueueParameter(
                queueName,
                s"Cannot decrease partition count from $existing"
              )
            )
            .whenA(existing > partitions)
        }
      } yield ()
    }

    (checkConcurrency, checkPartitions).tupled.void
  }

  /** Update the user-defined parameters of an existing queue resource. */
  def patchQueue(name: String, newParameters: QueueParameters): IO[Queue] = {
    val updates = Fragments.setOpt(
      newParameters.schema.map(s => fr"request_schema = $s"),
      newParameters.maxConcurrentTransfers.map(m => fr"max_in_flight = $m"),
      newParameters.partitionCount.map(p => fr"partition_count = $p")
    )

    val patchTransaction = for {
      maybeId <- lookupQueueId(name)
      id <- maybeId.liftTo[ConnectionIO](NoSuchQueue(name))
      queue <- (fr"update queues" ++ updates ++ fr"where id = $id").update
        .withUniqueGeneratedKeys[Queue](queueColumns: _*)
    } yield {
      queue
    }

    for {
      _ <- checkParameters(name, newParameters)
      queue <- patchTransaction.transact(dbClient)
    } yield {
      queue
    }
  }

  /**
    * Look up information stored by Transporter for the queue resource with the given name,
    * if one exists.
    */
  def lookupQueue(name: String): IO[Queue] =
    for {
      _ <- logger.info(s"Looking up queue $name")
      maybeQueue <- (fr"select" ++ queueColsFragment ++ fr"from queues where name = $name")
        .query[Queue]
        .option
        .transact(dbClient)
      queue <- maybeQueue.liftTo[IO](NoSuchQueue(name))
    } yield {
      queue
    }

}

object QueueController {

  /** Minimum partitions supported for Kafka topics created by Transporter. */
  val MinPartitions = 1

  /** Maximum partitions supported for Kafka topics created by Transporter. */
  val MaxPartitions = 64
}
