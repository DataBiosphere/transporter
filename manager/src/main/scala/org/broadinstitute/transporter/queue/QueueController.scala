package org.broadinstitute.transporter.queue

import java.util.UUID

import cats.effect.{ExitCase, IO}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.AdminClient
import org.broadinstitute.transporter.kafka.config.KafkaConfig
import org.broadinstitute.transporter.queue.api.{Queue, QueueParameters, QueueRequest}

/** Component responsible for handling all queue-related web requests. */
trait QueueController {

  /**
    * Create a new queue matching the parameters of the given request,
    * returning all data for the new resource on success.
    *
    * Fails with a [[QueueController.QueueAlreadyExists]] if trying to create a queue
    * with a name that's already registered in the DB.
    */
  def createQueue(request: QueueRequest): IO[Queue]

  /** Update the runtime parameters of an existing queue. */
  def patchQueue(name: String, newParameters: QueueParameters): IO[Queue]

  /**
    * Fetch the queue model with the given name, if it exists
    * and is in a consistent state.
    */
  final def lookupQueue(name: String): IO[Queue] =
    lookupQueueInfo(name).flatMap {
      case Some((_, queue)) => IO.pure(queue)
      case None             => IO.raiseError(QueueController.NoSuchQueue(name))
    }

  /**
    * Fetch information stored about the queue with the given name,
    * if one exists and is in a consistent state.
    *
    * Checks both the DB and Kafka for the existence of queue resources to avoid
    * getting into a state where we report existing info from the DB but always
    * fail to submit transfers because of nonexistent Kafka topics.
    */
  def lookupQueueInfo(name: String): IO[Option[(UUID, Queue)]]
}

object QueueController {

  // Pseudo-constructor for the Impl subclass.
  def apply(dbClient: DbClient, kafkaClient: AdminClient): QueueController =
    new Impl(dbClient, kafkaClient)

  /** Exception used to mark when a user attempts to interact with a nonexistent queue. */
  case class NoSuchQueue(name: String) extends IllegalArgumentException

  /** Exception used to mark when a user attempts to create a queue that already exists. */
  case class QueueAlreadyExists(name: String) extends IllegalArgumentException

  /** Exception used to mark when a user attempts to overwrite a queue parameter with an invalid value. */
  case class InvalidQueueParameter(name: String, message: String)
      extends IllegalArgumentException

  /**
    * Concrete implementation of the controller used by mainline code.
    *
    * @param dbClient client which can interact with Transporter's DB
    * @param kafkaClient client which can interact with Transporter's Kafka cluster
    */
  private[queue] class Impl(dbClient: DbClient, kafkaClient: AdminClient)
      extends QueueController {

    private val logger = Slf4jLogger.getLogger[IO]

    override def createQueue(request: QueueRequest): IO[Queue] = {
      val name = request.name
      for {
        (preexisting, topicsExist) <- checkDbAndKafkaForQueue(name)
        queue <- (preexisting, topicsExist) match {
          case (Some(_), true) =>
            IO.raiseError(QueueAlreadyExists(name))
          case (Some((id, queue)), false) =>
            for {
              _ <- logger.warn(
                s"Attempting to correct inconsistent state for queue: $name"
              )
              _ <- checkConcurrencyValid(request.name, request.maxConcurrentTransfers)
              _ <- dbClient.patchQueueParameters(
                id,
                QueueParameters(
                  schema = Some(request.schema),
                  maxConcurrentTransfers = Some(request.maxConcurrentTransfers)
                )
              )
              _ <- kafkaClient.createTopics(
                queue.requestTopic,
                queue.progressTopic,
                queue.responseTopic
              )
            } yield {
              queue.copy(schema = request.schema)
            }
          case (None, _) =>
            for {
              _ <- checkConcurrencyValid(request.name, request.maxConcurrentTransfers)
              _ <- logger.info(s"Initializing resources for queue ${request.name}")
              initialized <- initializeQueue(request)
            } yield initialized
        }
      } yield {
        queue
      }
    }

    override def patchQueue(name: String, newParameters: QueueParameters): IO[Queue] =
      for {
        maybeInfo <- lookupQueueInfo(name)
        (id, queue) <- maybeInfo.liftTo[IO](NoSuchQueue(name))
        _ <- dbClient.patchQueueParameters(id, newParameters)
      } yield {
        queue.copy(
          schema = newParameters.schema.getOrElse(queue.schema),
          maxConcurrentTransfers =
            newParameters.maxConcurrentTransfers.getOrElse(queue.maxConcurrentTransfers)
        )
      }

    override def lookupQueueInfo(name: String): IO[Option[(UUID, Queue)]] =
      for {
        _ <- logger.info(s"Looking up info for queue with name '$name'")
        (maybeInfo, topicsExist) <- checkDbAndKafkaForQueue(name)
      } yield {
        maybeInfo.filter(_ => topicsExist)
      }

    /**
      * Check if the DB contains a record for a queue with the given name. If so,
      * check if the Kafka topics associated with the queue exist.
      *
      * Kafka topics for a recorded queue might not exist if a catastrophic failure
      * occurred between writing to the DB and topic creation (or between a failure in
      * topic creation and the follow-up DB rollback).
      */
    private def checkDbAndKafkaForQueue(
      name: String
    ): IO[(Option[(UUID, Queue)], Boolean)] =
      for {
        queueInfo <- dbClient.lookupQueue(name)
        topicsExist <- queueInfo.fold(IO.pure(false)) {
          case (_, queue) =>
            kafkaClient.topicsExist(
              queue.requestTopic,
              queue.progressTopic,
              queue.responseTopic
            )
        }
        _ <- if (queueInfo.isDefined && !topicsExist) {
          logger.warn(
            s"Inconsistent state detected: $name is registered in DB, but has no resources in Kafka"
          )
        } else {
          IO.unit
        }
      } yield {
        (queueInfo, topicsExist)
      }

    private def checkConcurrencyValid(queueName: String, concurrency: Int): IO[Unit] =
      IO.raiseError(
          InvalidQueueParameter(queueName, "Max concurrent requests must be non-negative")
        )
        .whenA(concurrency < 0)

    /**
      * Initialize resources for a new queue matching the parameters
      * of the given request, returning all data for the new resource
      * on success.
      *
      * The process of initializing a queue requires making updates to
      * both the DB and Kafka. To avoid inconsistent state, we:
      *   1. Enter new queue information into the DB
      *   2. Attempt to create Kafka topics matching the new DB info
      *   3. On failure of 2, delete the new row from the DB
      *
      * If a catastrophic failure occurs between 1 and 2, the DB will
      * include pointers to nonexistent Kafka topics. We deal with this
      * by having our queue-lookup functionality check for both a row
      * in the DB _and_ the corresponding Kafka topics.
      */
    private def initializeQueue(request: QueueRequest): IO[Queue] = {
      val name = request.name
      val uuid = UUID.randomUUID()
      val queue = api.Queue(
        name = request.name,
        requestTopic = s"${KafkaConfig.RequestTopicPrefix}$uuid",
        progressTopic = s"${KafkaConfig.ProgressTopicPrefix}$uuid",
        responseTopic = s"${KafkaConfig.ResponseTopicPrefix}$uuid",
        schema = request.schema,
        maxConcurrentTransfers = request.maxConcurrentTransfers
      )

      for {

        // `bracketCase` is like try-catch-finally for the FP libs.
        // It schedules cleanup code in a way that prevents it from
        // being skipped by cancellation.
        _ <- dbClient
          .createQueue(uuid, queue)
          .bracketCase { _ =>
            kafkaClient.createTopics(
              queue.requestTopic,
              queue.progressTopic,
              queue.responseTopic
            )
          } { (_, status) =>
            status match {
              case ExitCase.Completed =>
                logger.info(s"Successfully created queue: $name")

              case ExitCase.Canceled =>
                for {
                  _ <- logger.warn(s"Creation of topics for queue $name was canceled")
                  _ <- dbClient.deleteQueue(uuid)
                } yield ()

              case ExitCase.Error(e) =>
                for {
                  _ <- logger
                    .error(e)(s"Failed to create topics for queue $name, rolling back")
                  _ <- dbClient.deleteQueue(uuid)
                } yield ()
            }
          }
      } yield queue
    }
  }
}
