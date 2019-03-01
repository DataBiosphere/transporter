package org.broadinstitute.transporter.queue

import cats.effect.{ExitCase, IO}
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient

/**
  * Component responsible for handling all queue-related requests.
  *
  * @param dbClient client which can interact with Transporter's DB
  * @param kafkaClient client which can interact with Transporter's Kafka cluster
  */
class QueueController(dbClient: DbClient, kafkaClient: KafkaClient) {
  import QueueController._

  private val logger = Slf4jLogger.getLogger[IO]

  /**
    * Create a new queue matching the parameters of the given request,
    * returning all data for the new resource on success.
    *
    * Fails with a [[QueueAlreadyExists]] if trying to create a queue
    * with a name that's already registered in the DB.
    */
  def createQueue(request: QueueRequest): IO[Queue] =
    for {
      preexisting <- dbClient.lookupQueue(request.name)
      _ <- preexisting.fold(IO.unit) { _ =>
        IO.raiseError(QueueAlreadyExists(request.name))
      }
      queue <- initializeQueue(request)
    } yield {
      queue
    }

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
    */
  private def initializeQueue(request: QueueRequest): IO[Queue] = {

    val prepQueue = for {
      _ <- logger.info(s"Initializing resources for queue ${request.name}")
      uuid <- FUUID.randomFUUID[IO]
      topicPrefix = s"${request.name}.$uuid"
      queue = Queue(
        name = request.name,
        requestTopic = s"$topicPrefix.requests",
        responseTopic = s"$topicPrefix.responses",
        schema = request.schema
      )
      _ <- dbClient.insertQueue(queue)
    } yield queue

    val createTopics = (queue: Queue) =>
      kafkaClient.createTopics(List(queue.requestTopic, queue.responseTopic)).as(queue)

    prepQueue.bracketCase(createTopics) { (queue, status) =>
      status match {
        case ExitCase.Completed =>
          logger.info(s"Successfully created queue ${queue.name}")

        case ExitCase.Canceled =>
          for {
            _ <- logger.warn(s"Creation of topics for queue ${queue.name} was canceled")
            _ <- dbClient.deleteQueue(queue.name)
          } yield ()

        case ExitCase.Error(e) =>
          for {
            _ <- logger.error(e)(
              s"Failed to create topics for queue ${queue.name}, rolling back"
            )
            _ <- dbClient.deleteQueue(queue.name)
          } yield ()
      }
    }
  }

  /**
    * Fetch the information stored about the queue with the given name.
    *
    * Returns a [[NoSuchQueue]] error if no queue with the given name
    * exists in Transporter's system.
    */
  def lookupQueue(name: String): IO[Queue] =
    dbClient.lookupQueue(name).flatMap { maybeQueue =>
      maybeQueue
        .fold[IO[Queue]](IO.raiseError(NoSuchQueue(name)))(IO.pure)
    }
}

object QueueController {

  /** Exception used to mark when a user attempts to create a queue that already exists. */
  case class QueueAlreadyExists(name: String)
      extends IllegalArgumentException(s"Queue '$name' already exists")

  /** Exception used to mark when a user queries a nonexistent queue. */
  case class NoSuchQueue(name: String)
      extends IllegalArgumentException(s"No such queue: $name")
}
