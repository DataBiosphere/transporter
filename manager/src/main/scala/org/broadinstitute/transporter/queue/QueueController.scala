package org.broadinstitute.transporter.queue

import cats.effect.{ExitCase, IO}
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient

class QueueController(dbClient: DbClient, kafkaClient: KafkaClient) {
  import QueueController._

  private val logger = Slf4jLogger.getLogger[IO]

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
      kafkaClient.createTopics(queue.requestTopic, queue.responseTopic).as(queue)

    val maybeRollback = (queue: Queue, status: ExitCase[Throwable]) =>
      status match {
        case ExitCase.Completed =>
          logger.info(s"Successfully created queue ${queue.name}")
        case ExitCase.Canceled =>
          logger.warn(s"Creation of topics for queue ${queue.name} was canceled")
        case ExitCase.Error(e) =>
          for {
            _ <- logger.error(e)(
              s"Failed to create topics for queue ${queue.name}, rolling back"
            )
            _ <- dbClient.deleteQueue(queue.name)
          } yield ()
      }

    prepQueue.bracketCase(createTopics)(maybeRollback)
  }

  def lookupQueue(name: String): IO[Queue] =
    dbClient.lookupQueue(name).flatMap { maybeQueue =>
      maybeQueue
        .fold[IO[Queue]](IO.raiseError(NoSuchQueue(name)))(IO.pure)
    }
}

object QueueController {
  case class QueueAlreadyExists(name: String)
      extends IllegalArgumentException(s"Queue '$name' already exists")
  case class NoSuchQueue(name: String)
      extends IllegalArgumentException(s"No such queue: $name")
}
