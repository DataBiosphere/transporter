package org.broadinstitute.transporter.queue

import cats.effect.IO
import io.chrisdavenport.fuuid.FUUID
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient

class QueueController(dbClient: DbClient, kafkaClient: KafkaClient) {

  def createQueue(request: QueueRequest): IO[Queue] =
    for {
      uuid <- FUUID.randomFUUID[IO]
      topicPrefix = s"${request.name}.$uuid"
      queue = Queue(
        name = request.name,
        requestTopic = s"$topicPrefix.requests",
        responseTopic = s"$topicPrefix.responses",
        schema = request.schema
      )
      _ <- kafkaClient.createTopics(List(queue.requestTopic, queue.responseTopic))
      _ <- dbClient.insertQueue(queue)
    } yield {
      queue
    }

  def lookupQueue(name: String): IO[Queue] =
    dbClient.lookupQueue(name).flatMap { maybeQueue =>
      maybeQueue
        .fold[IO[Queue]](IO.raiseError(QueueController.NoSuchQueue(name)))(IO.pure)
    }
}

object QueueController {
  case class NoSuchQueue(name: String)
      extends IllegalArgumentException(s"No such queue: $name")
}
