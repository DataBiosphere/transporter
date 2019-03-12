package org.broadinstitute.transporter.transfer

import java.util.UUID

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.kafka.{KafkaProducer, ProducerMessage, ProducerRecord, Serializer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaConfig
import org.broadinstitute.transporter.queue.{QueueController, QueueSchema}

class TransferController private[transfer] (
  queueController: QueueController,
  dbClient: DbClient,
  kafkaProducer: KafkaProducer[IO, UUID, Json]
) {
  import TransferController._

  private val logger = Slf4jLogger.getLogger[IO]

  def submitTransfer(queueName: String, request: TransferRequest): IO[TransferAck] =
    for {
      (dbInfo, topicsExist) <- queueController.checkDbAndKafkaForQueue(queueName)
      canSubmit = dbInfo.filter(_ => topicsExist)
      (queueId, requestTopic, _, schema) <- canSubmit.liftTo[IO](NoSuchQueue(queueName))
      _ <- logger.info(
        s"Submitting ${request.transfers.length} transfers to queue $queueName"
      )
      _ <- validateRequests(request.transfers, schema)
      (requestId, transfersById) <- dbClient.recordTransferRequest(queueId, request)
      message = wrapTransfers(requestTopic, transfersById)
      _ <- logger.debug(s"Buffering transfers for request $requestId")
      ackIO <- kafkaProducer.producePassthrough(message)
      _ <- logger.debug(s"Waiting for acknowledgement of request $requestId")
      _ <- ackIO
    } yield {
      TransferAck(requestId)
    }

  private def validateRequests(requests: List[Json], schema: QueueSchema): IO[Unit] =
    for {
      _ <- logger.debug(s"Validating requests against schema: $schema")
      _ <- requests.traverse_(schema.validate(_).toValidatedNel) match {
        case Valid(_) => IO.unit
        case Invalid(errs) =>
          errs
            .traverse_(e => logger.error(e.getCause)(e.getMessage))
            .flatMap(_ => IO.raiseError(TransferController.InvalidRequest(errs)))
      }
    } yield ()

  private def wrapTransfers(
    requestTopic: String,
    transfersById: List[(UUID, Json)]
  ): ProducerMessage[List, UUID, Json, Unit] = {
    val records = transfersById.map {
      case (id, transfer) =>
        ProducerRecord(requestTopic, id, transfer)
    }

    ProducerMessage(records)
  }
}

object TransferController {

  private val jsonSerializer = Serializer.string.contramap[Json](_.noSpaces)

  def resource(
    queueController: QueueController,
    dbClient: DbClient,
    config: KafkaConfig
  )(implicit cs: ContextShift[IO]): Resource[IO, TransferController] =
    fs2.kafka
      .producerResource[IO]
      .using(config.producerSettings(Serializer.uuid, jsonSerializer))
      .map(new TransferController(queueController, dbClient, _))

  case class NoSuchQueue(name: String)
      extends IllegalArgumentException(s"Queue '$name' does not exist")

  case class InvalidRequest(failures: NonEmptyList[Throwable])
      extends IllegalArgumentException(
        s"Request includes ${failures.length} invalid transfers"
      )
}
