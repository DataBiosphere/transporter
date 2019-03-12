package org.broadinstitute.transporter.transfer

import java.util.UUID

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.{ExitCase, IO}
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient
import org.broadinstitute.transporter.queue.{QueueController, QueueSchema}

trait TransferController {
  def submitTransfer(queueName: String, request: TransferRequest): IO[TransferAck]
}

object TransferController {

  def apply(
    queueController: QueueController,
    dbClient: DbClient,
    kafkaClient: KafkaClient
  ): TransferController = new Impl(queueController, dbClient, kafkaClient)

  case class NoSuchQueue(name: String)
      extends IllegalArgumentException(s"Queue '$name' does not exist")

  case class InvalidRequest(failures: NonEmptyList[Throwable])
      extends IllegalArgumentException(
        s"Request includes ${failures.length} invalid transfers"
      )

  private[transfer] class Impl(
    queueController: QueueController,
    dbClient: DbClient,
    kafkaClient: KafkaClient
  ) extends TransferController {

    private val logger = Slf4jLogger.getLogger[IO]

    override def submitTransfer(
      queueName: String,
      request: TransferRequest
    ): IO[TransferAck] =
      for {
        info <- queueController.lookupQueueInfo(queueName)
        (queueId, requestTopic, _, schema) <- info.liftTo[IO](NoSuchQueue(queueName))
        _ <- logger.info(
          s"Submitting ${request.transfers.length} transfers to queue $queueName"
        )
        _ <- validateRequests(request.transfers, schema)
        (requestId, transfersById) <- dbClient.recordTransferRequest(queueId, request)
        _ <- submitOrRollback(requestId, requestTopic, transfersById)
      } yield {
        TransferAck(requestId)
      }

    private def validateRequests(requests: List[Json], schema: QueueSchema): IO[Unit] =
      for {
        _ <- logger.debug(s"Validating requests against schema: $schema")
        _ <- requests.traverse_(schema.validate(_).toValidatedNel) match {
          case Valid(_) => IO.unit
          case Invalid(errs) =>
            logger
              .error(s"Requests failed validation:")
              .flatMap(_ => errs.traverse_(e => logger.error(e.getMessage)))
              .flatMap(_ => IO.raiseError(TransferController.InvalidRequest(errs)))
        }
      } yield ()

    private def submitOrRollback(
      requestId: UUID,
      requestTopic: String,
      messages: List[(UUID, Json)]
    ): IO[Unit] =
      kafkaClient.submit(requestTopic, messages).guaranteeCase {
        case ExitCase.Completed =>
          logger.info(s"Successfully submitted request $requestId")
        case ExitCase.Canceled =>
          logger
            .warn(s"Submitting request $requestId was canceled")
            .flatMap(_ => dbClient.deleteTransferRequest(requestId))
        case ExitCase.Error(err) =>
          logger
            .error(err)(s"Failed to submit request $requestId")
            .flatMap(_ => dbClient.deleteTransferRequest(requestId))
      }
  }
}
