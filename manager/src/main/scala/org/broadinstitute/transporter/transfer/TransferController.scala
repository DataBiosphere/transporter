package org.broadinstitute.transporter.transfer

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.queue.{QueueController, QueueSchema}

class TransferController(queueController: QueueController, dbClient: DbClient) {
  import TransferController._

  private val logger = Slf4jLogger.getLogger[IO]

  def submitTransfer(queueName: String, request: TransferRequest): IO[TransferAck] =
    for {
      (dbInfo, topicsExist) <- queueController.checkDbAndKafkaForQueue(queueName)
      canSubmit = dbInfo.filter(_ => topicsExist)
      (queueId, requestTopic, _, schema) <- canSubmit.liftTo[IO](NoSuchQueue(queueName))
      _ <- validateRequests(request.transfers, schema)
      (requestId, transfersById) <- dbClient.recordTransferRequest(queueId, request)
    } yield {
      val _ = (requestId, transfersById, requestTopic)
      ???
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
}

object TransferController {

  case class NoSuchQueue(name: String)
      extends IllegalArgumentException(s"Queue '$name' does not exist")

  case class InvalidRequest(failures: NonEmptyList[Throwable])
      extends IllegalArgumentException(
        s"Request includes ${failures.length} invalid transfers"
      )
}
