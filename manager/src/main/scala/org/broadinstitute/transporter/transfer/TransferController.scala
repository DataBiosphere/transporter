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

/** Component responsible for handling all transfer-related web requests. */
trait TransferController {

  /**
    * Submit a batch of transfer requests to the given queue resource,
    * returning a unique ID which can be queried for status updates.
    *
    * Submission initializes rows in the DB for tracking, then pushes the individual
    * transfer descriptions onto the "request" topic for the queue in Kafka.
    */
  def submitTransfer(queueName: String, request: TransferRequest): IO[TransferAck]
}

object TransferController {

  // Pseudo-constructor for the Impl subclass.
  def apply(
    queueController: QueueController,
    dbClient: DbClient,
    kafkaClient: KafkaClient
  ): TransferController = new Impl(queueController, dbClient, kafkaClient)

  /** Exception used to mark when a user submits transfers to a nonexistent queue. */
  case class NoSuchQueue(name: String)
      extends IllegalArgumentException(s"Queue '$name' does not exist")

  /** Exception used to mark when a user submits transfers that don't match a queue's expected schema. */
  case class InvalidRequest(failures: NonEmptyList[Throwable])
      extends IllegalArgumentException(
        s"Request includes ${failures.length} invalid transfers"
      )

  /**
    * Concrete implementation of the controller used by mainline code.
    *
    * @param queueController controller to delegate to for performing queue-level operations
    * @param dbClient client which can interact with Transporter's DB
    * @param kafkaClient client which can interact with Transporter's Kafka cluster
    */
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

    /** Validate that every request in a batch matches the expected JSON schema for a queue. */
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

    /**
      * Attempt to submit a batch of messages to Kafka, cleaning up corresponding
      * DB records on failure.
      */
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
