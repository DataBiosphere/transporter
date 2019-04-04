package org.broadinstitute.transporter.transfer

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.{ExitCase, IO}
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaProducer
import org.broadinstitute.transporter.queue.{Queue, QueueController, QueueSchema}

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

  /**
    * Summarize the current status of a request which was previously submitted
    * to a queue resource, returning:
    *   1. An overall summary status for the request
    *   2. The number of transfers found in each potential "transfer status"
    *   3. Any "info" messages sent by agents about transfers in the request
    */
  def lookupTransferStatus(queueName: String, requestId: FUUID): IO[RequestStatus]
}

object TransferController {

  // Pseudo-constructor for the Impl subclass.
  def apply(
    queueController: QueueController,
    dbClient: DbClient,
    producer: KafkaProducer[FUUID, Json]
  ): TransferController = new Impl(queueController, dbClient, producer)

  /** Exception used to mark when a user submits transfers that don't match a queue's expected schema. */
  case class InvalidRequest(failures: NonEmptyList[Throwable])
      extends IllegalArgumentException(
        s"Request includes ${failures.length} invalid transfers"
      )

  /** Exception used to mark when a user attempts to interact with a nonexistent request. */
  case class NoSuchRequest(id: FUUID)
      extends IllegalArgumentException(s"No transfers registered under ID $id")

  /**
    * Concrete implementation of the controller used by mainline code.
    *
    * @param queueController controller to delegate to for performing queue-level operations
    * @param dbClient client which can interact with Transporter's DB
    * @param producer client which can push messages into Kafka
    */
  private[transfer] class Impl(
    queueController: QueueController,
    dbClient: DbClient,
    producer: KafkaProducer[FUUID, Json]
  ) extends TransferController {

    private val logger = Slf4jLogger.getLogger[IO]

    override def submitTransfer(
      queueName: String,
      request: TransferRequest
    ): IO[TransferAck] =
      for {
        (id, queue) <- getQueueInfo(queueName)
        _ <- logger.info(
          s"Submitting ${request.transfers.length} transfers to queue $queueName"
        )
        _ <- validateRequests(request.transfers, queue.schema)
        (requestId, transfersById) <- dbClient.recordTransferRequest(id, request)
        _ <- submitOrRollback(requestId, queue.requestTopic, transfersById)
      } yield {
        TransferAck(requestId)
      }

    private val baselineCounts = TransferStatus.values.map(_ -> 0L).toMap

    override def lookupTransferStatus(
      queueName: String,
      requestId: FUUID
    ): IO[RequestStatus] =
      for {
        (queueId, _) <- getQueueInfo(queueName)
        transfersByStatus <- dbClient
          .lookupTransfers(queueId, requestId)
        counts = transfersByStatus.mapValues(_._1)
        maybeStatus = List(
          TransferStatus.Failed,
          TransferStatus.Retrying,
          TransferStatus.Submitted,
          TransferStatus.Succeeded
        ).find(counts.getOrElse(_, 0L) > 0L)
        status <- maybeStatus.liftTo[IO](NoSuchRequest(requestId))
      } yield {
        RequestStatus(
          status,
          baselineCounts.combine(counts),
          transfersByStatus.flatMap(_._2._2).toList
        )
      }

    private def getQueueInfo(queueName: String): IO[(FUUID, Queue)] =
      for {
        maybeInfo <- queueController.lookupQueueInfo(queueName)
        info <- maybeInfo.liftTo[IO](QueueController.NoSuchQueue(queueName))
      } yield {
        info
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
      requestId: FUUID,
      requestTopic: String,
      messages: List[(FUUID, Json)]
    ): IO[Unit] =
      producer.submit(requestTopic, messages).guaranteeCase {
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
