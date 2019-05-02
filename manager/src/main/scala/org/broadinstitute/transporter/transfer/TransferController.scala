package org.broadinstitute.transporter.transfer

import java.time.OffsetDateTime
import java.util.UUID

import cats.Order
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.queue.api.Queue
import org.broadinstitute.transporter.queue.{QueueController, QueueSchema}
import org.broadinstitute.transporter.transfer.api._

/** Component responsible for handling all transfer-related web requests. */
trait TransferController {

  /**
    * Submit a batch of transfer requests to the given queue resource,
    * returning a unique ID which can be queried for status updates.
    *
    * Submission initializes rows in the DB for tracking, then pushes the individual
    * transfer descriptions onto the "request" topic for the queue in Kafka.
    */
  def submitTransfer(queueName: String, request: BulkRequest): IO[RequestAck]

  /**
    * Summarize the current status of a request which was previously submitted to a queue.
    */
  def lookupRequestStatus(queueName: String, requestId: UUID): IO[RequestStatus]

  /**
    * Get any information collected by the manager from successful transfers under
    * a previously-submitted request.
    */
  def lookupRequestOutputs(queueName: String, requestId: UUID): IO[RequestMessages]

  /**
    * Get any information collected by the manager from failed transfers under
    * a previously-submitted request.
    */
  def lookupRequestFailures(queueName: String, requestId: UUID): IO[RequestMessages]

  /** Get detailed information about a single transfer running under a queue. */
  def lookupTransferDetails(
    queueName: String,
    requestId: UUID,
    transferId: UUID
  ): IO[TransferDetails]
}

object TransferController {

  // Pseudo-constructor for the Impl subclass.
  def apply(queueController: QueueController, dbClient: DbClient): TransferController =
    new Impl(queueController, dbClient)

  /** Exception used to mark when a user submits transfers that don't match a queue's expected schema. */
  case class InvalidRequest(failures: NonEmptyList[Throwable])
      extends IllegalArgumentException(
        s"Request includes ${failures.length} invalid transfers"
      )

  /** Exception used to mark when a user attempts to interact with a nonexistent request. */
  case class NoSuchRequest(queue: String, id: UUID)
      extends IllegalArgumentException(
        s"No request with ID $id registered under queue $queue "
      )

  case class NoSuchTransfer(queue: String, requestId: UUID, id: UUID)
      extends IllegalArgumentException(
        s"No transfer with ID $id registered under request $requestId in queue $queue"
      )

  /**
    * Concrete implementation of the controller used by mainline code.
    *
    * @param queueController controller to delegate to for performing queue-level operations
    * @param dbClient client which can interact with Transporter's DB
    */
  private[transfer] class Impl(queueController: QueueController, dbClient: DbClient)
      extends TransferController {

    private val logger = Slf4jLogger.getLogger[IO]

    override def submitTransfer(queueName: String, request: BulkRequest): IO[RequestAck] =
      for {
        (id, queue) <- getQueueInfo(queueName)
        _ <- logger.info(
          s"Submitting ${request.transfers.length} transfers to queue $queueName"
        )
        _ <- validateRequests(request.transfers, queue.schema)
        requestId <- dbClient.recordTransferRequest(id, request)
      } yield {
        RequestAck(requestId)
      }

    private val baselineCounts = TransferStatus.values.map(_ -> 0L).toMap

    private implicit val odtOrder: Order[OffsetDateTime] = _.compareTo(_)

    override def lookupRequestStatus(
      queueName: String,
      requestId: UUID
    ): IO[RequestStatus] =
      for {
        (queueId, _) <- getQueueInfo(queueName)
        summariesByStatus <- dbClient
          .summarizeTransfersByStatus(queueId, requestId)
        counts = summariesByStatus.mapValues(_._1)
        maybeStatus = List(
          TransferStatus.Submitted,
          TransferStatus.Pending,
          TransferStatus.Failed,
          TransferStatus.Succeeded
        ).find(counts.getOrElse(_, 0L) > 0L)
        status <- maybeStatus.liftTo[IO](NoSuchRequest(queueName, requestId))
      } yield {
        val flattenedInfo = summariesByStatus.values
        RequestStatus(
          requestId,
          status,
          baselineCounts.combine(counts),
          submittedAt = flattenedInfo
            .flatMap(_._2)
            .toList
            .minimumOption,
          updatedAt = flattenedInfo
            .flatMap(_._3)
            .toList
            .maximumOption
        )
      }

    override def lookupRequestOutputs(
      queueName: String,
      requestId: UUID
    ): IO[RequestMessages] =
      lookupRequestMessages(queueName, requestId, TransferStatus.Succeeded)

    override def lookupRequestFailures(
      queueName: String,
      requestId: UUID
    ): IO[RequestMessages] =
      lookupRequestMessages(queueName, requestId, TransferStatus.Failed)

    def lookupTransferDetails(
      queueName: String,
      requestId: UUID,
      transferId: UUID
    ): IO[TransferDetails] =
      for {
        (queueId, _) <- getQueueInfo(queueName)
        maybeDetails <- dbClient.lookupTransferDetails(queueId, requestId, transferId)
        details <- maybeDetails.liftTo[IO](
          NoSuchTransfer(queueName, requestId, transferId)
        )
      } yield {
        details
      }

    private def getQueueInfo(queueName: String): IO[(UUID, Queue)] =
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
      * Get any information collected by the manager from transfers under a previously-submitted
      * request which have a given status.
      */
    private def lookupRequestMessages(
      queueName: String,
      requestId: UUID,
      status: TransferStatus
    ): IO[RequestMessages] =
      for {
        (queueId, _) <- getQueueInfo(queueName)
        successMessages <- dbClient.lookupTransferMessages(queueId, requestId, status)
      } yield {
        RequestMessages(requestId, successMessages)
      }
  }
}
