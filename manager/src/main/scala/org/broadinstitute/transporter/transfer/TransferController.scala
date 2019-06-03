package org.broadinstitute.transporter.transfer

import java.time.OffsetDateTime
import java.util.UUID

import cats.Order
import cats.data.Validated.{Invalid, Valid}
import cats.effect.IO
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.{Constants, DbLogHandler, DoobieInstances}
import org.broadinstitute.transporter.error.{
  InvalidRequest,
  NoSuchRequest,
  NoSuchTransfer
}
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.config.TransferSchema

/**
  * Component responsible for backing Transporter's transfer-level web APIs.
  *
  * The controller records new transfer requests, but doesn't submit them to Kafka.
  * We introduce an async delay between those two steps to allow for backpressure /
  * priority queueing within the manager, since Kafka doesn't have native support
  * for that functionality.
  *
  * @param schema JSON schema that transfer requests handled by this controller
  *               must match in order to be recorded for later submission
  * @param dbClient client which can interact with Transporter's backing DB
  */
class TransferController(
  schema: TransferSchema,
  dbClient: Transactor[IO]
) {

  import DoobieInstances._
  import Constants._

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  /**
    * Record a new batch of transfers.
    *
    * Transfer payloads will be validated against a configured schema before
    * being recorded in the DB. Transfers are not immediately submitted to Kafka upon
    * being recorded; instead, a periodic sweeper pulls pending messages from the DB
    * to maintain a configured parallelism factor.
    */
  def recordTransfer(request: BulkRequest): IO[RequestAck] =
    for {
      _ <- validateRequests(request.transfers)
      _ <- logger.info(
        s"Submitting ${request.transfers.length} transfers"
      )
      ack <- recordTransferRequest(request).transact(dbClient)
    } yield {
      ack
    }

  /** Record a batch of validated transfers with the given ID. */
  private def recordTransferRequest(request: BulkRequest): ConnectionIO[RequestAck] =
    for {
      requestId <- (Fragment
        .const(s"insert into $RequestsTable (id) values") ++ fr"(${UUID.randomUUID()})").update
        .withUniqueGeneratedKeys[UUID]("id")
      transferInfo = request.transfers.map { body =>
        (UUID.randomUUID(), requestId, TransferStatus.Pending: TransferStatus, body, -1)
      }
      transferCount <- Update[(UUID, UUID, TransferStatus, Json, Int)](
        s"insert into $TransfersTable (id, request_id, status, body, steps_run) values (?, ?, ?, ?, ?)"
      ).updateMany(transferInfo)
    } yield {
      RequestAck(requestId, transferCount)
    }

  /** Validate that every request in a batch matches an expected JSON schema. */
  private def validateRequests(requests: List[Json]): IO[Unit] =
    for {
      _ <- logger.debug(s"Validating requests against schema: $schema")
      _ <- requests.traverse_(schema.validate(_).toValidatedNel) match {
        case Valid(_) => IO.unit
        case Invalid(errs) =>
          logger
            .error(s"Requests failed validation:")
            .flatMap(_ => errs.traverse_(e => logger.error(e.getMessage)))
            .flatMap(_ => IO.raiseError(InvalidRequest(errs)))
      }
    } yield ()

  /**
    * Zero counts for every possible transfer status.
    *
    * For API consistency, we ensure request summaries contain values for each
    * possible status.
    */
  private val baselineCounts = TransferStatus.values.map(_ -> 0L).toMap

  private implicit val odtOrder: Order[OffsetDateTime] = _.compareTo(_)

  /**
    * Check that a request with the given ID exists, then run an operation using the ID as input.
    *
    * Utility for sharing guardrails across request-level operations in the controller.
    */
  private def checkAndExec[Out](requestId: UUID)(
    f: UUID => ConnectionIO[Out]
  ): IO[Out] = {
    val transaction = for {
      requestRow <- List(
        Fragment.const(s"select 1 from $RequestsTable"),
        fr"where id = $requestId limit 1"
      ).combineAll
        .query[Long]
        .option
      _ <- IO
        .raiseError(NoSuchRequest(requestId))
        .whenA(requestRow.isEmpty)
        .to[ConnectionIO]
      out <- f(requestId)
    } yield {
      out
    }

    transaction.transact(dbClient)
  }

  /** Get the current summary-level status for a request. */
  def lookupRequestStatus(requestId: UUID): IO[RequestStatus] =
    checkAndExec(requestId) { rId =>
      List(
        fr"select t.status, count(*), min(t.submitted_at), max(t.updated_at) from",
        TransfersJoinTable,
        Fragments.whereAnd(fr"r.id = $rId"),
        fr"group by t.status"
      ).combineAll
        .query[(TransferStatus, Long, Option[OffsetDateTime], Option[OffsetDateTime])]
        .to[List]
    }.map { summaries =>
      val counts = summaries.map { case (status, n, _, _) => status -> n }.toMap
      val maybeStatus = List(
        TransferStatus.InProgress,
        TransferStatus.Submitted,
        TransferStatus.Pending,
        TransferStatus.Failed,
        TransferStatus.Succeeded
      ).find(counts.getOrElse(_, 0L) > 0L)

      RequestStatus(
        requestId,
        // No transfers, instant success!
        maybeStatus.getOrElse(TransferStatus.Succeeded),
        baselineCounts.combine(counts),
        submittedAt = summaries
          .flatMap(_._3)
          .minimumOption,
        updatedAt = summaries
          .flatMap(_._4)
          .maximumOption
      )
    }

  /**
    * Get the JSON payloads returned by agents for successfully-completed transfers
    * under a request.
    */
  def lookupRequestOutputs(requestId: UUID): IO[RequestInfo] =
    lookupRequestInfo(requestId, TransferStatus.Succeeded)

  /**
    * Get the JSON payloads returned by agents for failed transfers
    * under a request.
    */
  def lookupRequestFailures(requestId: UUID): IO[RequestInfo] =
    lookupRequestInfo(requestId, TransferStatus.Failed)

  /**
    * Get any information collected by the manager from transfers under a previously-submitted
    * request which have a given status.
    */
  private def lookupRequestInfo(
    requestId: UUID,
    status: TransferStatus
  ): IO[RequestInfo] =
    checkAndExec(requestId) { rId =>
      List(
        fr"select t.id, t.info from",
        TransfersJoinTable,
        Fragments.whereAnd(
          fr"r.id = $rId",
          fr"t.status = $status",
          fr"t.info is not null"
        )
      ).combineAll
        .query[TransferInfo]
        .to[List]
    }.map(RequestInfo(requestId, _))

  /**
    * Reset the statuses for all failed transfers under a request to 'pending',
    * so that they will be re-submitted by the periodic sweeper.
    */
  def reconsiderRequest(requestId: UUID): IO[RequestAck] =
    checkAndExec(requestId) { rId =>
      List(
        Fragment.const(s"update $TransfersTable t"),
        fr"set status = ${TransferStatus.Pending: TransferStatus} from",
        Fragment.const(s"$RequestsTable r"),
        Fragments.whereAnd(
          fr"t.request_id = r.id",
          fr"r.id = $rId",
          fr"t.status = ${TransferStatus.Failed: TransferStatus}"
        )
      ).combineAll.update.run
    }.map(RequestAck(requestId, _))

  /**
    * Get all information stored by Transporter about a specific transfer under a request.
    */
  def lookupTransferDetails(
    requestId: UUID,
    transferId: UUID
  ): IO[TransferDetails] =
    for {
      maybeDetails <- checkAndExec(requestId) { rId =>
        List(
          fr"select t.id, t.status, t.body, t.submitted_at, t.updated_at, t.info from",
          TransfersJoinTable,
          Fragments.whereAnd(fr"r.id = $rId", fr"t.id = $transferId")
        ).combineAll
          .query[TransferDetails]
          .option
      }
      details <- maybeDetails.liftTo[IO](
        NoSuchTransfer(requestId, transferId)
      )
    } yield {
      details
    }
}
