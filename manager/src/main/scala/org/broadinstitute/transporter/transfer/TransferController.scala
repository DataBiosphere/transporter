package org.broadinstitute.transporter.transfer

import java.time.{Instant, OffsetDateTime}
import java.util.UUID

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.{Clock, IO}
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.{Constants, DbLogHandler, DoobieInstances}
import org.broadinstitute.transporter.error.ApiError.{Conflict, InvalidRequest, NotFound}
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.config.TransferSchema

import scala.collection.JavaConverters._

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
)(implicit clk: Clock[IO]) {

  import DoobieInstances._
  import Constants._

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  /** Get the total number of transfer requests stored by Transporter. */
  def countRequests: IO[Long] =
    Fragment
      .const(s"SELECT COUNT(1) FROM $RequestsTable")
      .query[Long]
      .unique
      .transact(dbClient)

  /**
    * Fetch summaries of all the batch requests stored by Transporter
    * which fall within a specified page range.
    */
  def listRequests(
    offset: Long,
    limit: Long,
    newestFirst: Boolean
  ): IO[List[RequestSummary]] = {
    val query = for {
      ids <- List(
        Fragment.const(s"SELECT id FROM $RequestsTable ORDER BY received_at"),
        Fragment.const(if (newestFirst) "DESC" else "ASC"),
        fr"LIMIT $limit OFFSET $offset"
      ).combineAll.query[UUID].to[List]
      summaries <- lookupSummaries(ids)
    } yield {
      summaries
    }

    query.transact(dbClient).map { summaries =>
      if (newestFirst) {
        summaries.sortBy(_.receivedAt)(Ordering[OffsetDateTime].reverse)
      } else {
        summaries.sortBy(_.receivedAt)
      }
    }
  }

  private val baselineStatusCounts = TransferStatus.values.map(_ -> 0L).toMap

  /**
    * Fetch summaries of either:
    *   1. All batch requests stored by Transporter which fall in a page range, or
    *   2. A specific batch requests, identified by ID
    *
    * Batch requests are ordered by their IDs for now.
    */
  private def lookupSummaries(ids: List[UUID]): ConnectionIO[List[RequestSummary]] =
    for {
      countsPerStatusPerRequest <- List(
        fr"SELECT r.id AS id, t.status AS status, COUNT(t.id) AS n",
        fr"FROM" ++ TransfersJoinTable,
        fr"WHERE r.id = ANY($ids) GROUP BY r.id, t.status, r.received_at"
      ).combineAll.query[(UUID, TransferStatus, Long)].to[List].map { perStateCounts =>
        perStateCounts.foldLeft(Map.empty[UUID, Map[TransferStatus, Long]]) {
          case (acc, (id, status, count)) =>
            val newCounts = acc.get(id) match {
              case None           => baselineStatusCounts.updated(status, count)
              case Some(existing) => existing.updated(status, existing(status) + count)
            }
            acc.updated(id, newCounts)
        }
      }
      summaries <- List(
        fr"SELECT r.id, r.received_at, MIN(t.submitted_at), MAX(t.updated_at)",
        fr"FROM" ++ TransfersJoinTable,
        fr"WHERE r.id = ANY($ids) GROUP BY r.id, r.received_at"
      ).combineAll
        .query[(UUID, OffsetDateTime, Option[OffsetDateTime], Option[OffsetDateTime])]
        .map {
          case (id, receivedAt, submittedAt, updatedAt) =>
            RequestSummary(
              id,
              receivedAt,
              countsPerStatusPerRequest(id),
              submittedAt,
              updatedAt
            )
        }
        .to[List]
    } yield {
      summaries
    }

  /**
    * Record a new batch of transfers.
    *
    * Transfer payloads will be validated against a configured schema before
    * being recorded in the DB. Transfers are not immediately submitted to Kafka upon
    * being recorded; instead, a periodic sweeper pulls pending messages from the DB
    * to maintain a configured parallelism factor.
    */
  def recordRequest(request: BulkRequest): IO[RequestAck] = {

    val transfersWithDefaults = request.transfers.map { transfer =>
      if (request.defaults.isDefined) {
        TransferRequest(
          request.defaults.get.payload.deepMerge(transfer.payload),
          transfer.priority.orElse(request.defaults.get.priority)
        )
      } else {
        transfer
      }
    }

    for {
      _ <- validateRequests(transfersWithDefaults.map(_.payload))
      _ <- logger.info(
        s"Received ${request.transfers.length} transfers"
      )
      now <- clk
        .realTime(scala.concurrent.duration.MILLISECONDS)
        .map(Instant.ofEpochMilli)
      ack <- storeRequest(now).flatMap { requestId =>
        recordTransferRequest(
          transfersWithDefaults,
          requestId,
          0.toShort
        ).map { transferIds =>
          RequestAck(requestId, transferIds.length)
        }
      }.transact(dbClient)
    } yield {
      ack
    }
  }

  /** Generate and record a request ID for an incoming request of transfers. */
  private def storeRequest(now: Instant): ConnectionIO[UUID] = {
    val rId = UUID.randomUUID()
    val updateSql = List(
      Fragment.const(s"INSERT INTO $RequestsTable (id, received_at) VALUES"),
      fr"($rId," ++ Fragment.const(timestampSql(now)) ++ fr")"
    ).combineAll
    for {
      requestId <- updateSql.update.withUniqueGeneratedKeys[UUID]("id")
    } yield {
      requestId
    }
  }

  /** Record a batch of validated transfers with the given ID. */
  private[transfer] def recordTransferRequest(
    transfers: List[TransferRequest],
    requestId: UUID,
    fallbackPriority: Short
  ): ConnectionIO[List[UUID]] = {
    val transferInfo = transfers.map { transfer =>
      (
        UUID.randomUUID(),
        requestId,
        TransferStatus.Pending: TransferStatus,
        transfer.payload,
        -1,
        transfer.priority.getOrElse(fallbackPriority)
      )
    }
    for {
      _ <- Update[(UUID, UUID, TransferStatus, Json, Int, Short)](
        s"INSERT INTO $TransfersTable (id, request_id, status, body, steps_run, priority) VALUES (?, ?, ?, ?, ?, ?)"
      ).updateMany(transferInfo)
    } yield {
      transferInfo.map(_._1)
    }
  }

  /** Validate that every request in a batch matches an expected JSON schema. */
  private def validateRequests(requests: List[Json]): IO[Unit] =
    for {
      _ <- logger.debug(s"Validating requests against schema: $schema")
      _ <- requests.traverse_(schema.validate(_).toValidatedNel) match {
        case Valid(_) => IO.unit
        case Invalid(errs) =>
          NonEmptyList
            .fromList(errs.toList.flatMap(_.getAllMessages.asScala.toList))
            .fold(IO.unit) { msgs =>
              logger
                .error("Requests failed validation:")
                .flatTap(_ => msgs.traverse_(logger.error(_)))
                .flatMap(_ => IO.raiseError(InvalidRequest(msgs)))
            }
      }
    } yield ()

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
        Fragment.const(s"SELECT 1 FROM $RequestsTable"),
        fr"WHERE id = $requestId LIMIT 1"
      ).combineAll
        .query[Long]
        .option
      _ <- IO
        .raiseError(NotFound(requestId))
        .whenA(requestRow.isEmpty)
        .to[ConnectionIO]
      out <- f(requestId)
    } yield {
      out
    }

    transaction.transact(dbClient)
  }

  /**
    * Check that a transfer with the given ID exists, then run an operation using the ID as input.
    *
    * Utility for sharing guardrails across transfer-level operations in the controller.
    */
  private def checkAndExec[Out](requestId: UUID, transferId: UUID)(
    f: (UUID, UUID) => ConnectionIO[Out]
  ): IO[Out] = {
    val transaction = for {
      requestRow <- List(
        Fragment.const(s"SELECT 1 FROM $TransfersTable"),
        Fragments.whereAnd(fr"request_id = $requestId", fr"id = $transferId")
      ).combineAll
        .query[Long]
        .option
      _ <- IO
        .raiseError(NotFound(requestId, Some(transferId)))
        .whenA(requestRow.isEmpty)
        .to[ConnectionIO]
      out <- f(requestId, transferId)
    } yield {
      out
    }

    transaction.transact(dbClient)
  }

  /** Get the current summary-level status for a request. */
  def lookupRequestStatus(requestId: UUID): IO[RequestSummary] =
    checkAndExec(requestId)(rId => lookupSummaries(List(rId)).map(_.head))

  /**
    * Reset the statuses for all failed transfers under a request to 'pending',
    * so that they will be re-submitted by the periodic sweeper.
    */
  def reconsiderRequest(requestId: UUID): IO[RequestAck] =
    checkAndExec(requestId) { rId =>
      List(
        Fragment.const(s"UPDATE $TransfersTable t"),
        fr"SET status = ${TransferStatus.Pending: TransferStatus} FROM",
        Fragment.const(s"$RequestsTable r"),
        Fragments.whereAnd(
          fr"t.request_id = r.id",
          fr"r.id = $rId",
          fr"t.status = ${TransferStatus.Failed: TransferStatus}"
        )
      ).combineAll.update.run
    }.map(RequestAck(requestId, _))

  /**
    * Reset the status for a single failed transfer to 'pending' so that it will be re-submitted by the periodic
    * sweeper.
    */
  def reconsiderSingleTransfer(requestId: UUID, transferId: UUID): IO[RequestAck] =
    checkAndExec(requestId, transferId) { (rId, tId) =>
      List(
        Fragment.const(s"UPDATE $TransfersTable t"),
        fr"SET status = ${TransferStatus.Pending: TransferStatus}",
        Fragments.whereAnd(
          fr"t.request_id = $rId",
          fr"t.id = $tId",
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
    checkAndExec(requestId, transferId) { (rId, tId) =>
      List(
        Fragment.const(
          s"SELECT id, status, priority, body, submitted_at, updated_at, info FROM $TransfersTable"
        ),
        Fragments.whereAnd(fr"request_id = $rId", fr"id = $tId")
      ).combineAll
        .query[TransferDetails]
        .unique
    }

  /** Get the total number of transfers stored by Transporter. */
  def countTransfers(requestId: UUID, status: Option[TransferStatus] = None): IO[Long] =
    List(
      fr"SELECT COUNT(1) FROM",
      Fragment.const(TransfersTable),
      Fragments.whereAndOpt(
        Some(fr"request_id = $requestId"),
        status.map(s => fr"status = $s")
      )
    ).combineAll
      .query[Long]
      .unique
      .transact(dbClient)

  /**
    * For a request ID, page #, and IDs per page, return the list of associated transfer IDs. Page order will match
    * the order of the input batch.
    */
  def listTransfers(
    requestId: UUID,
    offset: Long,
    limit: Long,
    sortDesc: Boolean,
    status: Option[TransferStatus] = None
  ): IO[List[TransferDetails]] =
    checkAndExec(requestId) { rId =>
      val order = Fragment.const(if (sortDesc) "desc" else "asc")
      val statusFilter = status match {
        case Some(actualStatus) => fr"AND status = $actualStatus"
        case None               => fr""
      }
      List(
        Fragment.const(
          s"SELECT id, status, priority, body, submitted_at, updated_at, info FROM $TransfersTable"
        ),
        fr"WHERE request_id = $rId",
        statusFilter,
        fr"ORDER BY id" ++ order ++ fr"LIMIT $limit OFFSET $offset"
      ).combineAll
        .query[TransferDetails]
        .to[List]
    }

  /**
    * Update the priority of all transfers under a request if the transfer is pending.
    * Will overwrite every priority stored in the DB, even for transfers with non-default priorities
    * on submission.
    */
  def updateRequestPriority(requestId: UUID, priority: Short): IO[RequestAck] =
    checkAndExec(requestId) { rId =>
      for {
        checkRows <- List(
          Fragment.const(s"SELECT COUNT(1) FROM $TransfersTable"),
          Fragments.whereAnd(
            fr"request_id = $rId",
            fr"status = ${TransferStatus.Pending: TransferStatus}"
          )
        ).combineAll.query[Long].unique
        _ <- IO
          .raiseError(Conflict(requestId))
          .whenA(checkRows == 0)
          .to[ConnectionIO]
        updatedRows <- List(
          Fragment.const(s"UPDATE $TransfersTable t"),
          fr"SET priority = $priority",
          Fragments.whereAnd(
            fr"t.request_id = $rId",
            fr"t.priority != $priority",
            fr"t.status = ${TransferStatus.Pending: TransferStatus}"
          )
        ).combineAll.update.run
      } yield {
        RequestAck(requestId, updatedRows)
      }
    }

  /**
    * Update the priority of a specific transfer under a request if the transfer is pending.
    */
  def updateTransferPriority(
    requestId: UUID,
    transferId: UUID,
    priority: Short
  ): IO[RequestAck] =
    checkAndExec(requestId, transferId) { (rId, tId) =>
      for {
        checkRows <- List(
          Fragment.const(s"SELECT COUNT(1) FROM $TransfersTable"),
          Fragments.whereAnd(
            fr"id = $tId",
            fr"status = ${TransferStatus.Pending: TransferStatus}"
          )
        ).combineAll.query[Long].unique
        _ <- IO
          .raiseError(Conflict(rId, Some(tId)))
          .whenA(checkRows == 0)
          .to[ConnectionIO]
        updatedRows <- List(
          Fragment.const(s"UPDATE $TransfersTable SET priority = $priority"),
          Fragments.whereAnd(
            fr"id = $tId",
            fr"priority != $priority",
            fr"status = ${TransferStatus.Pending: TransferStatus}"
          )
        ).combineAll.update.run
      } yield {
        RequestAck(requestId, updatedRows)
      }
    }
}
