package org.broadinstitute.transporter.transfer

import java.time.Instant
import java.util.UUID

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
import org.broadinstitute.transporter.error.ApiError.{InvalidRequest, NotFound}
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
)(implicit clk: Clock[IO]) {

  import DoobieInstances._
  import Constants._

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  /** Get the total number of transfer requests stored by Transporter. */
  def countRequests: IO[Long] =
    Fragment
      .const(s"select count(1) from $RequestsTable")
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
  ): IO[List[RequestSummary]] =
    lookupSummaries(Left((offset, limit)), newestFirst)
      .transact(dbClient)

  /**
    * Fetch summaries of either:
    *   1. All batch requests stored by Transporter which fall in a page range, or
    *   2. A specific batch requests, identified by ID
    *
    * Batch requests are ordered by their IDs for now.
    */
  private def lookupSummaries(
    paginationOrId: Either[(Long, Long), UUID],
    newestFirst: Boolean = true
  ): ConnectionIO[List[RequestSummary]] = {

    val order = Fragment.const(if (newestFirst) "desc" else "asc")

    val groupBy = fr"group by r.id, r.received_at, t.status"
    val filterOrLimit = paginationOrId.fold(
      {
        case (offset, limit) =>
          groupBy ++ fr"order by r.received_at" ++ order ++ fr"limit $limit offset $offset"
      },
      id => fr"where r.id = $id" ++ groupBy
    )

    val tempTable = "id_status_summaries"

    // The 'crosstab' function needs a real (if temporary) table to work.
    val createTempTable = List(
      Fragment.const(s"create temporary table $tempTable as"),
      fr"select r.id as id, t.status as status, r.received_at as receive_t,",
      fr"min(t.submitted_at) as submit_t, max(t.updated_at) as update_t, count(t.id) as n",
      fr"from" ++ TransfersJoinTable,
      filterOrLimit
    ).combineAll

    val pivotTable = "count_pivot"
    val typedStatusColumns =
      Fragment.const(
        TransferStatus.values.map(s => s"${s.entryName} integer").mkString(", ")
      )
    val statusKvPairs = Fragment.const(
      TransferStatus.values
        .flatMap(s => Vector(s"'${s.entryName}'", s"coalesce(${s.entryName}, 0)"))
        .mkString(", ")
    )

    val buildPivot = List(
      Fragment.const(s"create temporary table $pivotTable as"),
      // crosstab lets us dynamically pivot the table from 3 columns of (id, status, count)
      // into N columns of (id, status1, status2, ...) with count values in the row cells.
      fr"select * from crosstab(",
      Fragment.const(s"'select id, status, n from $tempTable order by 1, 2',"),
      Fragment.const(s"'select unnest(enum_range(NULL::transfer_status)) order by 1'"),
      fr") as final_result(id uuid," ++ typedStatusColumns ++ fr")"
    ).combineAll

    val getSummaryInfo = List(
      fr"select id, received_at, json_build_object(" ++ statusKvPairs ++ fr"), first_submit, last_update from",
      Fragment.const(
        s"""(
           |  select id, receive_t as received_at, min(submit_t) as first_submit, max(update_t) as last_update
           |  from $tempTable group by id, receive_t
           |) as times""".stripMargin
      ),
      Fragment.const(s"left join $pivotTable using (id)"),
      fr"order by received_at" ++ order
    ).combineAll

    for {
      _ <- createTempTable.update.run
      _ <- buildPivot.update.run
      summaries <- getSummaryInfo.query[RequestSummary].to[List]
    } yield {
      summaries
    }
  }

  /**
    * Record a new batch of transfers.
    *
    * Transfer payloads will be validated against a configured schema before
    * being recorded in the DB. Transfers are not immediately submitted to Kafka upon
    * being recorded; instead, a periodic sweeper pulls pending messages from the DB
    * to maintain a configured parallelism factor.
    */
  def recordRequest(request: BulkRequest): IO[RequestAck] =
    for {
      _ <- validateRequests(request.transfers)
      _ <- logger.info(
        s"Received ${request.transfers.length} transfers"
      )
      now <- clk
        .realTime(scala.concurrent.duration.MILLISECONDS)
        .map(Instant.ofEpochMilli)
      ack <- recordTransferRequest(request, now).transact(dbClient)
    } yield {
      ack
    }

  /** Record a batch of validated transfers with the given ID. */
  private def recordTransferRequest(
    request: BulkRequest,
    now: Instant
  ): ConnectionIO[RequestAck] = {
    val requestId = UUID.randomUUID()

    val updateSql = List(
      Fragment.const(s"insert into $RequestsTable (id, received_at) values"),
      fr"($requestId," ++ Fragment.const(timestampSql(now)) ++ fr")"
    ).combineAll

    for {
      requestId <- updateSql.update.withUniqueGeneratedKeys[UUID]("id")
      transferInfo = request.transfers.map { body =>
        (UUID.randomUUID(), requestId, TransferStatus.Pending: TransferStatus, body, -1)
      }
      transferCount <- Update[(UUID, UUID, TransferStatus, Json, Int)](
        s"insert into $TransfersTable (id, request_id, status, body, steps_run) values (?, ?, ?, ?, ?)"
      ).updateMany(transferInfo)
    } yield {
      RequestAck(requestId, transferCount)
    }
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
            .flatMap(_ => IO.raiseError(InvalidRequest(errs.map(_.getMessage))))
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
        Fragment.const(s"select 1 from $RequestsTable"),
        fr"where id = $requestId limit 1"
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
        Fragment.const(s"select 1 from $TransfersTable"),
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
    checkAndExec(requestId)(rId => lookupSummaries(Right(rId)).map(_.head))

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
    * Reset the status for a single failed transfer to 'pending' so that it will be re-submitted by the periodic
    * sweeper.
    */
  def reconsiderSingleTransfer(requestId: UUID, transferId: UUID): IO[RequestAck] =
    checkAndExec(requestId, transferId) { (rId, tId) =>
      List(
        Fragment.const(s"update $TransfersTable t"),
        fr"set status = ${TransferStatus.Pending: TransferStatus}",
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
        fr"select t.id, t.status, t.body, t.submitted_at, t.updated_at, t.info from",
        TransfersJoinTable,
        Fragments.whereAnd(fr"r.id = $rId", fr"t.id = $tId")
      ).combineAll
        .query[TransferDetails]
        .unique
    }

  /** Get the total number of transfers stored by Transporter. */
  def CountTransfers: IO[Long] =
    Fragment
      .const(s"select count(1) from $TransfersTable")
      .query[Long]
      .unique
      .transact(dbClient)

  /**
    * For a request ID, page #, and IDs per page, return the list of associated transfer IDs. Page order should match
    * the order of the input batch.
    */
  def listTransfers(
    requestId: UUID,
    offset: Long,
    limit: Long,
    newestFirst: Boolean
  ): IO[List[TransferDetails]] =
    checkAndExec(requestId) { rId =>
      val order = Fragment.const(if (newestFirst) "desc" else "asc")
      List(
        fr"select id, status, body, submitted_at, updated_at, info from $TransfersTable",
        fr"where request_id = $rId order by id " ++ order ++ fr" limit $limit offset $offset"
      ).combineAll
        .query[TransferDetails]
        .to[List]
    }

}
