package org.broadinstitute.transporter.transfer

import java.util.UUID

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
) {

  import DoobieInstances._
  import Constants._

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  def listRequests(offset: Long, limit: Long): IO[List[RequestSummary]] =
    lookupSummaries(Left((offset, limit))).transact(dbClient)

  private def lookupSummaries(
    paginationOrId: Either[(Long, Long), UUID]
  ): ConnectionIO[List[RequestSummary]] = {
    val filterOrLimit = paginationOrId.fold(
      { case (offset, limit) => fr"order by r.id limit $limit offset $offset" },
      id => fr"where r.id = $id"
    )

    val tempTable = "id_status_summaries"

    // The 'crosstab' function needs a real (if temporary) table to work.
    val createTempTable = List(
      Fragment.const(s"create temporary table $tempTable as"),
      fr"select r.id as id, t.status as status,",
      fr"min(t.submitted_at) as submit_t, max(t.updated_at) as update_t, count(t.id) as n",
      fr"from" ++ TransfersJoinTable,
      filterOrLimit,
      fr"group by r.id, t.status"
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
      fr"select id, json_build_object(" ++ statusKvPairs ++ fr"), first_submit, last_update from",
      Fragment.const(
        s"(select id, min(submit_t) as first_submit, max(update_t) as last_update from $tempTable group by id) as times"
      ),
      Fragment.const(s"left join $pivotTable using (id)")
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
      details <- maybeDetails.liftTo[IO](NotFound(requestId, Some(transferId)))
    } yield {
      details
    }
}
