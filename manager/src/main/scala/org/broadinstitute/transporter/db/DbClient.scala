package org.broadinstitute.transporter.db

import java.sql.Timestamp
import java.time.{OffsetDateTime, ZoneId}
import java.util.UUID

import cats.effect.{Async, Clock, ContextShift, IO, Resource}
import cats.implicits._
import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.postgres.{Instances => PostgresInstances}
import doobie.postgres.circe.Instances.JsonInstances
import doobie.util.{ExecutionContexts, Get, Put}
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.config.DbConfig
import org.broadinstitute.transporter.queue.QueueSchema
import org.broadinstitute.transporter.queue.api.{Queue, QueueParameters}
import org.broadinstitute.transporter.transfer._
import org.broadinstitute.transporter.transfer.api.{
  BulkRequest,
  TransferDetails,
  TransferMessage
}

import scala.concurrent.ExecutionContext

/**
  * Client responsible for sending requests to / parsing responses from
  * Transporter's backing DB.
  */
trait DbClient {

  /** Check if the client can interact with the backing DB. */
  def checkReady: IO[Boolean]

  /**
    * Create a new queue resource in the DB.
    *
    * Will fail if a queue already exists with the input's name.
    *
    * @param id unique ID to assign to the new queue
    * @param queue information to persist about the new queue
    */
  def createQueue(id: UUID, queue: Queue): IO[Unit]

  /** Update the registered parameters for the queue with the given ID. */
  def patchQueueParameters(id: UUID, params: QueueParameters): IO[Unit]

  /**
    * Remove the queue resource with the given ID from the DB.
    *
    * No-ops if no such queue exists in the DB.
    */
  def deleteQueue(id: UUID): IO[Unit]

  /** Pull the queue resource with the given name from the DB, if it exists. */
  def lookupQueue(name: String): IO[Option[(UUID, Queue)]]

  /**
    * Insert a top-level "transfer request" row and corresponding per-request "transfer"
    * rows into the DB, returning the unique ID of the request.
    *
    * @param queueId ID of the queue to associate with the top-level transfer
    * @param request top-level description of the batch request to insert
    */
  def recordTransferRequest(queueId: UUID, request: BulkRequest): IO[UUID]

  /** Check if an ID is registered to a transfer request under a given queue. */
  def checkRequestInQueue(queueId: UUID, requestId: UUID): IO[Boolean]

  /** Fetch summary info for transfers in a request, grouped by current status. */
  def summarizeTransfersByStatus(
    queueId: UUID,
    requestId: UUID
  ): IO[Map[TransferStatus, (Long, Option[OffsetDateTime], Option[OffsetDateTime])]]

  /** Fetch information about transfers under a request which have a given status. */
  def lookupTransferMessages(
    queueId: UUID,
    requestId: UUID,
    status: TransferStatus
  ): IO[List[TransferMessage]]

  /**
    * Reset the state of all failed transfers under a request to 'pending',
    * to be picked up by the submission sweeper.
    */
  def resetTransferFailures(queueId: UUID, requestId: UUID): IO[Unit]

  /** Fetch detailed information about a single transfer. */
  def lookupTransferDetails(
    queueId: UUID,
    requestId: UUID,
    transferId: UUID
  ): IO[Option[TransferDetails]]

  /**
    * Extract a set of transfers which are eligible for submission from the DB,
    * submit them, and mark their state change.
    *
    * The submission action is run within the context of the DB transaction so
    * that if it fails, the DB state change will be rolled back.
    *
    * @param doSubmit logic to run on the extracted set of transfers to actually
    *                 submit them to agents
    */
  def submitTransfers[Out](
    doSubmit: List[(String, List[TransferRequest[Json]])] => IO[Out]
  ): IO[Out]

  /**
    * Update the current view of the status of a set of in-flight transfers
    * based on a batch of results returned by some number of agents.
    *
    * @param summariesByTopic batch of (response-topic -> summary) messages pulled from
    *                         Kafka which should be pushed into the DB
    */
  def updateTransfers(summariesByTopic: List[(String, TransferSummary[Json])]): IO[Unit]
}

object DbClient extends PostgresInstances with JsonInstances {

  // Recommendation from Hikari docs:
  // https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing#the-formula
  private val MaxDbConnections = (2 * Runtime.getRuntime.availableProcessors) + 1

  /**
    * Construct a DB client, wrapped in logic which will:
    *   1. Automatically spin up a Hikari connection pool on startup, and
    *   2. Automatically clean up the connection pool on shutdown
    *
    * @param config settings for the underlying DB transactor powering the client
    * @param blockingEc execution context which should run all blocking
    *                   I/O required by the DB transactor
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  def resource(config: DbConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO],
    clk: Clock[IO]
  ): Resource[IO, DbClient] =
    for {
      // Recommended by doobie docs: use a fixed-size thread pool to avoid flooding the DB.
      connectionContext <- ExecutionContexts.fixedThreadPool[IO](MaxDbConnections)
      // NOTE: Lines beneath here are from doobie's implementation of `HikariTransactor.newHikariTransactor`.
      // Have to open up the guts to set detailed configuration.
      _ <- Resource.liftF(IO.delay(Class.forName(config.driverClassname)))
      transactor <- HikariTransactor.initial[IO](
        connectEC = connectionContext,
        transactEC = blockingEc
      )
      _ <- Resource.liftF {
        transactor.configure { dataSource =>
          IO.delay {
            // Basic connection config:
            dataSource.setJdbcUrl(config.connectURL)
            dataSource.setUsername(config.username)
            dataSource.setPassword(config.password)

            // Turn knobs here:
            dataSource.setMaximumPoolSize(MaxDbConnections)
            dataSource.setConnectionTimeout(config.timeouts.connectionTimeout.toMillis)
            dataSource.setMaxLifetime(config.timeouts.maxConnectionLifetime.toMillis)
            dataSource.setValidationTimeout(
              config.timeouts.connectionValidationTimeout.toMillis
            )
            dataSource.setLeakDetectionThreshold(
              config.timeouts.leakDetectionThreshold.toMillis
            )
          }
        }
      }
    } yield {
      new Impl(transactor)
    }

  /*
   * "Orphan" instances describing how to get/put our custom JSON schema
   * type from/into the DB.
   *
   * We define them here instead of in the schema class' companion object
   * because the schema class lives in our "common" subproject, which doesn't
   * need to depend on anything doobie-related.
   */
  implicit val schemaGet: Get[QueueSchema] = pgDecoderGet
  implicit val schemaPut: Put[QueueSchema] = pgEncoderPut

  implicit val odtGet: Get[OffsetDateTime] = Get[Timestamp].map { ts =>
    OffsetDateTime.ofInstant(ts.toInstant, ZoneId.of("UTC"))
  }

  /**
    * Concrete implementation of our DB client used by mainline code.
    *
    * @param transactor wrapper around a source of DB connections which can
    *                   actually run SQL
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    * @see https://tpolecat.github.io/doobie/docs/01-Introduction.html for
    *      documentation on `doobie`, the library used for DB access
    */
  private[db] class Impl(transactor: Transactor[IO])(
    implicit cs: ContextShift[IO],
    clk: Clock[IO]
  ) extends DbClient {

    private val logger = Slf4jLogger.getLogger[IO]
    private implicit val logHandler: LogHandler = DbLogHandler(logger)

    override def checkReady: IO[Boolean] = {
      val check = for {
        _ <- logger.info("Running status check against DB...")
        isValid <- doobie.free.connection.isValid(0).transact(transactor)
      } yield {
        isValid
      }

      check.handleErrorWith { err =>
        logger.error(err)("DB status check hit error").as(false)
      }
    }

    override def createQueue(id: UUID, queue: Queue): IO[Unit] =
      sql"""insert into queues
            (id, name, request_topic, progress_topic, response_topic, request_schema, max_in_flight)
            values (
              $id,
              ${queue.name},
              ${queue.requestTopic},
              ${queue.progressTopic},
              ${queue.responseTopic},
              ${queue.schema},
              ${queue.maxConcurrentTransfers}
            )""".update.run.transact(transactor).flatMap { numUpdated =>
        if (numUpdated == 1) {
          IO.unit
        } else {
          IO.raiseError(
            new RuntimeException(s"Queue INSERT command for $id updated $numUpdated rows")
          )
        }
      }

    override def patchQueueParameters(id: UUID, params: QueueParameters): IO[Unit] = {
      val updates = Fragments.setOpt(
        params.schema.map(s => fr"request_schema = $s"),
        params.maxConcurrentTransfers.map(m => fr"max_in_flight = $m")
      )

      (fr"update queues" ++ updates ++ fr"where id = $id").update.run.void
        .transact(transactor)
    }

    override def deleteQueue(id: UUID): IO[Unit] =
      sql"""delete from queues where id = $id""".update.run.void.transact(transactor)

    override def lookupQueue(name: String): IO[Option[(UUID, Queue)]] =
      sql"""select id, name, request_topic, progress_topic, response_topic, request_schema, max_in_flight
          from queues where name = $name"""
        .query[(UUID, Queue)]
        .option
        .transact(transactor)

    override def recordTransferRequest(queueId: UUID, request: BulkRequest): IO[UUID] = {
      val requestId = UUID.randomUUID()
      val transferInfo = request.transfers.map { body =>
        (UUID.randomUUID(), requestId, TransferStatus.Pending: TransferStatus, body)
      }

      val insertRequest =
        sql"insert into transfer_requests (id, queue_id) values ($requestId, $queueId)".update.run.void

      val insertTransfers = Update[(UUID, UUID, TransferStatus, Json)](
        "insert into transfers (id, request_id, status, body) values (?, ?, ?, ?)"
      ).updateMany(transferInfo).void

      insertRequest.flatMap(_ => insertTransfers).transact(transactor).as(requestId)
    }

    override def checkRequestInQueue(queueId: UUID, requestId: UUID): IO[Boolean] =
      List(
        fr"select count(1) from transfer_requests r join queues q on r.queue_id = q.id",
        Fragments.whereAnd(fr"q.id = $queueId", fr"r.id = $requestId")
      ).reduce(_ ++ _)
        .query[Long]
        .unique
        .map(_ > 0L)
        .transact(transactor)

    private val transfersJoinTable =
      fr"""transfers t
           join transfer_requests r on t.request_id = r.id
           join queues q on r.queue_id = q.id"""

    override def summarizeTransfersByStatus(
      queueId: UUID,
      requestId: UUID
    ): IO[Map[TransferStatus, (Long, Option[OffsetDateTime], Option[OffsetDateTime])]] =
      List(
        fr"select t.status, count(*), min(t.submitted_at), max(t.updated_at) from",
        transfersJoinTable,
        Fragments.whereAnd(fr"q.id = $queueId", fr"r.id = $requestId"),
        fr"group by t.status"
      ).reduce(_ ++ _)
        .query[
          (TransferStatus, Long, Option[OffsetDateTime], Option[OffsetDateTime])
        ]
        .to[List]
        .map(_.map {
          case (status, count, firstSubmitted, lastUpdated) =>
            (status, (count, firstSubmitted, lastUpdated))
        }.toMap)
        .transact(transactor)

    override def lookupTransferMessages(
      queueId: UUID,
      requestId: UUID,
      status: TransferStatus
    ): IO[List[TransferMessage]] =
      List(
        fr"select t.id, t.info from",
        transfersJoinTable,
        Fragments.whereAnd(
          fr"q.id = $queueId",
          fr"r.id = $requestId",
          fr"t.status = $status",
          fr"t.info is not null"
        )
      ).reduce(_ ++ _)
        .query[TransferMessage]
        .to[List]
        .transact(transactor)

    override def resetTransferFailures(queueId: UUID, requestId: UUID): IO[Unit] =
      List(
        fr"update transfers t set status = ${TransferStatus.Pending: TransferStatus}",
        fr"from transfer_requests r, queues q",
        Fragments.whereAnd(
          fr"t.request_id = r.id",
          fr"r.queue_id = q.id",
          fr"q.id = $queueId",
          fr"r.id = $requestId",
          fr"t.status = ${TransferStatus.Failed: TransferStatus}"
        )
      ).reduce(_ ++ _).update.run.void.transact(transactor)

    override def lookupTransferDetails(
      queueId: UUID,
      requestId: UUID,
      transferId: UUID
    ): IO[Option[TransferDetails]] =
      List(
        fr"select t.id, t.status, t.body, t.submitted_at, t.updated_at, t.info from",
        transfersJoinTable,
        Fragments.whereAnd(
          fr"q.id = $queueId",
          fr"r.id = $requestId",
          fr"t.id = $transferId"
        )
      ).reduce(_ ++ _)
        .query[TransferDetails]
        .option
        .transact(transactor)

    override def submitTransfers[Out](
      doSubmit: List[(String, List[TransferRequest[Json]])] => IO[Out]
    ): IO[Out] =
      for {
        now <- clk.realTime(scala.concurrent.duration.MILLISECONDS)
        out <- submitTransfers(doSubmit, now).transact(transactor)
      } yield {
        out
      }

    private def submitTransfers[Out](
      doSubmit: List[(String, List[TransferRequest[Json]])] => IO[Out],
      nowMillis: Long
    ): ConnectionIO[Out] = {
      val connAsync = Async[ConnectionIO]
      for {
        /*
         * Lock the transfers table up-front to prevent submitting a transfer
         * multiple times on concurrent access from multiple manager apps.
         */
        _ <- sql"lock table transfers in share row exclusive mode".update.run
        submittableCounts <- currentSubmittableCounts
        submission <- submittableCounts.traverse {
          case (topic, count) =>
            prepSubmissionBatch(topic, count, nowMillis).map(topic -> _)
        }
        out <- connAsync.liftIO(doSubmit(submission))
      } yield {
        out
      }
    }

    /**
      * Build a statement which will get a mapping from Kafka topic name to the
      * number of requests which could currently be submitted to that topic.
      *
      * The eligible request count is computed based on:
      *   1. The number of pending transfers registered in the DB
      *   2. The number of submitted transfers registered in the DB
      *   3. The maximum number of in-flight transfers configured for the queue
      */
    private def currentSubmittableCounts: ConnectionIO[List[(String, Long)]] =
      for {
        maxCounts <- sql"select request_topic, max_in_flight from queues"
          .query[(String, Long)]
          .to[List]
          .map(_.toMap)
        submittedCount <- getCountsInState(TransferStatus.Submitted)
      } yield {
        maxCounts.map {
          case (topic, max) =>
            topic -> math.max(0L, max - submittedCount.getOrElse(topic, 0L))
        }.toList
      }

    /**
      * Build a statement which will get the number of transfers associated with
      * each request topic registered in the DB which have a particular status.
      */
    private def getCountsInState(
      status: TransferStatus
    ): ConnectionIO[Map[String, Long]] =
      List(
        fr"select q.request_topic, count(t.id) from",
        transfersJoinTable,
        fr"where t.status = $status group by q.id"
      ).reduce(_ ++ _)
        .query[(String, Long)]
        .to[List]
        .map(_.toMap)

    /**
      * Build a statement which will mark a batch of transfers associated with
      * a request topic as 'submitted', returning those transfers for use in
      * the actual submission logic.
      *
      * @param requestTopic Kafka topic that the requests will be submitted into
      * @param batchLimit maximum number of transfers to mark and extract
      * @param nowMillis epoch timestamp to mark each of the extracted transfers with
      */
    private def prepSubmissionBatch(
      requestTopic: String,
      batchLimit: Long,
      nowMillis: Long
    ): ConnectionIO[List[TransferRequest[Json]]] = {
      val batch_select = List(
        fr"select t.body, t.id, r.id as request_id from",
        transfersJoinTable,
        Fragments.whereAnd(
          fr"q.request_topic = $requestTopic",
          fr"t.status = ${TransferStatus.Pending: TransferStatus}"
        ),
        fr"limit $batchLimit"
      ).reduce(_ ++ _)

      List(
        fr"with submission_batch as",
        Fragments.parentheses(batch_select),
        fr"update transfers",
        Fragments.set(
          fr"status = ${TransferStatus.Submitted: TransferStatus}",
          fr"submitted_at =" ++ Fragment.const(timestampSql(nowMillis))
        ),
        fr"from submission_batch where transfers.id = submission_batch.id",
        fr"returning submission_batch.body, submission_batch.id, submission_batch.request_id"
      ).reduce(_ ++ _)
        .update
        .withGeneratedKeys[TransferRequest[Json]]("body", "id", "request_id")
        .compile
        .toList
    }

    override def updateTransfers(
      summariesByTopic: List[(String, TransferSummary[Json])]
    ): IO[Unit] =
      for {
        now <- clk.realTime(scala.concurrent.duration.MILLISECONDS)
        _ <- updateTransfers(summariesByTopic, now).transact(transactor)
      } yield ()

    /** Build a transaction which will update info stored for in-flight transfers. */
    private def updateTransfers(
      summariesByTopic: List[(String, TransferSummary[Json])],
      nowMillis: Long
    ): ConnectionIO[Unit] = {
      val summaryData = summariesByTopic.map {
        case (responseTopic, summary) =>
          val status = summary.result match {
            case TransferResult.Success      => TransferStatus.Succeeded
            case TransferResult.FatalFailure => TransferStatus.Failed
          }

          (status, summary.info, summary.id, summary.requestId, responseTopic)
      }

      Update[(TransferStatus, Json, UUID, UUID, String)](
        s"""update transfers
           |set status = ?, info = ?, updated_at = ${timestampSql(nowMillis)}
           |from transfer_requests, queues
           |where transfers.request_id = transfer_requests.id and transfer_requests.queue_id = queues.id
           |and transfers.id = ? and transfer_requests.id = ? and queues.response_topic = ?""".stripMargin
      ).updateMany(summaryData).void
    }

    private def timestampSql(millis: Long): String =
      s"TO_TIMESTAMP($millis::double precision / 1000)"
  }
}
