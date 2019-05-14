package org.broadinstitute.transporter.transfer

import java.time.{Instant, OffsetDateTime}
import java.util.UUID

import cats.Order
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
import org.broadinstitute.transporter.db.DbLogHandler
import org.broadinstitute.transporter.error.{
  InvalidRequest,
  NoSuchQueue,
  NoSuchRequest,
  NoSuchTransfer
}
import org.broadinstitute.transporter.kafka.KafkaProducer
import org.broadinstitute.transporter.queue.QueueSchema
import org.broadinstitute.transporter.transfer.api._

/**
  * Component responsible for updating and retrieving transfer-level information
  * for the Transporter system.
  *
  * @param dbClient client which can interact with Transporter's backing DB
  * @param producer client which can push messages into Transporter's Kafka cluster
  */
class TransferController(
  dbClient: Transactor[IO],
  producer: KafkaProducer[Json]
)(implicit clk: Clock[IO]) {
  import org.broadinstitute.transporter.db.DoobieInstances._

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  /**
    * Record a new batch of transfers under a queue.
    *
    * Transfer payloads will be validated against the queue's recorded schema before
    * being recorded in the DB. Transfers are not immediately submitted to Kafka upon
    * being recorded; instead, a periodic sweeper pulls pending messages from the DB
    * to maintain a user-defined per-queue parallelism factor.
    */
  def recordTransfer(queueName: String, request: BulkRequest): IO[RequestAck] =
    for {
      maybeInfo <- sql"select id, request_schema from queues where name = $queueName"
        .query[(UUID, QueueSchema)]
        .option
        .transact(dbClient)
      (queueId, schema) <- maybeInfo.liftTo[IO](NoSuchQueue(queueName))
      _ <- validateRequests(request.transfers, schema)
      _ <- logger.info(
        s"Submitting ${request.transfers.length} transfers to queue $queueName"
      )
      ack <- recordTransferRequest(queueId, request).transact(dbClient)
    } yield {
      ack
    }

  /** Record a batch of validated transfers under the queue with the given ID. */
  private def recordTransferRequest(
    queueId: UUID,
    request: BulkRequest
  ): ConnectionIO[RequestAck] =
    for {
      requestId <- sql"""insert into transfer_requests (id, queue_id)
             values (${UUID.randomUUID()}, $queueId)""".update
        .withUniqueGeneratedKeys[UUID]("id")
      transferInfo = request.transfers.map { body =>
        (UUID.randomUUID(), requestId, TransferStatus.Pending: TransferStatus, body)
      }
      transferCount <- Update[(UUID, UUID, TransferStatus, Json)](
        "insert into transfers (id, request_id, status, body) values (?, ?, ?, ?)"
      ).updateMany(transferInfo)
    } yield {
      RequestAck(requestId, transferCount)
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
    * Check that a queue with the given name exists, and that a request with the given ID
    * is registered under that queue; then run an operation using the queue's ID as input.
    *
    * Utility for sharing guardrails across request-level operations in the controller.
    */
  private def checkAndExec[Out](queueName: String, requestId: UUID)(
    f: UUID => ConnectionIO[Out]
  ): IO[Out] = {
    val transaction = for {
      maybeId <- sql"select id from queues where name = $queueName".query[UUID].option
      queueId <- maybeId.liftTo[ConnectionIO](NoSuchQueue(queueName))
      requestInQueue <- checkRequestInQueue(queueId, requestId)
      _ <- IO
        .raiseError(NoSuchRequest(queueName, requestId))
        .whenA(!requestInQueue)
        .to[ConnectionIO]
      out <- f(queueId)
    } yield {
      out
    }

    transaction.transact(dbClient)
  }

  /** Check if a request with a certain ID is registered under a specific queue ID. */
  private def checkRequestInQueue(
    queueId: UUID,
    requestId: UUID
  ): ConnectionIO[Boolean] =
    List(
      fr"select count(1) from transfer_requests r join queues q on r.queue_id = q.id",
      Fragments.whereAnd(fr"q.id = $queueId", fr"r.id = $requestId")
    ).combineAll
      .query[Long]
      .unique
      .map(_ > 0L)

  /**
    * SQL fragment linking transfer-level information to queue-level information via
    * the requests table.
    */
  private val transfersJoinTable =
    fr"""transfers t
         join transfer_requests r on t.request_id = r.id
         join queues q on r.queue_id = q.id"""

  /** Get the current summary-level status for a request under a queue. */
  def lookupRequestStatus(
    queueName: String,
    requestId: UUID
  ): IO[RequestStatus] =
    checkAndExec(queueName, requestId) { queueId =>
      List(
        fr"select t.status, count(*), min(t.submitted_at), max(t.updated_at) from",
        transfersJoinTable,
        Fragments.whereAnd(fr"q.id = $queueId", fr"r.id = $requestId"),
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
    * under a request within a queue.
    */
  def lookupRequestOutputs(
    queueName: String,
    requestId: UUID
  ): IO[RequestInfo] =
    lookupRequestInfo(queueName, requestId, TransferStatus.Succeeded)

  /**
    * Get the JSON payloads returned by agents for failed transfers
    * under a request within a queue.
    */
  def lookupRequestFailures(
    queueName: String,
    requestId: UUID
  ): IO[RequestInfo] =
    lookupRequestInfo(queueName, requestId, TransferStatus.Failed)

  /**
    * Get any information collected by the manager from transfers under a previously-submitted
    * request which have a given status.
    */
  private def lookupRequestInfo(
    queueName: String,
    requestId: UUID,
    status: TransferStatus
  ): IO[RequestInfo] =
    checkAndExec(queueName, requestId) { queueId =>
      List(
        fr"select t.id, t.info from",
        transfersJoinTable,
        Fragments.whereAnd(
          fr"q.id = $queueId",
          fr"r.id = $requestId",
          fr"t.status = $status",
          fr"t.info is not null"
        )
      ).combineAll
        .query[TransferInfo]
        .to[List]
    }.map(RequestInfo(requestId, _))

  /**
    * Reset the statuses for all failed transfers under a request in a queue to 'pending',
    * so that they will be re-submitted by the periodic sweeper.
    */
  def reconsiderRequest(queueName: String, requestId: UUID): IO[RequestAck] =
    checkAndExec(queueName, requestId) { queueId =>
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
      ).combineAll.update.run
    }.map(RequestAck(requestId, _))

  /**
    * Get all information stored by Transporter about a specific transfer under a request
    * within a queue.
    */
  def lookupTransferDetails(
    queueName: String,
    requestId: UUID,
    transferId: UUID
  ): IO[TransferDetails] =
    for {
      maybeDetails <- checkAndExec(queueName, requestId) { queueId =>
        List(
          fr"select t.id, t.status, t.body, t.submitted_at, t.updated_at, t.info from",
          transfersJoinTable,
          Fragments.whereAnd(
            fr"q.id = $queueId",
            fr"r.id = $requestId",
            fr"t.id = $transferId"
          )
        ).combineAll
          .query[TransferDetails]
          .option
      }
      details <- maybeDetails.liftTo[IO](
        NoSuchTransfer(queueName, requestId, transferId)
      )
    } yield {
      details
    }

  private def getNow: IO[Instant] =
    clk.realTime(scala.concurrent.duration.MILLISECONDS).map(Instant.ofEpochMilli)

  /**
    * Find and submit as many eligible pending transfers as possible to Kafka.
    *
    * Eligibility is determined on a per-queue basis based on the number of transfers
    * currently running under that queue and the user-defined max parallelism for the queue.
    *
    * Submission to Kafka runs within the DB transaction which updates transfer status,
    * so that if submission fails the transfers remain eligible for submission on a
    * following sweep.
    */
  def submitEligibleTransfers: IO[Unit] =
    for {
      now <- getNow
      extractPushAndMark = for {
        submissionBatch <- extractSubmissionBatch(now)
        submissionsByTopic = submissionBatch.foldMap {
          case (topic, ids, message) => Map(topic -> List(ids -> message))
        }.toList
        _ <- producer.submit(submissionsByTopic).to[ConnectionIO]
      } yield {
        submissionBatch.length
      }
      _ <- logger.info("Submitting eligible transfers to Kafka...")
      numSubmitted <- extractPushAndMark.transact(dbClient)
      _ <- logger.info(s"Submitted $numSubmitted transfers")
    } yield ()

  /** Find as many eligible pending transfers as possible, and mark them for submission. */
  private def extractSubmissionBatch(
    submitTime: Instant
  ): ConnectionIO[List[(String, TransferIds, Json)]] =
    for {
      /*
       * Lock the transfers table up-front to prevent submitting a transfer
       * multiple times on concurrent access from multiple manager apps.
       */
      _ <- sql"lock table transfers in share row exclusive mode".update.run
      submittableCounts <- currentSubmittableCounts
      submission <- submittableCounts.flatTraverse {
        case (id, count) =>
          prepSubmissionBatch(id, count, submitTime)
      }
    } yield {
      submission
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
  private def currentSubmittableCounts: ConnectionIO[List[(UUID, Long)]] =
    for {
      maxCounts <- sql"select id, max_in_flight from queues"
        .query[(UUID, Long)]
        .to[List]
        .map(_.toMap)
      submittedCount <- getCountsInState(TransferStatus.Submitted)
      inProgressCount <- getCountsInState(TransferStatus.InProgress)
    } yield {
      maxCounts.map {
        case (topic, max) =>
          val totalSubmitted =
            submittedCount.getOrElse(topic, 0L) + inProgressCount.getOrElse(topic, 0L)

          topic -> math.max(0L, max - totalSubmitted)
      }.toList
    }

  /**
    * Build a statement which will get the number of transfers associated with
    * each request topic registered in the DB which have a particular status.
    */
  private def getCountsInState(
    status: TransferStatus
  ): ConnectionIO[Map[UUID, Long]] =
    List(
      fr"select q.id, count(t.id) from",
      transfersJoinTable,
      fr"where t.status = $status group by q.id"
    ).combineAll
      .query[(UUID, Long)]
      .to[List]
      .map(_.toMap)

  private def timestampSql(now: Instant): String =
    s"TO_TIMESTAMP(${now.toEpochMilli}::double precision / 1000)"

  /**
    * Build a statement which will mark a batch of transfers associated with
    * a queue as 'submitted', returning those transfers for use in the actual
    * submission logic.
    */
  private def prepSubmissionBatch(
    queueId: UUID,
    batchLimit: Long,
    now: Instant
  ): ConnectionIO[List[(String, TransferIds, Json)]] = {
    val batchSelect = List(
      fr"select t.body, t.id, r.id as request_id, q.id as queue_id, q.request_topic from",
      transfersJoinTable,
      Fragments.whereAnd(
        fr"q.id = $queueId",
        fr"t.status = ${TransferStatus.Pending: TransferStatus}"
      ),
      fr"limit $batchLimit"
    ).combineAll

    List(
      fr"with batch as",
      Fragments.parentheses(batchSelect),
      fr"update transfers",
      Fragments.set(
        fr"status = ${TransferStatus.Submitted: TransferStatus}",
        fr"submitted_at =" ++ Fragment.const(timestampSql(now))
      ),
      fr"from batch where transfers.id = batch.id",
      fr"returning batch.request_topic, batch.queue_id, batch.request_id, batch.id, batch.body"
    ).combineAll.update
      .withGeneratedKeys[(String, TransferIds, Json)](
        "request_topic",
        "queue_id",
        "request_id",
        "id",
        "body"
      )
      .compile
      .toList
  }

  /**
    * Update Transporter's view of a set of transfers based on
    * terminal results collected from Kafka.
    */
  def recordTransferResults(
    results: List[(TransferIds, (TransferResult, Json))]
  ): IO[Unit] =
    for {
      now <- getNow
      statusUpdates = results.map {
        case (ids, (result, info)) =>
          val status = result match {
            case TransferResult.Success      => TransferStatus.Succeeded
            case TransferResult.FatalFailure => TransferStatus.Failed
          }
          (status, info, ids.transfer, ids.request, ids.queue)
      }
      _ <- Update[(TransferStatus, Json, UUID, UUID, UUID)](
        s"""update transfers
           |set status = ?, info = ?, updated_at = ${timestampSql(now)}
           |from transfer_requests, queues
           |where transfers.request_id = transfer_requests.id and transfer_requests.queue_id = queues.id
           |and transfers.id = ? and transfer_requests.id = ? and queues.id = ?""".stripMargin
      ).updateMany(statusUpdates).void.transact(dbClient)
    } yield ()

  /**
    * Update Transporter's view of a set of transfers based on
    * incremental progress messages collected from Kafka.
    */
  def markTransfersInProgress(
    progress: List[(TransferIds, Json)]
  ): IO[Unit] = {
    val statuses = List(TransferStatus.Submitted, TransferStatus.InProgress)
      .map(_.entryName.toLowerCase)
      .mkString("('", "','", "')")
    for {
      now <- getNow
      statusUpdates = progress.map {
        case (ids, info) =>
          (
            TransferStatus.InProgress: TransferStatus,
            info,
            ids.transfer,
            ids.request,
            ids.queue
          )
      }
      _ <- Update[(TransferStatus, Json, UUID, UUID, UUID)](
        s"""update transfers
           |set status = ?, info = ?, updated_at = ${timestampSql(now)}
           |from transfer_requests, queues
           |where transfers.request_id = transfer_requests.id and transfer_requests.queue_id = queues.id
           |and transfers.status in $statuses
           |and transfers.id = ? and transfer_requests.id = ? and queues.id = ?""".stripMargin
      ).updateMany(statusUpdates).void.transact(dbClient)
    } yield ()
  }
}
