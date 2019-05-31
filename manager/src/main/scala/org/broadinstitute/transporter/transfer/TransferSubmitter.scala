package org.broadinstitute.transporter.transfer

import java.time.Instant

import cats.effect.{Clock, IO, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.{Constants, DbLogHandler, DoobieInstances}
import org.broadinstitute.transporter.kafka.KafkaProducer

import scala.concurrent.duration.FiniteDuration

class TransferSubmitter(
  requestTopic: String,
  maxTransfersInFlight: Long,
  submissionInterval: FiniteDuration,
  dbClient: Transactor[IO],
  producer: KafkaProducer[Json]
)(implicit clk: Clock[IO], t: Timer[IO]) {

  import Constants._
  import DoobieInstances._

  private val logger = Slf4jLogger.getLogger[IO]
  private implicit val logHandler: LogHandler = DbLogHandler(logger)

  def sweepSubmissions: Stream[IO, Int] =
    Stream.fixedDelay(submissionInterval).evalMap(_ => submitEligibleTransfers)

  /**
    * Find and submit as many eligible pending transfers as possible to Kafka.
    *
    * Eligibility is determined based on the number of transfers currently running
    * and the configured max parallelism.
    *
    * Submission to Kafka runs within the DB transaction which updates transfer status,
    * so that if submission fails the transfers remain eligible for submission on a
    * following sweep.
    */
  private def submitEligibleTransfers: IO[Int] =
    for {
      now <- clk
        .realTime(scala.concurrent.duration.MILLISECONDS)
        .map(Instant.ofEpochMilli)
      extractPushAndMark = for {
        submissionBatch <- extractSubmissionBatch(now)
        submissionsByTopic = submissionBatch.foldMap {
          case (ids, message) => Map(requestTopic -> List(ids -> message))
        }.toList
        _ <- producer.submit(submissionsByTopic).to[ConnectionIO]
      } yield {
        submissionBatch.length
      }
      _ <- logger.info("Submitting eligible transfers to Kafka...")
      numSubmitted <- extractPushAndMark.transact(dbClient)
      _ <- logger.info(s"Submitted $numSubmitted transfers")
    } yield {
      numSubmitted
    }

  /** Find as many eligible pending transfers as possible, and mark them for submission. */
  private def extractSubmissionBatch(
    submitTime: Instant
  ): ConnectionIO[List[(TransferIds, Json)]] =
    for {
      /*
       * Lock the transfers table up-front to prevent submitting a transfer
       * multiple times on concurrent access from multiple manager apps.
       */
      _ <- Fragment
        .const(s"lock table $TransfersTable in share row exclusive mode")
        .update
        .run
      submittableCount <- currentSubmittableCount
      submission <- prepSubmissionBatch(submittableCount, submitTime)
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
    *   3. The maximum number of in-flight transfers
    */
  private def currentSubmittableCount: ConnectionIO[Long] =
    List(
      fr"select t.status, count(t.status) from",
      TransfersJoinTable,
      fr"group by t.status"
    ).combineAll
      .query[(TransferStatus, Long)]
      .to[List]
      .map { countList =>
        val countsByState = countList.toMap
        val totalSubmitted = countsByState.getOrElse(TransferStatus.Submitted, 0L) +
          countsByState.getOrElse(TransferStatus.InProgress, 0L)
        math.max(0L, maxTransfersInFlight - totalSubmitted)
      }

  /**
    * Build a statement which will mark a batch of transfers as 'submitted',
    * returning those transfers for use in the actual submission logic.
    */
  private def prepSubmissionBatch(
    batchLimit: Long,
    now: Instant
  ): ConnectionIO[List[(TransferIds, Json)]] = {
    val batchSelect = List(
      fr"select t.body, t.id, r.id as request_id from",
      TransfersJoinTable,
      Fragments.whereAnd(
        fr"t.status = ${TransferStatus.Pending: TransferStatus}"
      ),
      fr"limit $batchLimit"
    ).combineAll

    List(
      fr"with batch as",
      Fragments.parentheses(batchSelect),
      Fragment.const(s"update $TransfersTable"),
      Fragments.set(
        fr"status = ${TransferStatus.Submitted: TransferStatus}",
        fr"submitted_at =" ++ Fragment.const(timestampSql(now))
      ),
      Fragment.const(s"from batch where $TransfersTable.id = batch.id"),
      fr"returning batch.request_id, batch.id, batch.body"
    ).combineAll.update
      .withGeneratedKeys[(TransferIds, Json)](
        "request_id",
        "id",
        "body"
      )
      .compile
      .toList
  }
}
