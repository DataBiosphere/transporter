package org.broadinstitute.transporter.transfer

import java.time.Instant

import cats.effect.{Clock, ContextShift, IO, Resource, Timer}
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
import org.broadinstitute.transporter.kafka.config.KafkaConfig
import org.broadinstitute.transporter.transfer.config.TransferConfig

import scala.concurrent.duration.FiniteDuration

/**
  * Component responsible for pushing queued transfer requests into Kafka.
  *
  * @param requestTopic Kafka topic to push requests into
  * @param maxTransfersInFlight max number of transfers which should be "live" in Kafka
  *                             at any point. Used to implement back-pressure, since our
  *                             approach to incremental progress tracking breaks Kafka's
  *                             simple built-in implementation
  * @param submissionInterval amount of time to wait between submissions
  * @param dbClient client which can interact with Transporter's backing DB
  * @param producer client which can push messages into Kafka
  */
class TransferSubmitter private[transfer] (
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

  /**
    * Stream which, when run, triggers message submission at a fixed interval.
    *
    * The stream emits the number of messages pushed into Kafka after each submission.
    */
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
  private[transfer] def submitEligibleTransfers: IO[Int] =
    for {
      now <- clk
        .realTime(scala.concurrent.duration.MILLISECONDS)
        .map(Instant.ofEpochMilli)
      extractPushAndMark = for {
        submissionBatch <- extractSubmissionBatch(now)
        _ <- producer.submit(requestTopic, submissionBatch).to[ConnectionIO]
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
  ): ConnectionIO[List[TransferMessage[Json]]] =
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
  ): ConnectionIO[List[TransferMessage[Json]]] = {
    val batchSelect = List(
      fr"select t.body, t.id, r.id as request_id from",
      TransfersJoinTable,
      Fragments.whereAnd(
        fr"t.status = ${TransferStatus.Pending: TransferStatus}"
      ),
      fr"order by priority desc limit $batchLimit"
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
      .withGeneratedKeys[TransferMessage[Json]](
        "request_id",
        "id",
        "body"
      )
      .compile
      .toList
  }
}

object TransferSubmitter {
  import org.broadinstitute.transporter.kafka.Serdes._

  /**
    * Build a submitter wrapped in setup / teardown logic for its underlying clients.
    *
    * @param dbClient client which can interact with Transporter's backing DB
    * @param transferConfig parameters determining how transfer batches should
    *                       be constructed
    * @param kafkaConfig parameters determining how Transporter should communicate
    *                    with Kafka
    */
  def resource(
    dbClient: Transactor[IO],
    transferConfig: TransferConfig,
    kafkaConfig: KafkaConfig
  )(implicit cs: ContextShift[IO], t: Timer[IO]): Resource[IO, TransferSubmitter] =
    KafkaProducer.resource[Json](kafkaConfig.connection).map { producer =>
      new TransferSubmitter(
        kafkaConfig.topics.requestTopic,
        transferConfig.maxInFlight,
        transferConfig.submissionInterval,
        dbClient,
        producer
      )
    }
}
