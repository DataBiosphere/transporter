package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaConsumer

/**
  * Component responsible for processing summary messages received
  * from Transporter agents over Kafka.
  *
  * Coordinates persisting results to the DB and resubmitting transfers
  * that fail on transient errors.
  */
trait ResultListener {

  /**
    * Launch the stream that will listen to & process all results
    * pulled from Kafka.
    *
    * NOTE: The returned `IO` won't complete except on error / cancellation;
    * run it concurrently as a background thread.
    */
  def processResults: IO[Unit]
}

object ResultListener {

  // Pseudo-constructor for the Impl subclass.
  def apply(
    consumer: KafkaConsumer[FUUID, TransferSummary[Option[Json]]],
    dbClient: DbClient
  )(implicit cs: ContextShift[IO]): ResultListener =
    new Impl(consumer, dbClient)

  /**
    * Concrete listener implementation used by mainline app code.
    *
    * @param consumer Kafka consumer subscribed to Transporter result topics
    * @param dbClient DB client to use for persisting transfer results
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  private[transfer] class Impl(
    consumer: KafkaConsumer[FUUID, TransferSummary[Option[Json]]],
    dbClient: DbClient
  )(implicit cs: ContextShift[IO])
      extends ResultListener {

    private val logger = Slf4jLogger.getLogger[IO]

    override def processResults: IO[Unit] = consumer.runForeach(processBatch)

    /** Process a single batch of results received from some number of Transporter agents. */
    private[transfer] def processBatch(
      batch: List[KafkaConsumer.Attempt[(FUUID, TransferSummary[Option[Json]])]]
    ): IO[Unit] = {
      val (numMalformed, results) = batch.foldMap {
        case Right(res) => (0, List(res))
        case Left(_)    => (1, Nil)
      }

      val logPrefix = s"Recording ${results.length} transfer results"

      for {
        _ <- if (numMalformed > 0) {
          logger.warn(s"$logPrefix; ignoring $numMalformed malformed entities in batch")
        } else {
          logger.info(logPrefix)
        }
        _ <- dbClient.updateTransfers(results)
      } yield ()
    }
  }
}
