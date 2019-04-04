package org.broadinstitute.transporter.transfer

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import cats.implicits._
import io.chrisdavenport.fuuid.FUUID
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.{KafkaConsumer, KafkaProducer}

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
    consumer: KafkaConsumer[FUUID, TransferSummary],
    producer: KafkaProducer[FUUID, Json],
    dbClient: DbClient
  )(implicit cs: ContextShift[IO]): ResultListener =
    new Impl(consumer, producer, dbClient)

  /**
    * Concrete listener implementation used by mainline app code.
    *
    * @param consumer Kafka consumer subscribed to Transporter result topics
    * @param producer Kafka producer to use for resubmitting transient failures
    * @param dbClient DB client to use for persisting transfer results
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  private[transfer] class Impl(
    consumer: KafkaConsumer[FUUID, TransferSummary],
    producer: KafkaProducer[FUUID, Json],
    dbClient: DbClient
  )(implicit cs: ContextShift[IO])
      extends ResultListener {

    private val logger = Slf4jLogger.getLogger[IO]

    override def processResults: IO[Unit] = consumer.runForeach(processBatch)

    /** Process a single batch of results received from some number of Transporter agents. */
    private[transfer] def processBatch(
      batch: List[KafkaConsumer.Attempt[(FUUID, TransferSummary)]]
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
        _ <- resubmitTransientFailures(results)
      } yield ()
    }

    /**
      * Resubmit transfers which failed on transient errors.
      *
      * Whether or not an error is transient is determined by the reporting agent,
      * not the manager service.
      */
    private def resubmitTransientFailures(
      results: List[(FUUID, TransferSummary)]
    ): IO[Unit] = {
      val transientFailures = results.collect {
        case (id, TransferSummary(TransferResult.TransientFailure, _)) => id
      }

      NonEmptyList.fromList(transientFailures).fold(IO.unit) { toRetry =>
        for {
          _ <- logger.info(s"Resubmitting ${toRetry.length} transient failures")
          resubmitInfo <- dbClient.getResubmitInfo(toRetry)
          resubmissionsByTopic = resubmitInfo.groupBy(_.requestTopic)
          _ <- resubmissionsByTopic.toList.parTraverse {
            case (topic, messages) =>
              producer.submit(topic, messages.map(m => m.transferId -> m.transferBody))
          }
        } yield ()
      }
    }
  }
}
