package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.{Chunk, Stream}
import fs2.kafka.{
  AutoOffsetReset,
  CommittableOffset,
  ConsumerSettings,
  Deserializer,
  KafkaConsumer => KConsumer
}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.kafka.config.{ConnectionConfig, ConsumerConfig}
import org.broadinstitute.transporter.transfer.TransferMessage

import scala.concurrent.duration.FiniteDuration

/**
  * Client responsible for processing messages from a single Kafka subscription.
  *
  * Raw Kafka consumer instances are configured to work with specific key/value types,
  * so this class does the same.
  *
  * @tparam M type which messages pulled from Kafka should be parsed into
  */
trait KafkaConsumer[M] {

  /**
    * Stream emitting batches of messages pulled from Kafka, paired with their
    * corresponding offsets.
    *
    * Processors of this stream must commit each offset after processing its
    * paired message to let the Kafka broker know that it's been handled.
    */
  def stream: Stream[IO, Chunk[(TransferMessage[M], CommittableOffset[IO])]]
}

object KafkaConsumer {

  /**
    * Partially set up a Kafka consumer so that the eventually-produced
    * consumer will read messages from a single topic.
    */
  def ofTopic[M](
    topic: String,
    connectionConfig: ConnectionConfig,
    consumerConfig: ConsumerConfig
  )(
    implicit cs: ContextShift[IO],
    t: Timer[IO],
    d: Deserializer.Attempt[TransferMessage[M]]
  ): Resource[IO, KafkaConsumer[M]] = {
    val built = for {
      settings <- consumerSettings[Unit, Serdes.Attempt[TransferMessage[M]]](
        connectionConfig,
        consumerConfig
      )
      consumer <- fs2.kafka.consumerResource[IO].using(settings)
    } yield {
      consumer
    }

    built.evalMap { c =>
      c.subscribeTo(topic).as {
        new Impl(c, consumerConfig.maxRecordsPerBatch, consumerConfig.waitTimePerBatch)
      }
    }
  }

  /**
    * Build settings for a [[org.apache.kafka.clients.consumer.Consumer]] from our config types.
    *
    * Some settings in the output are hard-coded to prevent silent data loss in the consumer,
    * which isn't acceptable for our use-case.
    */
  private def consumerSettings[K: Deserializer, V: Deserializer](
    conn: ConnectionConfig,
    consumer: ConsumerConfig
  ) =
    // NOTE: This EC is backed by a single-threaded pool. If a pool with > 1 thread is created,
    // the Kafka libs will detect the possibility of concurrent modification and throw an exception.
    fs2.kafka.consumerExecutionContextResource[IO].map { actorEc =>
      ConsumerSettings[K, V](actorEc)
      // Required to connect to Kafka at all.
        .withBootstrapServers(conn.bootstrapServers.intercalate(","))
        .withProperties(ConnectionConfig.securityProperties(conn.tls, conn.scramSha))
        // Required to be the same across all instances of a single application,
        // to avoid duplicate message processing.
        .withGroupId(consumer.groupId)
        // For debugging on the Kafka server; adds an ID to the logs.
        .withClientId(conn.clientId)
        // Force manual commits to avoid accidental data loss.
        .withEnableAutoCommit(false)
        /*
         * When subscribing to a topic for the first time, start from the earliest message
         * instead of the latest. Prevents accidentally losing data if a producer writes to
         * a topic before the consumer subscribes to it.
         */
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        // No "official" recommendation on these values, we can tweak as we see fit.
        .withRequestTimeout(conn.requestTimeout)
        .withCloseTimeout(conn.closeTimeout)
    }

  /**
    * Concrete implementation of our consumer used by mainline code.
    *
    * @param consumer client which can pull "raw" messages from Kafka.
    *                 NOTE: This class assumes a subscription has already
    *                 been initialized in the consumer
    * @param maxPerBatch max number of messages to pull from Kafka before emitting
    *                    the accumulated batch to downstream processors
    * @param waitTimePerBatch max time to wait for the buffer to fill to `maxPerBatch`
    *                         before emitting what's been received so far to downstream
    *                         processors
    */
  private[kafka] class Impl[M](
    consumer: KConsumer[IO, Unit, Serdes.Attempt[TransferMessage[M]]],
    maxPerBatch: Int,
    waitTimePerBatch: FiniteDuration
  )(implicit cs: ContextShift[IO], t: Timer[IO])
      extends KafkaConsumer[M] {

    private val logger = Slf4jLogger.getLogger[IO]

    override def stream: Stream[IO, Chunk[(TransferMessage[M], CommittableOffset[IO])]] =
      consumer.stream.evalTap { message =>
        logger.info(s"Got message from topic ${message.record.topic}")
      }.map { message =>
        message.record.value().map(_ -> message.committableOffset)
      }.evalTap {
        case Right((m, _)) => logger.debug(s"Decoded message from Kafka: $m")
        case Left(err)     => logger.error(err)("Failed to decode Kafka message")
      }.rethrow.groupWithin(maxPerBatch, waitTimePerBatch)
  }

}
