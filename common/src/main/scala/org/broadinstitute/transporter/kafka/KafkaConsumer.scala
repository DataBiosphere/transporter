package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.Chunk
import fs2.kafka.{AutoOffsetReset, ConsumerSettings, Deserializer}
import fs2.kafka.{KafkaConsumer => KConsumer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.kafka.config.{ConnectionConfig, ConsumerConfig}
import org.broadinstitute.transporter.transfer.{TransferIds, TransferMessage}

import scala.concurrent.duration.FiniteDuration
import scala.util.matching.Regex

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
    * Run an effecting operation on every batch of messages pulled from the Kafka subscription.
    *
    * The returned `IO` will run until cancelled. Messages will be committed
    * in batches as they are successfully processed by `f`.
    */
  def runForeach(f: List[(TransferIds, M)] => IO[Unit]): IO[Unit]
}

object KafkaConsumer {

  /**
    * Function which will use Kafka configuration to set up an active
    * Kafka consumer.
    */
  type UnconfiguredConsumer[M] =
    (ConnectionConfig, ConsumerConfig) => Resource[IO, KafkaConsumer[M]]

  /**
    * Partially set up a Kafka consumer so that the eventually-produced
    * consumer will read messages from a single topic.
    */
  def ofTopic[M](topic: String)(
    implicit cs: ContextShift[IO],
    t: Timer[IO],
    d: Deserializer.Attempt[TransferMessage[M]]
  ): UnconfiguredConsumer[M] = buildAndSubscribe(_.subscribeTo(topic))

  /**
    * Partially set up a Kafka consumer so that the eventually-produced
    * consumer will read messages from all topics matching a naming pattern.
    */
  def ofTopicPattern[M](topicPattern: Regex)(
    implicit cs: ContextShift[IO],
    t: Timer[IO],
    d: Deserializer.Attempt[TransferMessage[M]]
  ): UnconfiguredConsumer[M] = buildAndSubscribe(_.subscribe(topicPattern))

  /**
    * Produce a function which, given the needed configuration, will set up
    * a Kakfa consumer and subscribe it to some number of topics.
    */
  private def buildAndSubscribe[M](subscribe: KConsumer[IO, _, _] => IO[Unit])(
    implicit cs: ContextShift[IO],
    t: Timer[IO],
    d: Deserializer.Attempt[TransferMessage[M]]
  ): UnconfiguredConsumer[M] = (connConfig, consumerConfig) => {
    val built = for {
      settings <- consumerSettings[Unit, Serdes.Attempt[TransferMessage[M]]](
        connConfig,
        consumerConfig
      )
      consumer <- fs2.kafka.consumerResource[IO].using(settings)
    } yield {
      consumer
    }

    built.evalMap { c =>
      subscribe(c).as(
        new Impl(c, consumerConfig.maxRecordsPerBatch, consumerConfig.waitTimePerBatch)
      )
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
        // Sets the frequency at which the Kafka libs re-query brokers for new topic info.
        .withProperty(
          "metadata.max.age.ms",
          consumer.topicMetadataTtl.toMillis.toString
        )
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
    */
  private[kafka] class Impl[M](
    consumer: KConsumer[IO, Unit, Serdes.Attempt[TransferMessage[M]]],
    maxPerBatch: Int,
    waitTimePerBatch: FiniteDuration
  )(implicit cs: ContextShift[IO], t: Timer[IO])
      extends KafkaConsumer[M] {

    private val logger = Slf4jLogger.getLogger[IO]

    override def runForeach(f: List[(TransferIds, M)] => IO[Unit]): IO[Unit] =
      consumer.stream.evalTap { message =>
        logger.info(s"Got message from topic ${message.record.topic}")
      }.map { message =>
        message.record.value().map(_ -> message.committableOffset)
      }.evalTap {
        case Right((m, _)) => logger.debug(s"Decoded message from Kafka: $m")
        case Left(err)     => logger.error(err)("Failed to decode Kafka message")
      }.rethrow
        .groupWithin(maxPerBatch, waitTimePerBatch)
        .evalMap { chunk =>
          // There's probably a more efficient way to do this, but I doubt
          // it'll have noticeable impact unless `maxRecords` is huge.
          val (attempts, offsets) = chunk.toList.unzip
          f(attempts.map(m => (m.ids, m.message))).as(Chunk.iterable(offsets))
        }
        .through(fs2.kafka.commitBatchChunk)
        .compile
        .drain
  }

}
