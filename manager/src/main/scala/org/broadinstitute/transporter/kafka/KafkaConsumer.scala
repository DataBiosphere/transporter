package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.Chunk
import fs2.kafka.{Deserializer, KafkaConsumer => KConsumer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.kafka.config.{ConsumerBatchConfig, KafkaConfig}

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
    * Run an effecting operation on every batch of key/value pairs pulled
    * from the Kafka subscription.
    *
    * The returned `IO` will run until cancelled. Messages will be committed
    * in batches as they are successfully processed by `f`.
    */
  def runForeach(f: List[KafkaConsumer.Attempt[M]] => IO[Unit]): IO[Unit]
}

object KafkaConsumer {

  type Attempt[T] = Either[Throwable, T]

  /**
    * Construct a consumer wrapped in logic to set up / tear down
    * the threading infrastructure required by the underlying Java client,
    * and subscribe the consumer to all topics matching a pattern.
    *
    * @param topicPattern regex matching all topics which should be included
    *                     in the subscription. Topics will dynamically join/leave
    *                     the subscriptions as they're created/destroyed in
    *                     Kafka, as determined by a polling interval set in config
    * @param config       settings for the underlying Kafka client
    * @param cs           proof of the ability to shift IO-wrapped computations
    *                     onto other threads
    * @param t            proof of the ability to schedule tasks for later execution
    */
  def resource[M](
    topicPattern: Regex,
    config: KafkaConfig,
    d: Deserializer.Attempt[M]
  )(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, KafkaConsumer[M]] = {
    val underlyingConsumer = for {
      ec <- fs2.kafka.consumerExecutionContextResource[IO]
      settings = config.consumerSettings(ec, Deserializer.unit, d)
      consumer <- fs2.kafka.consumerResource[IO].using(settings)
    } yield {
      consumer
    }

    underlyingConsumer.evalMap { c =>
      c.subscribe(topicPattern).as(new Impl(c, config.batchParams))
    }
  }

  /**
    * Concrete implementation of our consumer used by mainline code.
    *
    * @param consumer client which can pull "raw" messages from Kafka.
    *                 NOTE: This class assumes a subscription has already
    *                 been initialized in the consumer
    */
  private[kafka] class Impl[M](
    consumer: KConsumer[IO, Unit, Attempt[M]],
    batchConfig: ConsumerBatchConfig
  )(implicit cs: ContextShift[IO], t: Timer[IO])
      extends KafkaConsumer[M] {

    private val logger = Slf4jLogger.getLogger[IO]

    override def runForeach(f: List[Attempt[M]] => IO[Unit]): IO[Unit] =
      consumer.stream.evalTap { message =>
        logger.info(s"Got message from topic ${message.record.topic}")
      }.map(message => message.record.value() -> message.committableOffset)
        .evalTap {
          case (Right(m), _)  => logger.debug(s"Decoded message from Kafka: $m")
          case (Left(err), _) => logger.warn(err)("Failed to decode Kafka message")
        }
        .groupWithin(batchConfig.maxRecords, batchConfig.waitTime)
        .evalMap { chunk =>
          // There's probably a more efficient way to do this, but I doubt
          // it'll have noticeable impact unless `maxRecords` is huge.
          val (attempts, offsets) = chunk.toList.unzip
          f(attempts).as(Chunk.iterable(offsets))
        }
        .through(fs2.kafka.commitBatchChunk)
        .compile
        .drain
  }

}
