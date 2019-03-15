package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import fs2.kafka.{Deserializer, KafkaConsumer => KConsumer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.util.matching.Regex

/**
  * Client responsible for processing messages from a single Kafka subscription.
  *
  * Raw Kafka consumer instances are configured to work with specific key/value types,
  * so this class does the same.
  *
  * @tparam K type which keys pulled from Kafka should be parsed into
  * @tparam V type which values pulled from Kafka should be parsed into
  */
trait KafkaConsumer[K, V] {

  /**
    * Run an effecting operation on every key/value pair pulled from the Kafka subscription.
    *
    * The returned `IO` will run until cancelled. Messages will be committed
    * in batches as they are successfully processed by `f`.
    */
  def runForeach(f: KafkaConsumer.Attempt[(K, V)] => IO[Unit]): IO[Unit]
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
    * @param config settings for the underlying Kafka client
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    * @param t proof of the ability to schedule tasks for later execution
    */
  def resource[K: Deserializer.Attempt, V: Deserializer.Attempt](
    topicPattern: Regex,
    config: KafkaConfig
  )(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, KafkaConsumer[K, V]] = {
    val underlyingConsumer = for {
      ec <- fs2.kafka.consumerExecutionContextResource[IO]
      settings = config.consumerSettings[Attempt[K], Attempt[V]](ec)
      consumer <- fs2.kafka.consumerResource[IO].using(settings)
    } yield {
      consumer
    }

    underlyingConsumer.evalMap(c => c.subscribe(topicPattern).as(new Impl(c)))
  }

  /**
    * Concrete implementation of our consumer used by mainline code.
    *
    * @param consumer client which can pull "raw" messages from Kafka.
    *                 NOTE: This class assumes a subscription has already
    *                 been initialized in the consumer
    */
  private[kafka] class Impl[K, V](consumer: KConsumer[IO, Attempt[K], Attempt[V]])
      extends KafkaConsumer[K, V] {

    private val logger = Slf4jLogger.getLogger[IO]

    override def runForeach(f: Attempt[(K, V)] => IO[Unit]): IO[Unit] =
      consumer.stream.map { message =>
        (message.record.key(), message.record.value()).tupled -> message.committableOffset
      }.evalTap {
        case (Right((k, v)), _) =>
          logger.debug(s"Decoded key-value pair from Kafka: [$k: $v]")
        case (Left(err), _) =>
          logger.warn(err)(s"Failed to decode Kafka message")
      }.evalMap {
        case (maybeKv, offset) => f(maybeKv).as(offset)
      }.through(fs2.kafka.commitBatch).compile.drain
  }
}
