package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.kafka.{ProducerMessage, ProducerRecord, Serializer, KafkaProducer => KProducer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.kafka.config.KafkaConfig

/**
  * Client responsible for pushing messages into Kafka topics.
  *
  * Raw Kafka producer instances are configured to work with specific
  * key/value types, so this class does the same.
  *
  * @tparam K the type of keys which should be pushed to Kafka by this producer
  * @tparam V the type of values which should be pushed to Kafka by this producer
  */
trait KafkaProducer[K, V] {

  /**
    * Submit a batch of messages to a topic.
    *
    * The returned `IO` will only complete when the produced
    * messages have been acknowledged by the Kafka cluster.
    */
  def submit(topic: String, messages: List[(K, V)]): IO[Unit]
}

object KafkaProducer {

  /**
    * Construct a producer wrapped in logic to set up / tear down
    * the threading infrastructure required by the underlying Java client.
    *
    * @param config settings for the underlying Kafka client
    * @param cs     proof of the ability to shift IO-wrapped computations
    *               onto other threads
    */
  def resource[K, V](config: KafkaConfig, ks: Serializer[K], vs: Serializer[V])(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaProducer[K, V]] =
    fs2.kafka.producerResource[IO].using(config.producerSettings(ks, vs)).map(new Impl(_))

  /**
    * Concrete implementation of our producer used by mainline code.
    *
    * @param producer client which can push "raw" messages to Kafka
    */
  private[kafka] class Impl[K, V](producer: KProducer[IO, K, V])
      extends KafkaProducer[K, V] {

    private val logger = Slf4jLogger.getLogger[IO]

    override def submit(topic: String, messages: List[(K, V)]): IO[Unit] = {
      val records = messages.map {
        case (id, value) => ProducerRecord(topic, id, value)
      }

      for {
        _ <- logger.info(s"Submitting ${messages.length} records to Kafka topic $topic")
        ackIO <- producer.producePassthrough(ProducerMessage(records))
        _ <- logger.debug(s"Waiting for Kafka to acknowledge submission...")
        _ <- ackIO
      } yield ()
    }
  }

}
