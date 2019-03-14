package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.kafka.{ProducerMessage, ProducerRecord, Serializer, KafkaProducer => KProducer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

trait KafkaProducer[K, V] {

  /** Submit a batch of messages to a topic. */
  def submit(topic: String, messages: List[(K, V)]): IO[Unit]
}

object KafkaProducer {

  def resource[K: Serializer, V: Serializer](config: KafkaConfig)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaProducer[K, V]] =
    fs2.kafka.producerResource[IO].using(config.producerSettings[K, V]).map(new Impl(_))

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
