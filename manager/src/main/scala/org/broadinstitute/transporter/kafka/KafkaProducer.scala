package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.kafka.{ProducerMessage, ProducerRecord, Serializer, KafkaProducer => KProducer}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.kafka.config.KafkaConfig

/**
  * Client responsible for pushing messages into Kafka topics.
  *
  * @tparam M the type of messages which should be pushed to Kafka by this producer
  */
trait KafkaProducer[M] {

  /**
    * Submit batches of messages to Kafka topics.
    *
    * The returned `IO` will only complete when the produced
    * messages have been acknowledged by the Kafka cluster.
    */
  def submit(messagesByTopic: List[(String, List[M])]): IO[Unit]
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
  def resource[M](config: KafkaConfig, s: Serializer[M])(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaProducer[M]] =
    fs2.kafka
      .producerResource[IO]
      .using(config.producerSettings(Serializer.unit, s))
      .map(new Impl(_))

  /**
    * Concrete implementation of our producer used by mainline code.
    *
    * @param producer client which can push "raw" messages to Kafka
    */
  private[kafka] class Impl[M](producer: KProducer[IO, Unit, M])
      extends KafkaProducer[M] {

    private val logger = Slf4jLogger.getLogger[IO]

    override def submit(messagesByTopic: List[(String, List[M])]): IO[Unit] = {
      val records = messagesByTopic.flatMap {
        case (topic, messages) =>
          messages.map(ProducerRecord(topic, (), _))
      }

      for {
        _ <- logger.info(s"Submitting ${records.length} records to Kafka")
        ackIO <- producer.producePassthrough(ProducerMessage(records))
        _ <- logger.debug(s"Waiting for Kafka to acknowledge submission...")
        _ <- ackIO
      } yield ()
    }
  }

}
