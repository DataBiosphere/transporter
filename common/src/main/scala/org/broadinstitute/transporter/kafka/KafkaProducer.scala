package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.kafka.{
  ProducerMessage,
  ProducerRecord,
  ProducerSettings,
  Serializer,
  KafkaProducer => KProducer
}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.kafka.config.ConnectionConfig
import org.broadinstitute.transporter.transfer.{TransferIds, TransferMessage}

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
  def submit(messagesByTopic: List[(String, List[(TransferIds, M)])]): IO[Unit]
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
  def resource[M](config: ConnectionConfig)(
    implicit cs: ContextShift[IO],
    s: Serializer[TransferMessage[M]]
  ): Resource[IO, KafkaProducer[M]] =
    fs2.kafka
      .producerResource[IO]
      .using(producerSettings[Unit, TransferMessage[M]](config))
      .map(new Impl(_))

  /**
    * Build settings for a [[org.apache.kafka.clients.producer.Producer]] from our config.
    *
    * Some settings in the output are hard-coded to prevent silent data loss in the producer,
    * which isn't acceptable for our use-case.
    */
  private def producerSettings[K: Serializer, V: Serializer](config: ConnectionConfig) =
    ProducerSettings[K, V]
    // Required to connect to Kafka at all.
      .withBootstrapServers(config.bootstrapServers.intercalate(","))
      // For debugging on the Kafka server; adds an ID to the logs.
      .withClientId(config.clientId)
      // Recommended for apps where it's not acceptable to lose messages.
      .withRetries(Int.MaxValue)
      // Recommended for apps where it's not acceptable to double-send messages.
      .withEnableIdempotence(true)
      // No "official" recommendation on these values, we can tweak as we see fit.
      .withRequestTimeout(config.requestTimeout)
      .withCloseTimeout(config.closeTimeout)

  /**
    * Concrete implementation of our producer used by mainline code.
    *
    * @param producer client which can push "raw" messages to Kafka
    */
  private[kafka] class Impl[M](producer: KProducer[IO, Unit, TransferMessage[M]])
      extends KafkaProducer[M] {

    private val logger = Slf4jLogger.getLogger[IO]

    override def submit(
      messagesByTopic: List[(String, List[(TransferIds, M)])]
    ): IO[Unit] = {
      val records = messagesByTopic.flatMap {
        case (topic, messages) =>
          messages.map {
            case (ids, message) =>
              ProducerRecord(topic, (), TransferMessage(ids, message))
          }
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
