package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import fs2.kafka.{Deserializer, Serializer}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaProducerSpec extends FlatSpec with Matchers with EmbeddedKafka {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val topicConfig = TopicConfig(
    partitions = 1,
    replicationFactor = 1
  )
  private val timeouts = TimeoutConfig(
    requestTimeout = 2.seconds,
    closeTimeout = 1.seconds,
    metadataTtl = 500.millis
  )
  private val baseConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
  private def config(embeddedConfig: EmbeddedKafkaConfig) = KafkaConfig(
    List(s"localhost:${embeddedConfig.kafkaPort}"),
    "test-producer",
    topicConfig,
    timeouts
  )

  private def producerResource[K: Serializer, V: Serializer](
    embeddedConfig: EmbeddedKafkaConfig
  ): Resource[IO, KafkaProducer[K, V]] =
    KafkaProducer.resource[K, V](config(embeddedConfig))

  behavior of "KafkaProducer"

  it should "submit messages" in {
    val topic = "the-topic"

    implicit val keySerializer: Serializer[Int] = Serializer.int
    implicit val keyDeserializer: Deserializer[Int] =
      Deserializer.int.map(_.valueOr(throw _))
    implicit val valSerializer: Serializer[String] = Serializer.string
    implicit val valDeserializer: Deserializer[String] = Deserializer.string

    val messages = List(1 -> "foo", 2 -> "bar", 3 -> "baz")

    val published = withRunningKafkaOnFoundPort(baseConfig) { implicit actualConfig =>
      producerResource[Int, String](actualConfig).use { producer =>
        for {
          _ <- IO.delay(createCustomTopic(topic))
          _ <- producer.submit(topic, messages)
          consumed <- IO.delay {
            consumeNumberKeyedMessagesFrom[Int, String](topic, messages.length)
          }
        } yield {
          consumed
        }
      }.unsafeRunSync()
    }

    published shouldBe messages
  }
}
