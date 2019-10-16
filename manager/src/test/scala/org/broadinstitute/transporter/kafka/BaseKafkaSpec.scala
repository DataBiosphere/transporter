package org.broadinstitute.transporter.kafka

import java.util

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import fs2.kafka.{Deserializer, Headers, Serializer}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{
  Deserializer => KDeserializer,
  Serializer => KSerializer
}
import org.broadinstitute.transporter.kafka.config.ConnectionConfig
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait BaseKafkaSpec extends FlatSpec with Matchers with EmbeddedKafka {

  protected implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val baseConfig = EmbeddedKafkaConfig(
    kafkaPort = 0,
    zooKeeperPort = 0,
    customConsumerProperties = Map(ConsumerConfig.GROUP_ID_CONFIG -> "test-group")
  )

  protected def connConfig(embeddedConfig: EmbeddedKafkaConfig) = ConnectionConfig(
    NonEmptyList.of(s"localhost:${embeddedConfig.kafkaPort}"),
    "kafka-test",
    requestTimeout = 2.seconds,
    closeTimeout = 1.second,
    tls = None,
    scram = None
  )

  protected def withKafka[T](body: EmbeddedKafkaConfig => T): T =
    withRunningKafkaOnFoundPort(baseConfig)(body)

  implicit def fs2SerToJavaSer[A](
    implicit serializer: Serializer[IO, A]
  ): KSerializer[A] =
    new KSerializer[A] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

      override def serialize(topic: String, data: A): Array[Byte] =
        serializer.serialize(topic, Headers.empty, data).unsafeRunSync()
      override def close(): Unit = ()
    }

  implicit def fs2DeserToJavaDeser[A](
    implicit deserializer: Deserializer[IO, A]
  ): KDeserializer[A] =
    new KDeserializer[A] {
      override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = ()

      override def deserialize(topic: String, data: Array[Byte]): A =
        deserializer.deserialize(topic, Headers.empty, data).unsafeRunSync()
      override def close(): Unit = ()
    }
}
