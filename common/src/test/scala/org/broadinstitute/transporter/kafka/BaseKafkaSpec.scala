package org.broadinstitute.transporter.kafka

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
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
    closeTimeout = 1.second
  )

  protected def withKafka[T](body: EmbeddedKafkaConfig => T): T =
    withRunningKafkaOnFoundPort(baseConfig)(body)
}
