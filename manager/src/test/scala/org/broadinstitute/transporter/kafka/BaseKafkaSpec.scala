package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait BaseKafkaSpec extends FlatSpec with Matchers with EmbeddedKafka {

  protected implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  protected val topicConfig = TopicConfig(
    partitions = 1,
    replicationFactor = 1
  )

  protected val timingConfig = TimeoutConfig(
    requestTimeout = 2.seconds,
    closeTimeout = 1.second
  )

  private val baseConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  protected def appConfig(embeddedConfig: EmbeddedKafkaConfig) = KafkaConfig(
    List(s"localhost:${embeddedConfig.kafkaPort}"),
    "kafka-test",
    topicConfig,
    timingConfig
  )

  protected def withKafka[T](body: KafkaConfig => T): T =
    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      body(appConfig(actualConfig))
    }
}
