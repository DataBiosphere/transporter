package org.broadinstitute.transporter.kafka

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.broadinstitute.transporter.kafka.config.{
  ConsumerBatchConfig,
  KafkaConfig,
  TimeoutConfig
}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

trait BaseKafkaSpec extends FlatSpec with Matchers with EmbeddedKafka {

  protected implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  protected val partitionCount: Int = 3
  protected val replicationFactor: Short = 1

  protected val timingConfig = TimeoutConfig(
    requestTimeout = 2.seconds,
    closeTimeout = 1.second,
    topicDiscoveryInterval = 500.millis
  )

  protected val batchConfig = ConsumerBatchConfig(
    maxRecords = 2,
    waitTime = 500.millis
  )

  private val baseConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  protected def appConfig(embeddedConfig: EmbeddedKafkaConfig) = KafkaConfig(
    NonEmptyList.of(s"localhost:${embeddedConfig.kafkaPort}"),
    "kafka-test",
    replicationFactor,
    batchConfig,
    timingConfig
  )

  protected def withKafka[T](body: (KafkaConfig, EmbeddedKafkaConfig) => T): T =
    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      body(appConfig(actualConfig), actualConfig)
    }
}
