package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.broadinstitute.transporter.kafka.config.{
  ConsumerBatchConfig,
  KafkaConfig,
  TimeoutConfig,
  TopicConfig
}
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
    closeTimeout = 1.second,
    topicDiscoveryInterval = 500.millis
  )

  protected val batchConfig = ConsumerBatchConfig(
    maxRecords = 2,
    waitTime = 500.millis
  )

  private val baseConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  protected def appConfig(embeddedConfig: EmbeddedKafkaConfig) = KafkaConfig(
    List(s"localhost:${embeddedConfig.kafkaPort}"),
    "kafka-test",
    batchConfig,
    topicConfig,
    timingConfig
  )

  protected def withKafka[T](body: (KafkaConfig, EmbeddedKafkaConfig) => T): T =
    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      body(appConfig(actualConfig), actualConfig)
    }
}
