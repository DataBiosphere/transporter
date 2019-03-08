package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import doobie.util.ExecutionContexts
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaClientSpec extends FlatSpec with Matchers with EmbeddedKafka {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val blockingEc = ExecutionContexts.cachedThreadPool[IO]

  private val topicConfig = TopicConfig(
    partitions = 1,
    replicationFactor = 1
  )
  private val timeouts = TimeoutConfig(
    requestTimeout = 2.seconds,
    closeTimeout = 1.seconds
  )

  private val baseConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
  private def config(embeddedConfig: EmbeddedKafkaConfig) = KafkaConfig(
    List(s"localhost:${embeddedConfig.kafkaPort}"),
    "test-admin",
    topicConfig,
    timeouts
  )

  private def clientResource(config: KafkaConfig): Resource[IO, KafkaClient] =
    for {
      ec <- blockingEc
      client <- KafkaClient.resource(config, ec)
    } yield client

  behavior of "KafkaClient"

  it should "report ready on good configuration" in {
    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      clientResource(config(actualConfig)).use(_.checkReady).unsafeRunSync() shouldBe true
    }
  }

  it should "report not ready on bad configuration" in {
    withRunningKafkaOnFoundPort(baseConfig) { _ =>
      clientResource(config(baseConfig)).use(_.checkReady).unsafeRunSync() shouldBe false
    }
  }

  it should "create and list topics" in {
    val topics = List("foo", "bar")

    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      clientResource(config(actualConfig)).use { client =>
        for {
          originalTopics <- client.listTopics
          _ <- client.createTopics(topics)
          newTopics <- client.listTopics
        } yield {
          newTopics.diff(originalTopics) shouldBe topics.toSet
        }
      }.unsafeRunSync()
    }
  }

  it should "roll back successfully created topics when other topics in the request fail" in {
    val topics = List("baz", "qux", "$$$")

    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      clientResource(config(actualConfig)).use { client =>
        for {
          err <- client.createTopics(topics).attempt
          existingTopics <- client.listTopics
        } yield {
          err.isLeft shouldBe true
          existingTopics.diff(topics.toSet) shouldBe existingTopics
        }
      }.unsafeRunSync()
    }
  }
}
