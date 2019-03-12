package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import doobie.util.ExecutionContexts
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{EitherValues, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaAdminClientSpec
    extends FlatSpec
    with Matchers
    with EmbeddedKafka
    with EitherValues {

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

  private def clientResource(config: KafkaConfig): Resource[IO, KafkaAdminClient.Impl] =
    for {
      ec <- blockingEc
      client <- KafkaAdminClient.adminResource(config, ec)
    } yield {
      new KafkaAdminClient.Impl(client, topicConfig)
    }

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

  it should "create topics" in {
    val topics = List("foo", "bar")

    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      clientResource(config(actualConfig)).use { client =>
        for {
          originalTopics <- client.listTopics
          _ <- client.createTopics(topics: _*)
          newTopics <- client.listTopics
        } yield {
          newTopics.diff(originalTopics) shouldBe topics.toSet
        }
      }.unsafeRunSync()
    }
  }

  it should "roll back successfully created topics when other topics in the request fail" in {
    val topics = List("foo", "bar", "$$$")

    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      clientResource(config(actualConfig)).use { client =>
        for {
          err <- client.createTopics(topics: _*).attempt
          existingTopics <- client.listTopics
        } yield {
          err.isLeft shouldBe true
          existingTopics.diff(topics.toSet) shouldBe existingTopics
        }
      }.unsafeRunSync()
    }
  }

  it should "check topic existence" in {
    val topics = List("foo", "bar")

    withRunningKafkaOnFoundPort(baseConfig) { actualConfig =>
      clientResource(config(actualConfig)).use { client =>
        for {
          existsBeforeCreate <- client.topicsExist(topics: _*)
          _ <- topics.traverse(topic => IO.delay(createCustomTopic(topic)))
          existsAfterCreate <- client.topicsExist(topics: _*)
        } yield {
          existsBeforeCreate shouldBe false
          existsAfterCreate shouldBe true
        }
      }
    }
  }
}
