package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import com.dimafeng.testcontainers.{Container, ForAllTestContainer, TestContainerProxy}
import doobie.util.ExecutionContexts
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.KafkaContainer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaClientSpec extends FlatSpec with ForAllTestContainer with Matchers {

  private val baseContainer = new KafkaContainer("5.1.1")

  override val container: Container = new TestContainerProxy[KafkaContainer] {
    override val container: KafkaContainer = baseContainer
  }

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
  private def config = KafkaConfig(
    baseContainer.getBootstrapServers.split(',').toList,
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
    clientResource(config).use(_.checkReady).unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val settings = config.copy(
      bootstrapServers = List(config.bootstrapServers.head.dropRight(1))
    )

    clientResource(settings).use(_.checkReady).unsafeRunSync() shouldBe false
  }

  it should "create and list topics" in {
    val topics = List("foo", "bar")

    clientResource(config).use { client =>
      for {
        originalTopics <- client.listTopics
        _ <- client.createTopics(topics)
        newTopics <- client.listTopics
      } yield {
        newTopics.diff(originalTopics) shouldBe topics.toSet
      }
    }.unsafeRunSync()
  }

  it should "roll back successfully created topics when other topics in the request fail" in {
    val topics = List("baz", "qux", "$$$")

    clientResource(config).use { client =>
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
