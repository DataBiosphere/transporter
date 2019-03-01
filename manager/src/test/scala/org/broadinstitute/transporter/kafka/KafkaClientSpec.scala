package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO}
import cats.implicits._
import com.dimafeng.testcontainers.{Container, ForAllTestContainer, TestContainerProxy}
import doobie.util.ExecutionContexts
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.KafkaContainer

import scala.collection.JavaConverters._
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

  import KafkaClient.KafkaFutureSyntax

  behavior of "KafkaClient"

  it should "report ready on good configuration" in {
    val clientResource = for {
      ec <- blockingEc
      client <- KafkaClient.resource(config, ec)
    } yield {
      client
    }

    clientResource.use(_.checkReady).unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val settings = config.copy(
      bootstrapServers = List(config.bootstrapServers.head.dropRight(1))
    )

    val clientResource = for {
      ec <- blockingEc
      client <- KafkaClient.resource(settings, ec)
    } yield {
      client
    }

    clientResource.use(_.checkReady).unsafeRunSync() shouldBe false
  }

  it should "create topics" in {
    val topics = List("foo", "bar")

    blockingEc.flatMap { ec =>
      KafkaClient.clientResource(config, ec).map(_ -> ec)
    }.use {
      case (client, ec) =>
        val wrapperClient = new KafkaClient(client, topicConfig, ec)

        for {
          originalTopics <- IO.suspend(client.listTopics().names().cancelable)
          _ <- wrapperClient.createTopics(topics: _*)
          newTopics <- IO.suspend(client.listTopics().names().cancelable)
        } yield {
          newTopics.asScala.diff(originalTopics.asScala) shouldBe topics.toSet
        }
    }.unsafeRunSync()
  }
}
