package org.broadinstitute.transporter.kafka

import cats.data.NonEmptyList
import cats.effect.{ContextShift, IO}
import doobie.util.ExecutionContexts
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.broadinstitute.transporter.kafka.config.{AdminConfig, ConnectionConfig}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaAdminClientSpec extends FlatSpec with Matchers with EmbeddedKafka {
  import KafkaAdminClient.KafkaFutureSyntax

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val baseConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)

  private def connConfig(embeddedConfig: EmbeddedKafkaConfig) = ConnectionConfig(
    NonEmptyList.of(s"localhost:${embeddedConfig.kafkaPort}"),
    "kafka-test",
    requestTimeout = 2.seconds,
    closeTimeout = 1.second
  )

  private def withKafka[T](body: EmbeddedKafkaConfig => T): T =
    withRunningKafkaOnFoundPort(baseConfig)(body)

  behavior of "KafkaAdminClient"

  it should "report ready when enough brokers are available" in withKafka { config =>
    val resource = for {
      ec <- ExecutionContexts.cachedThreadPool[IO]
      adminClient <- KafkaAdminClient.resource(connConfig(config), AdminConfig(1), ec)
    } yield {
      adminClient
    }

    resource.use(_.checkEnoughBrokers).unsafeRunSync() shouldBe true
  }

  it should "report not ready when not enough brokers are available" in withKafka {
    config =>
      val resource = for {
        ec <- ExecutionContexts.cachedThreadPool[IO]
        adminClient <- KafkaAdminClient.resource(connConfig(config), AdminConfig(3), ec)
      } yield {
        adminClient
      }

      resource.use(_.checkEnoughBrokers).unsafeRunSync() shouldBe false
  }

  it should "create topics" in withKafka { config =>
    val topics = List("foo", "bar")

    val resource = for {
      ec <- ExecutionContexts.cachedThreadPool[IO]
      jAdmin <- KafkaAdminClient.adminResource(connConfig(config), ec)
    } yield (jAdmin, new KafkaAdminClient.Impl(jAdmin, 1))

    resource.use {
      case (jClient, client) =>
        for {
          originalTopics <- IO.suspend(jClient.listTopics.names.cancelable.map(_.asScala))
          _ <- client.createTopics(topics.map(_ -> 3))
          newTopics <- IO.suspend(jClient.listTopics.names.cancelable.map(_.asScala))
        } yield {
          newTopics.diff(originalTopics)
        }
    }.unsafeRunSync() shouldBe topics.toSet
  }

  it should "create topics with specified partition counts" in withKafka { config =>
    val topicPartitions = List("foo" -> 2, "bar" -> 1)

    val resource = for {
      ec <- ExecutionContexts.cachedThreadPool[IO]
      jAdmin <- KafkaAdminClient.adminResource(connConfig(config), ec)
    } yield (jAdmin, new KafkaAdminClient.Impl(jAdmin, 1))

    resource.use {
      case (jClient, client) =>
        for {
          _ <- client.createTopics(topicPartitions)
          descriptions <- IO.suspend(
            jClient.describeTopics(topicPartitions.map(_._1).asJava).all().cancelable
          )
        } yield {
          val created = descriptions.asScala.mapValues(_.partitions().size()).toList
          created should contain theSameElementsAs topicPartitions
        }
    }.unsafeRunSync()
  }

  it should "roll back successfully created topics when other topics in the request fail" in withKafka {
    config =>
      val topics = List("foo", "bar", "$$$")

      val resource = for {
        ec <- ExecutionContexts.cachedThreadPool[IO]
        jAdmin <- KafkaAdminClient.adminResource(connConfig(config), ec)
      } yield (jAdmin, new KafkaAdminClient.Impl(jAdmin, 1))

      val (errored, createdTopics) = resource.use {
        case (jClient, client) =>
          for {
            originalTopics <- IO.suspend(
              jClient.listTopics.names.cancelable.map(_.asScala)
            )
            attempt <- client.createTopics(topics.map(_ -> 3)).attempt
            newTopics <- IO.suspend(jClient.listTopics.names.cancelable.map(_.asScala))
          } yield {
            (attempt.isLeft, newTopics.diff(originalTopics))
          }
      }.unsafeRunSync()

      errored shouldBe true
      createdTopics shouldBe empty
  }
}
