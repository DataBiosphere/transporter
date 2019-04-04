package org.broadinstitute.transporter.kafka

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits._
import doobie.util.ExecutionContexts
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import org.broadinstitute.transporter.kafka.config.KafkaConfig

class AdminClientSpec extends BaseKafkaSpec {

  private def withClient[T](body: (AdminClient.Impl, EmbeddedKafkaConfig) => IO[T]): T =
    withClient(identity, body)

  private def withClient[T](
    mapConfig: KafkaConfig => KafkaConfig,
    body: (AdminClient.Impl, EmbeddedKafkaConfig) => IO[T]
  ): T = withKafka[T] { (config, embeddedConfig) =>
    val resource = for {
      ec <- ExecutionContexts.cachedThreadPool[IO]
      adminClient <- AdminClient.adminResource(mapConfig(config), ec)
    } yield {
      new AdminClient.Impl(adminClient, replicationFactor)
    }

    resource.use(body(_, embeddedConfig)).unsafeRunSync()
  }

  behavior of "KafkaClient"

  it should "report ready on good configuration" in {
    withClient((client, _) => client.checkReady) shouldBe true
  }

  it should "report not ready on bad configuration" in {
    withClient(
      _.copy(bootstrapServers = NonEmptyList.of("localhost:1")),
      (client, _) => client.checkReady
    ) shouldBe false
  }

  it should "create topics" in {
    val topics = List("foo", "bar")

    val createdTopics = withClient { (client, _) =>
      for {
        originalTopics <- client.listTopics
        _ <- client.createTopics(topics: _*)
        newTopics <- client.listTopics
      } yield {
        newTopics.diff(originalTopics)
      }
    }

    createdTopics shouldBe topics.toSet
  }

  it should "roll back successfully created topics when other topics in the request fail" in {
    val topics = List("foo", "bar", "$$$")

    val (errored, createdTopics) = withClient { (client, _) =>
      for {
        originalTopics <- client.listTopics
        err <- client.createTopics(topics: _*).attempt
        newTopics <- client.listTopics
      } yield {
        (err.isLeft, newTopics.diff(originalTopics))
      }
    }

    errored shouldBe true
    createdTopics shouldBe empty
  }

  it should "check topic existence" in {
    val topics = List("foo", "bar")

    val (existsBefore, existsAfter) = withClient { (client, embeddedConfig) =>
      for {
        existsBeforeCreate <- client.topicsExist(topics: _*)
        _ <- topics.traverse(topic => IO.delay(createCustomTopic(topic)(embeddedConfig)))
        existsAfterCreate <- client.topicsExist(topics: _*)
      } yield {
        (existsBeforeCreate, existsAfterCreate)
      }
    }

    existsBefore shouldBe false
    existsAfter shouldBe true
  }
}
