package org.broadinstitute.transporter.kafka

import cats.effect.IO
import cats.implicits._
import doobie.util.ExecutionContexts

class AdminClientSpec extends BaseKafkaSpec {

  private def withClient[T](body: AdminClient.Impl => IO[T]): T =
    withClient(identity, body)

  private def withClient[T](
    mapConfig: KafkaConfig => KafkaConfig,
    body: AdminClient.Impl => IO[T]
  ): T = withKafka[T] { config =>
    val resource = for {
      ec <- ExecutionContexts.cachedThreadPool[IO]
      adminClient <- AdminClient.adminResource(mapConfig(config), ec)
    } yield {
      new AdminClient.Impl(adminClient, topicConfig)
    }

    resource.use(body).unsafeRunSync()
  }

  behavior of "KafkaClient"

  it should "report ready on good configuration" in {
    withClient(_.checkReady) shouldBe true
  }

  it should "report not ready on bad configuration" in {
    withClient(_.copy(bootstrapServers = Nil), _.checkReady) shouldBe false
  }

  it should "create topics" in {
    val topics = List("foo", "bar")

    val createdTopics = withClient { client =>
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

    val (errored, createdTopics) = withClient { client =>
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

    val (existsBefore, existsAfter) = withClient { client =>
      for {
        existsBeforeCreate <- client.topicsExist(topics: _*)
        _ <- topics.traverse(topic => IO.delay(createCustomTopic(topic)))
        existsAfterCreate <- client.topicsExist(topics: _*)
      } yield {
        (existsBeforeCreate, existsAfterCreate)
      }
    }

    existsBefore shouldBe false
    existsAfter shouldBe true
  }
}
