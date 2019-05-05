package org.broadinstitute.transporter.queue

import java.util.UUID

import cats.effect.IO
import cats.implicits._
import doobie._
import doobie.implicits._
import io.circe.Json
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.error.{
  InvalidQueueParameter,
  NoSuchQueue,
  QueueAlreadyExists
}
import org.broadinstitute.transporter.kafka.{KafkaAdminClient, TopicApi}
import org.broadinstitute.transporter.queue.api.{Queue, QueueParameters, QueueRequest}
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues

class QueueControllerSpec extends PostgresSpec with MockFactory with EitherValues {
  import org.broadinstitute.transporter.db.DoobieInstances._

  private val kafka = mock[KafkaAdminClient]

  private val request =
    QueueRequest("test-queue", Json.obj().as[QueueSchema].right.value, 2)

  private val countQueues =
    sql"select count(*) from queues where name = ${request.name}".query[Long].unique

  def withController(test: (Transactor[IO], QueueController) => Any): Unit = {
    val tx = transactor
    test(tx, new QueueController(tx, kafka))
    ()
  }

  behavior of "QueueController"

  it should "create new queues" in withController { (tx, controller) =>
    (kafka.createTopics _)
      .expects(where { topics: Seq[String] =>
        topics.length == 3 &&
        topics.exists(_.matches(TopicApi.RequestSubscriptionPattern.regex)) &&
        topics.exists(_.matches(TopicApi.ProgressSubscriptionPattern.regex)) &&
        topics.exists(_.matches(TopicApi.ResponseSubscriptionPattern.regex))
      })
      .returning(IO.unit)

    val checks = for {
      initCount <- countQueues.transact(tx)
      queue <- controller.createQueue(request)
      afterCount <- countQueues.transact(tx)
    } yield {
      initCount shouldBe 0
      afterCount shouldBe 1

      queue.name shouldBe request.name
      queue.schema shouldBe request.schema
      queue.maxConcurrentTransfers shouldBe request.maxConcurrentTransfers
      queue.requestTopic should fullyMatch regex TopicApi.RequestSubscriptionPattern
      queue.progressTopic should fullyMatch regex TopicApi.ProgressSubscriptionPattern
      queue.responseTopic should fullyMatch regex TopicApi.ResponseSubscriptionPattern
    }

    checks.unsafeRunSync()
  }

  it should "roll back DB changes if topic creation fails" in withController {
    (tx, controller) =>
      val err = new RuntimeException("OH NO")

      (kafka.createTopics _)
        .expects(*)
        .returning(IO.raiseError(err))

      val checks = for {
        initCount <- countQueues.transact(tx)
        queueOrError <- controller.createQueue(request).attempt
        afterCount <- countQueues.transact(tx)
      } yield {
        initCount shouldBe 0
        afterCount shouldBe 0
        queueOrError.left.value shouldBe err
      }

      checks.unsafeRunSync()
  }

  it should "reject illegal queue parameters on creation" in withController {
    (tx, controller) =>
      val checks = for {
        initCount <- countQueues.transact(tx)
        queueOrError <- controller
          .createQueue(request.copy(maxConcurrentTransfers = -1))
          .attempt
        afterCount <- countQueues.transact(tx)
      } yield {
        initCount shouldBe 0
        afterCount shouldBe 0
        queueOrError.left.value shouldBe an[InvalidQueueParameter]
      }

      checks.unsafeRunSync()
  }

  private val existingQueue = Queue(
    request.name,
    "req",
    "prog",
    "resp",
    Json.obj("type" -> Json.fromString("object")).as[QueueSchema].right.value,
    4
  )

  private val insertExisting = {
    val id = UUID.randomUUID()
    List(
      fr"insert into queues",
      fr"(id, name, request_topic, progress_topic, response_topic, request_schema, max_in_flight) values",
      Fragments.parentheses {
        List(
          fr"$id",
          fr"${existingQueue.name}",
          fr"${existingQueue.requestTopic}",
          fr"${existingQueue.progressTopic}",
          fr"${existingQueue.responseTopic}",
          fr"${existingQueue.schema}",
          fr"${existingQueue.maxConcurrentTransfers}"
        ).intercalate(fr",")
      }
    ).combineAll.update.run
  }

  private val newParameters =
    QueueParameters(Some(request.schema), Some(request.maxConcurrentTransfers))

  it should "not overwrite existing queues" in withController { (tx, controller) =>
    val checks = for {
      _ <- insertExisting.transact(tx)
      queueOrError <- controller.createQueue(request).attempt
      (requestTopic, progressTopic, responseTopic) <- sql"""select request_topic, progress_topic, response_topic
              from queues where name = ${request.name}"""
        .query[(String, String, String)]
        .unique
        .transact(transactor)
    } yield {
      queueOrError.left.value shouldBe QueueAlreadyExists(request.name)
      requestTopic shouldBe "req"
      progressTopic shouldBe "prog"
      responseTopic shouldBe "resp"
    }

    checks.unsafeRunSync()
  }

  it should "patch existing queue parameters" in withController { (tx, controller) =>
    val checks = for {
      _ <- insertExisting.transact(tx)
      updated <- controller.patchQueue(request.name, newParameters)
    } yield {
      updated shouldBe Queue(
        request.name,
        "req",
        "prog",
        "resp",
        request.schema,
        request.maxConcurrentTransfers
      )
    }

    checks.unsafeRunSync()
  }

  it should "fail to patch a nonexistent queue" in withController { (tx, controller) =>
    val checks = for {
      initCount <- countQueues.transact(tx)
      updatedOrError <- controller.patchQueue(request.name, newParameters).attempt
      afterCount <- countQueues.transact(tx)
    } yield {
      initCount shouldBe 0
      afterCount shouldBe 0
      updatedOrError.left.value shouldBe NoSuchQueue(request.name)
    }

    checks.unsafeRunSync()
  }

  it should "reject illegal queue parameters on patch" in withController {
    (tx, controller) =>
      val checks = for {
        _ <- insertExisting.transact(tx)
        updatedOrError <- controller
          .patchQueue(request.name, newParameters.copy(maxConcurrentTransfers = Some(-1)))
          .attempt
        (schema, max) <- sql"select request_schema, max_in_flight from queues where name = ${request.name}"
          .query[(QueueSchema, Int)]
          .unique
          .transact(tx)
      } yield {
        updatedOrError.left.value shouldBe a[InvalidQueueParameter]
        schema shouldBe existingQueue.schema
        max shouldBe existingQueue.maxConcurrentTransfers
      }

      checks.unsafeRunSync()
  }

  it should "look up queues by name" in withController { (tx, controller) =>
    val checks = for {
      _ <- insertExisting.transact(tx)
      lookedUp <- controller.lookupQueue(request.name)
    } yield {
      lookedUp shouldBe existingQueue
    }

    checks.unsafeRunSync()
  }
}
