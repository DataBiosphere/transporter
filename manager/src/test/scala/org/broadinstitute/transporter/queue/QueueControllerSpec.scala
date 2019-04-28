package org.broadinstitute.transporter.queue

import cats.effect.IO
import io.chrisdavenport.fuuid.FUUID
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.AdminClient
import org.broadinstitute.transporter.queue.api.{Queue, QueueRequest}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class QueueControllerSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {

  private val db = mock[DbClient]
  private val kafka = mock[AdminClient]

  private def controller = new QueueController.Impl(db, kafka)

  private val request = QueueRequest("test-queue", Json.obj().as[QueueSchema].right.value)

  private val id = FUUID.randomFUUID[IO].unsafeRunSync()
  private val queue =
    Queue(request.name, "requests", "progress", "responses", request.schema)
  private val dbInfo = (id, queue)

  behavior of "QueueController"

  it should "look up queue information by name in the DB" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(None))

    controller
      .lookupQueue(queue.name)
      .attempt
      .unsafeRunSync() shouldBe Left(QueueController.NoSuchQueue(queue.name))
  }

  it should "verify Kafka is consistent with the DB on lookups" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(dbInfo)))
    (kafka.topicsExist _)
      .expects(List(queue.requestTopic, queue.progressTopic, queue.responseTopic))
      .returning(IO.pure(false))

    controller
      .lookupQueue(queue.name)
      .attempt
      .unsafeRunSync() shouldBe Left(QueueController.NoSuchQueue(queue.name))
  }

  it should "look up queues with consistent state" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(dbInfo)))
    (kafka.topicsExist _)
      .expects(List(queue.requestTopic, queue.progressTopic, queue.responseTopic))
      .returning(IO.pure(true))

    controller.lookupQueue(queue.name).unsafeRunSync() shouldBe queue
  }

  it should "create new queues" in {
    (db.lookupQueue _).expects(request.name).returning(IO.pure(None))
    (db.createQueue _).expects(*, *).returning(IO.unit)
    (kafka.createTopics _).expects(*).returning(IO.unit)

    val created = controller.createQueue(request).unsafeRunSync()
    created.name shouldBe request.name
    created.schema shouldBe request.schema
  }

  it should "clean up the DB if topic creation fails" in {
    val err = new RuntimeException("OH NO")

    (db.lookupQueue _).expects(queue.name).returning(IO.pure(None))
    (db.createQueue _).expects(*, *).returning(IO.unit)
    (kafka.createTopics _).expects(*).throwing(err)
    (db.deleteQueue _).expects(*).returning(IO.unit)

    controller
      .createQueue(request)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe err
  }

  it should "not overwrite existing, consistent queues" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(dbInfo)))
    (kafka.topicsExist _)
      .expects(List(queue.requestTopic, queue.progressTopic, queue.responseTopic))
      .returning(IO.pure(true))

    controller
      .createQueue(request)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe a[QueueController.QueueAlreadyExists]
  }

  it should "detect and attempt to correct inconsistent state on creation" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(dbInfo)))
    (kafka.topicsExist _)
      .expects(List(queue.requestTopic, queue.progressTopic, queue.responseTopic))
      .returning(IO.pure(false))
    (db.patchQueueSchema _).expects(id, queue.schema).returning(IO.unit)
    (kafka.createTopics _)
      .expects(List(queue.requestTopic, queue.progressTopic, queue.responseTopic))
      .returning(IO.unit)

    controller.createQueue(request).unsafeRunSync() shouldBe queue
  }
}
