package org.broadinstitute.transporter.queue

import cats.effect.IO
import io.circe.Json
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class QueueControllerSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {

  private val db = mock[DbClient]
  private val kafka = mock[KafkaClient]

  private def controller = new QueueController(db, kafka)

  private val queue =
    Queue("test-queue", "requests", "responses", Json.obj().as[QueueSchema].right.value)

  behavior of "QueueController"

  it should "look up queue information by name in the DB" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(None))

    controller
      .lookupQueue(queue.name)
      .unsafeRunSync() shouldBe None
  }

  it should "verify Kafka is consistent with the DB on lookups" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(queue)))
    (kafka.listTopics _).expects().returning(IO.pure(Set.empty))

    controller
      .lookupQueue(queue.name)
      .unsafeRunSync() shouldBe None
  }

  it should "look up queues with consistent state" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(queue)))
    (kafka.listTopics _)
      .expects()
      .returning(IO.pure(Set(queue.requestTopic, queue.responseTopic)))

    controller
      .lookupQueue(queue.name)
      .unsafeRunSync() shouldBe Some(queue)
  }

  it should "create new queues" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(None))
    (db.insertQueue _)
      .expects(where((q: Queue) => q.name == queue.name && q.schema == queue.schema))
      .returning(IO.unit)
    (kafka.createTopics _)
      .expects(where((l: List[String]) => l.length == 2))
      .returning(IO.unit)

    val created =
      controller.createQueue(QueueRequest(queue.name, queue.schema)).unsafeRunSync()
    created.name shouldBe queue.name
    created.schema shouldBe queue.schema
  }

  it should "clean up the DB if topic creation fails" in {
    val err = new RuntimeException("OH NO")

    (db.lookupQueue _).expects(queue.name).returning(IO.pure(None))
    (db.insertQueue _)
      .expects(where((q: Queue) => q.name == queue.name && q.schema == queue.schema))
      .returning(IO.unit)
    (kafka.createTopics _).expects(*).throwing(err)
    (db.deleteQueue _).expects(queue.name).returning(IO.unit)

    controller
      .createQueue(QueueRequest(queue.name, queue.schema))
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe err
  }

  it should "not overwrite existing, consistent queues" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(queue)))
    (kafka.listTopics _)
      .expects()
      .returning(IO.pure(Set(queue.requestTopic, queue.responseTopic)))

    controller
      .createQueue(QueueRequest(queue.name, queue.schema))
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe a[QueueController.QueueAlreadyExists]
  }

  it should "detect and attempt to correct inconsistent state on creation" in {
    (db.lookupQueue _).expects(queue.name).returning(IO.pure(Some(queue)))
    (kafka.listTopics _).expects().returning(IO.pure(Set.empty))
    (kafka.createTopics _)
      .expects(List(queue.requestTopic, queue.responseTopic))
      .returning(IO.unit)

    controller
      .createQueue(QueueRequest(queue.name, queue.schema))
      .unsafeRunSync() shouldBe queue
  }
}
