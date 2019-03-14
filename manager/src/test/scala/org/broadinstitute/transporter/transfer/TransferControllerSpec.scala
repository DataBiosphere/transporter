package org.broadinstitute.transporter.transfer

import java.util.UUID

import cats.effect.IO
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaProducer
import org.broadinstitute.transporter.queue.{QueueController, QueueSchema}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class TransferControllerSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {

  private val db = mock[DbClient]
  private val queueController = mock[QueueController]
  private val kafka = mock[KafkaProducer[UUID, Json]]

  private val queueName = "queue"
  private val queueId = UUID.randomUUID()
  private val reqTopic = "requests"
  private val resTopic = "results"
  private val schema = json"""{ "type": "object" }""".as[QueueSchema].right.value
  private val queueInfo = (queueId, reqTopic, resTopic, schema)

  private val goodRequest = TransferRequest(
    List(json"""{ "a": "b" }""", json"""{ "c": "d" }""", json"""{ "e": "f" }""")
  )
  private val requestId = UUID.randomUUID()
  private val transfersWithIds = goodRequest.transfers.map(UUID.randomUUID() -> _)

  private def controller = new TransferController.Impl(queueController, db, kafka)

  behavior of "TransferController"

  it should "submit transfer requests to existing queues" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.recordTransferRequest _)
      .expects(queueId, goodRequest)
      .returning(IO.pure((requestId, transfersWithIds)))
    (kafka.submit _).expects(reqTopic, transfersWithIds).returning(IO.unit)

    controller
      .submitTransfer(queueName, goodRequest)
      .unsafeRunSync() shouldBe TransferAck(requestId)
  }

  it should "raise errors on submissions to nonexistent queues" in {
    (queueController.lookupQueueInfo _).expects(queueName).returning(IO.pure(None))

    a[TransferController.NoSuchQueue] shouldBe thrownBy {
      controller.submitTransfer(queueName, goodRequest).unsafeRunSync()
    }
  }

  it should "validate the schemas of submitted requests" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))

    val badRequest = TransferRequest(List(json"[1, 2, 3]", json""""hello world!""""))
    a[TransferController.InvalidRequest] shouldBe thrownBy {
      controller.submitTransfer(queueName, badRequest).unsafeRunSync()
    }
  }

  it should "roll back the DB if submitting to Kafka fails" in {
    val err = new RuntimeException("OH NO")

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.recordTransferRequest _)
      .expects(queueId, goodRequest)
      .returning(IO.pure((requestId, transfersWithIds)))
    (kafka.submit _).expects(reqTopic, transfersWithIds).returning(IO.raiseError(err))
    (db.deleteTransferRequest _).expects(requestId).returning(IO.unit)

    controller
      .submitTransfer(queueName, goodRequest)
      .attempt
      .unsafeRunSync() shouldBe Left(err)
  }
}
