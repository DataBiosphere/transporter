package org.broadinstitute.transporter.transfer

import java.time.{Instant, OffsetDateTime, ZoneId}

import cats.effect.IO
import io.chrisdavenport.fuuid.FUUID
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaProducer
import org.broadinstitute.transporter.queue.api.Queue
import org.broadinstitute.transporter.queue.{QueueController, QueueSchema}
import org.broadinstitute.transporter.transfer.api.{RequestAck, BulkRequest}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers, OptionValues}

class TransferControllerSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues
    with OptionValues {

  private val db = mock[DbClient]
  private val queueController = mock[QueueController]
  private val kafka = mock[KafkaProducer[FUUID, Json]]

  private val queueName = "queue"
  private val queueId = FUUID.randomFUUID[IO].unsafeRunSync()
  private val reqTopic = "requests"
  private val progressTopic = "progress"
  private val resTopic = "results"
  private val schema = json"""{ "type": "object" }""".as[QueueSchema].right.value
  private val queue = Queue(queueName, reqTopic, progressTopic, resTopic, schema)
  private val queueInfo = (queueId, queue)

  private val goodRequest = BulkRequest(
    List(json"""{ "a": "b" }""", json"""{ "c": "d" }""", json"""{ "e": "f" }""")
  )
  private val requestId = FUUID.randomFUUID[IO].unsafeRunSync()
  private val transfersWithIds =
    goodRequest.transfers.map(FUUID.randomFUUID[IO].unsafeRunSync() -> _)

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
      .unsafeRunSync() shouldBe RequestAck(requestId)
  }

  it should "raise errors on submissions to nonexistent queues" in {
    (queueController.lookupQueueInfo _).expects(queueName).returning(IO.pure(None))

    controller
      .submitTransfer(queueName, goodRequest)
      .attempt
      .unsafeRunSync() shouldBe Left(QueueController.NoSuchQueue(queueName))
  }

  it should "validate the schemas of submitted requests" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))

    val badRequest = BulkRequest(List(json"[1, 2, 3]", json""""hello world!""""))
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

  it should "look up statuses for running requests" in {
    val counts = Map[TransferStatus, Long](TransferStatus.Succeeded -> 10L)

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(counts.mapValues(c => (c, Vector.empty[Json], None, None))))

    controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync() shouldBe RequestStatus(
      TransferStatus.Succeeded,
      counts ++ Map(
        TransferStatus.Failed -> 0L,
        TransferStatus.Submitted -> 0L
      ),
      None,
      None,
      Nil
    )
  }

  it should "return an error if looking up statuses for an ID with no registered transfers" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(Map.empty))

    controller
      .lookupRequestStatus(queueName, requestId)
      .attempt
      .unsafeRunSync() shouldBe Left(TransferController.NoSuchRequest(requestId))
  }

  it should "return an error if looking up statuses for an unregistered queue name" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(None))

    controller
      .lookupRequestStatus(queueName, requestId)
      .attempt
      .unsafeRunSync() shouldBe Left(QueueController.NoSuchQueue(queueName))
  }

  it should "prioritize submissions over all in request summaries" in {
    val counts = Map(
      TransferStatus.Submitted -> 10L,
      TransferStatus.Succeeded -> 5L,
      TransferStatus.Failed -> 1L
    )

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(counts.mapValues(c => (c, Vector.empty[Json], None, None))))

    controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync() shouldBe RequestStatus(
      TransferStatus.Submitted,
      counts,
      None,
      None,
      Nil
    )
  }

  it should "prioritize failures over successes in request summaries" in {
    val counts = Map(
      TransferStatus.Failed -> 10L,
      TransferStatus.Succeeded -> 5L
    )

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(counts.mapValues(c => (c, Vector.empty[Json], None, None))))

    controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync() shouldBe RequestStatus(
      TransferStatus.Failed,
      counts ++ Map(
        TransferStatus.Submitted -> 0L
      ),
      None,
      None,
      Nil
    )
  }

  it should "include associated transfer info in request summaries" in {
    val counts = Map(
      TransferStatus.Submitted -> 10L,
      TransferStatus.Succeeded -> 5L,
      TransferStatus.Failed -> 1L
    )

    val infos = Map(
      TransferStatus.Submitted -> Vector.empty[Json],
      TransferStatus.Succeeded -> Vector.tabulate(5)(
        i => json"""{ "wat": "I worked!", "i": $i }"""
      ),
      TransferStatus.Failed -> Vector(json"""{ "wot": "I BROKE" }""")
    )

    val lookup =
      TransferStatus.values.map(s => (s, (counts(s), infos(s), None, None))).toMap

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(lookup))

    val status = controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync()

    status.overallStatus shouldBe TransferStatus.Submitted
    status.info should contain theSameElementsAs infos.flatMap(_._2)
  }

  it should "include overall submission and updated times in request summaries" in {
    val counts = Map(
      TransferStatus.Submitted -> 10L,
      TransferStatus.Succeeded -> 5L,
      TransferStatus.Failed -> 1L
    )

    def odt(millis: Long) =
      OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))

    val minSubmitted = odt(12344L)
    val maxUpdated = odt(12347L)

    val timestamps = Map(
      (TransferStatus.Submitted, (Some(odt(12345L)), None)),
      (TransferStatus.Succeeded, (Some(minSubmitted), Some(odt(12346L)))),
      (TransferStatus.Failed, (Some(odt(12346L)), Some(maxUpdated)))
    )

    val lookup =
      TransferStatus.values.map { s =>
        val (submit, update) = timestamps(s)
        (s, (counts(s), Vector.empty[Json], submit, update))
      }.toMap

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(lookup))

    val status = controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync()

    status.overallStatus shouldBe TransferStatus.Submitted
    status.submittedAt.value shouldBe minSubmitted
    status.updatedAt.value shouldBe maxUpdated
  }
}
