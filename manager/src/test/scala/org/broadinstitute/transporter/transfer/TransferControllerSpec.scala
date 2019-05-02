package org.broadinstitute.transporter.transfer

import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.UUID

import cats.effect.IO
import io.circe.literal._
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.queue.api.Queue
import org.broadinstitute.transporter.queue.{QueueController, QueueSchema}
import org.broadinstitute.transporter.transfer.api.{
  BulkRequest,
  RequestAck,
  RequestMessages,
  RequestStatus,
  TransferDetails,
  TransferMessage
}
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

  private val queueName = "queue"
  private val queueId = UUID.randomUUID()
  private val reqTopic = "requests"
  private val progressTopic = "progress"
  private val resTopic = "results"
  private val schema = json"""{ "type": "object" }""".as[QueueSchema].right.value
  private val queue = Queue(queueName, reqTopic, progressTopic, resTopic, schema, 2)
  private val queueInfo = (queueId, queue)

  private val goodRequest = BulkRequest(
    List(json"""{ "a": "b" }""", json"""{ "c": "d" }""", json"""{ "e": "f" }""")
  )
  private val requestId = UUID.randomUUID()

  private def controller = new TransferController.Impl(queueController, db)

  behavior of "TransferController"

  it should "record transfer requests to existing queues" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.recordTransferRequest _)
      .expects(queueId, goodRequest)
      .returning(IO.pure(requestId))

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

  it should "look up summaries for running requests" in {
    val counts = Map[TransferStatus, Long](TransferStatus.Succeeded -> 10L)

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(counts.mapValues(c => (c, None, None))))

    controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync() shouldBe RequestStatus(
      requestId,
      TransferStatus.Succeeded,
      counts ++ Map(
        TransferStatus.Failed -> 0L,
        TransferStatus.Submitted -> 0L,
        TransferStatus.Pending -> 0L
      ),
      None,
      None
    )
  }

  it should "return an error if looking up summaries for a nonexistent request" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(false))

    controller
      .lookupRequestStatus(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe TransferController.NoSuchRequest(queueName, requestId)
  }

  it should "return an error if looking up summaries for an unregistered queue name" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(None))

    controller
      .lookupRequestStatus(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe QueueController.NoSuchQueue(queueName)
  }

  it should "prioritize submissions over all in request summaries" in {
    val counts = Map(
      TransferStatus.Submitted -> 10L,
      TransferStatus.Pending -> 3L,
      TransferStatus.Succeeded -> 5L,
      TransferStatus.Failed -> 1L
    )

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(counts.mapValues(c => (c, None, None))))

    controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync() shouldBe RequestStatus(
      requestId,
      TransferStatus.Submitted,
      counts,
      None,
      None
    )
  }

  it should "prioritize pending over finished transfers in request summaries" in {
    val counts = Map(
      TransferStatus.Failed -> 10L,
      TransferStatus.Pending -> 3L,
      TransferStatus.Succeeded -> 5L
    )

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(counts.mapValues(c => (c, None, None))))

    controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync() shouldBe RequestStatus(
      requestId,
      TransferStatus.Pending,
      counts ++ Map(
        TransferStatus.Submitted -> 0L
      ),
      None,
      None
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
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.summarizeTransfersByStatus _)
      .expects(queueId, requestId)
      .returning(IO.pure(counts.mapValues(c => (c, None, None))))

    controller
      .lookupRequestStatus(queueName, requestId)
      .unsafeRunSync() shouldBe RequestStatus(
      requestId,
      TransferStatus.Failed,
      counts ++ Map(
        TransferStatus.Submitted -> 0L,
        TransferStatus.Pending -> 0L
      ),
      None,
      None
    )
  }

  it should "include overall submission and updated times in request summaries" in {
    val counts = Map(
      TransferStatus.Submitted -> 10L,
      TransferStatus.Succeeded -> 5L,
      TransferStatus.Failed -> 1L,
      TransferStatus.Pending -> 0L
    )

    def odt(millis: Long) =
      OffsetDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.of("UTC"))

    val minSubmitted = odt(12344L)
    val maxUpdated = odt(12347L)

    val timestamps = Map(
      (TransferStatus.Submitted, (Some(odt(12345L)), None)),
      (TransferStatus.Succeeded, (Some(minSubmitted), Some(odt(12346L)))),
      (TransferStatus.Failed, (Some(odt(12346L)), Some(maxUpdated))),
      (TransferStatus.Pending, (None, None))
    )

    val lookup =
      TransferStatus.values.map { s =>
        val (submit, update) = timestamps(s)
        (s, (counts(s), submit, update))
      }.toMap

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
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

  it should "look up outputs for requests" in {
    val outputs = List.tabulate(3)(i => TransferMessage(UUID.randomUUID(), json"$i"))

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.lookupTransferMessages _)
      .expects(queueId, requestId, TransferStatus.Succeeded)
      .returning(IO.pure(outputs))

    controller
      .lookupRequestOutputs(queueName, requestId)
      .unsafeRunSync() shouldBe RequestMessages(requestId, outputs)
  }

  it should "look up failures for requests" in {
    val failures = List.tabulate(3)(i => TransferMessage(UUID.randomUUID(), json"$i"))

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.lookupTransferMessages _)
      .expects(queueId, requestId, TransferStatus.Failed)
      .returning(IO.pure(failures))

    controller
      .lookupRequestFailures(queueName, requestId)
      .unsafeRunSync() shouldBe RequestMessages(requestId, failures)
  }

  it should "return an error if looking up outputs for a nonexistent request" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(false))

    controller
      .lookupRequestOutputs(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe TransferController.NoSuchRequest(queueName, requestId)
  }

  it should "return an error if looking up failures for a nonexistent request" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(false))

    controller
      .lookupRequestFailures(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe TransferController.NoSuchRequest(queueName, requestId)
  }

  it should "return an error if looking up outputs for an unregistered queue name" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(None))

    controller
      .lookupRequestOutputs(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe QueueController.NoSuchQueue(queueName)
  }

  it should "return an error if looking up failures for an unregistered queue name" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(None))

    controller
      .lookupRequestFailures(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe QueueController.NoSuchQueue(queueName)
  }

  it should "look up details for a transfer" in {
    val transferId = UUID.randomUUID()
    val details = TransferDetails(
      transferId,
      TransferStatus.Failed,
      json"123",
      Some(OffsetDateTime.now()),
      Some(OffsetDateTime.now()),
      Some(json"456")
    )

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.lookupTransferDetails _)
      .expects(queueId, requestId, transferId)
      .returning(IO.pure(Some(details)))

    controller
      .lookupTransferDetails(queueName, requestId, transferId)
      .unsafeRunSync() shouldBe details
  }

  it should "return an error if looking up details for an unregistered queue name" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(None))

    controller
      .lookupTransferDetails(queueName, requestId, UUID.randomUUID())
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe QueueController.NoSuchQueue(queueName)
  }

  it should "return an error if looking up details for a nonexistent request" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(false))

    controller
      .lookupTransferDetails(queueName, requestId, UUID.randomUUID())
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe TransferController.NoSuchRequest(queueName, requestId)
  }

  it should "return an error if looking up details for an unregistered transfer" in {
    val transferId = UUID.randomUUID()

    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.lookupTransferDetails _)
      .expects(queueId, requestId, transferId)
      .returning(IO.pure(None))

    controller
      .lookupTransferDetails(queueName, requestId, transferId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe
      TransferController.NoSuchTransfer(queueName, requestId, transferId)
  }

  it should "reconsider requests" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(true))
    (db.resetTransferFailures _).expects(queueId, requestId).returning(IO.unit)

    controller
      .reconsiderRequest(queueName, requestId)
      .unsafeRunSync() shouldBe RequestAck(requestId)
  }

  it should "return an error if reconsidering under a nonexistent queue" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(None))

    controller
      .reconsiderRequest(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe QueueController.NoSuchQueue(queueName)
  }

  it should "return an error if reconsidering a nonexistent request" in {
    (queueController.lookupQueueInfo _)
      .expects(queueName)
      .returning(IO.pure(Some(queueInfo)))
    (db.checkRequestInQueue _).expects(queueId, requestId).returning(IO.pure(false))

    controller
      .reconsiderRequest(queueName, requestId)
      .attempt
      .unsafeRunSync()
      .left
      .value shouldBe TransferController.NoSuchRequest(queueName, requestId)
  }
}
