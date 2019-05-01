package org.broadinstitute.transporter.db

import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.UUID

import cats.effect.concurrent.Ref
import cats.effect.{Clock, ContextShift, IO}
import cats.implicits._
import doobie.implicits._
import doobie.util.Put
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import fs2.Stream
import fs2.concurrent.{Queue => FSQueue}
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.queue.QueueSchema
import org.broadinstitute.transporter.queue.api.{Queue, QueueParameters}
import org.broadinstitute.transporter.transfer.api.{
  BulkRequest,
  TransferDetails,
  TransferMessage
}
import org.broadinstitute.transporter.transfer.TransferStatus
import org.scalatest.{EitherValues, OptionValues}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class DbClientSpec extends PostgresSpec with EitherValues with OptionValues {

  import DbClient._

  private val initNow = 12345L

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private implicit def clk: Clock[IO] = new Clock[IO] {
    private[this] val now = Ref.unsafe[IO, Long](initNow)

    private[this] def getNow =
      for {
        current <- now.get
        _ <- now.set(current + 1)
      } yield {
        current
      }

    override def realTime(unit: TimeUnit): IO[Long] = getNow
    override def monotonic(unit: TimeUnit): IO[Long] = getNow
  }

  private def testTransactor(password: String): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      container.driverClassName,
      container.jdbcUrl,
      container.username,
      password
    )

  private def odt(epochMillis: Long): OffsetDateTime =
    OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneId.of("UTC"))

  implicit val odtPut: Put[OffsetDateTime] = Put[Timestamp].contramap { odt =>
    Timestamp.from(odt.toInstant)
  }
  // Work-around for a compiler bug: scalac doesn't believe the implicit is used for
  // some reason, but when it's removed the downstream derivation steps fail.
  private val _ = odtPut

  private val schema = json"{}".as[QueueSchema].right.value
  private val queue = Queue("test-queue", "requests", "progress", "responses", schema, 2)
  private val queue2 = queue.copy(
    schema = json"""{ "type": "object" }""".as[QueueSchema].right.value,
    maxConcurrentTransfers = 1
  )
  private val queueId = UUID.randomUUID()

  behavior of "DbClient"

  it should "report ready on good configuration" in {
    val client = new DbClient.Impl(testTransactor(container.password))
    client.checkReady.unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val client = new DbClient.Impl(testTransactor("nope"))
    client.checkReady.unsafeRunSync() shouldBe false
  }

  it should "create, look up, update, and delete transfer queues" in {

    val client = new DbClient.Impl(testTransactor(container.password))

    val check = for {
      res <- client.lookupQueue(queue.name)
      _ <- client.createQueue(queueId, queue)
      res2 <- client.lookupQueue(queue.name)
      (outId, outQueue) = res2.value
      _ <- client.patchQueueParameters(
        outId,
        QueueParameters(
          schema = Some(queue2.schema),
          maxConcurrentTransfers = Some(queue2.maxConcurrentTransfers)
        )
      )
      res3 <- client.lookupQueue(queue.name)
      (updatedId, updatedQueue) = res3.value
      _ <- client.deleteQueue(queueId)
      res4 <- client.lookupQueue(queue.name)
    } yield {
      res shouldBe None

      outId shouldBe queueId
      outQueue shouldBe queue

      updatedId shouldBe queueId
      updatedQueue shouldBe queue2

      res4 shouldBe None
    }

    check.unsafeRunSync()
  }

  it should "fail to double-create a queue by name" in {
    val client = new DbClient.Impl(testTransactor(container.password))

    val tryInsert = for {
      _ <- client.createQueue(queueId, queue)
      _ <- client.createQueue(queueId, queue)
    } yield ()

    tryInsert.attempt.unsafeRunSync().isLeft shouldBe true
  }

  it should "no-op when deleting a nonexistent queue" in {
    val client = new DbClient.Impl(testTransactor(container.password))
    client.deleteQueue(queueId).unsafeRunSync()
  }

  it should "record new transfer requests under a queue" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val requests = BulkRequest(List.fill(10)(json"{}"))

    val countsQuery = for {
      requestCount <- sql"select count(*) from transfer_requests".query[Long].unique
      transferCount <- sql"select count(*) from transfers".query[Long].unique
    } yield {
      (requestCount, transferCount)
    }

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      (initReqs, initTransfers) <- countsQuery.transact(transactor)
      requestId <- client.recordTransferRequest(queueId, requests)
      (postReqs, postTransfers) <- countsQuery.transact(transactor)
      statuses <- sql"select distinct status from transfers where request_id = $requestId"
        .query[TransferStatus]
        .to[Set]
        .transact(transactor)
    } yield {
      initReqs shouldBe 0
      initTransfers shouldBe 0
      postReqs shouldBe 1
      postTransfers shouldBe 10

      statuses shouldBe Set(TransferStatus.Pending)
    }

    checks.unsafeRunSync()
  }

  private val fakeSubmit = Update[(TransferStatus, OffsetDateTime, UUID)](
    "update transfers set status = ?, submitted_at = ? where id = ?"
  )

  private val fakeUpdate = Update[(TransferStatus, OffsetDateTime, Json, UUID)](
    "update transfers set status = ?, updated_at = ?, info = ? where id = ?"
  )

  it should "summarize transfers in a request by status" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(10)(i => json"""{ "i": $i }"""))

    val submitTime = odt(initNow)
    val succeedTime = odt(initNow + 1)

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      reqId <- client.recordTransferRequest(queueId, request)
      initSummary <- client.summarizeTransfersByStatus(queueId, reqId)

      ids <- sql"select id from transfers where request_id = $reqId"
        .query[UUID]
        .to[List]
        .transact(transactor)

      submittedIds = ids.zipWithIndex.collect { case (id, i) if i % 2 == 0 => id }
      _ <- fakeSubmit
        .updateMany(
          submittedIds.map(i => (TransferStatus.Submitted: TransferStatus, submitTime, i))
        )
        .void
        .transact(transactor)
      submitSummary <- client.summarizeTransfersByStatus(queueId, reqId)

      succeededIds = submittedIds.zipWithIndex.collect {
        case (id, i) if i % 2 == 0 => id
      }
      _ <- fakeUpdate
        .updateMany(
          succeededIds.map { i =>
            (TransferStatus.Succeeded: TransferStatus, succeedTime, json"$i", i)
          }
        )
        .void
        .transact(transactor)
      succeedSummary <- client.summarizeTransfersByStatus(queueId, reqId)

    } yield {
      initSummary shouldBe Map((TransferStatus.Pending, (10L, None, None)))
      submitSummary shouldBe Map(
        (TransferStatus.Pending, (5L, None, None)),
        (TransferStatus.Submitted, (5L, Some(submitTime), None))
      )
      succeedSummary shouldBe Map(
        (TransferStatus.Pending, (5L, None, None)),
        (TransferStatus.Submitted, (2L, Some(submitTime), None)),
        (TransferStatus.Succeeded, (3L, Some(submitTime), Some(succeedTime)))
      )
    }

    checks.unsafeRunSync()
  }

  it should "look up messages for transfers with a certain status" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(10)(i => json"""{ "i": $i }"""))

    val checks = for {
      // Setup rows for request.
      _ <- client.createQueue(queueId, queue)
      reqId <- client.recordTransferRequest(queueId, request)
      initMessages <- client.lookupTransferMessages(
        queueId,
        reqId,
        TransferStatus.Failed
      )

      ids <- sql"select id from transfers where request_id = $reqId"
        .query[UUID]
        .to[List]
        .transact(transactor)
      _ <- fakeUpdate
        .updateMany(
          ids.map { i =>
            (TransferStatus.Failed: TransferStatus, odt(initNow), json"$i", i)
          }
        )
        .void
        .transact(transactor)
      postMessages <- client.lookupTransferMessages(queueId, reqId, TransferStatus.Failed)
    } yield {
      initMessages shouldBe empty
      postMessages should contain theSameElementsAs ids.map { i =>
        TransferMessage(i, json"$i")
      }
    }

    checks.unsafeRunSync()
  }

  it should "look up details for transfers" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = json"""{ "i": 1 }"""

    val submitTime = odt(initNow)
    val updateTime = odt(initNow + 1)

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      reqId <- client.recordTransferRequest(queueId, BulkRequest(List(request)))
      id <- sql"select id from transfers where request_id = $reqId"
        .query[UUID]
        .unique
        .transact(transactor)
      initDetails <- client.lookupTransferDetails(queueId, reqId, id)

      _ <- fakeSubmit
        .run((TransferStatus.Submitted, submitTime, id))
        .void
        .transact(transactor)
      postSubmitDetails <- client.lookupTransferDetails(queueId, reqId, id)

      _ <- fakeUpdate
        .run((TransferStatus.Succeeded, updateTime, json"1", id))
        .void
        .transact(transactor)
      postUpdateDetails <- client.lookupTransferDetails(queueId, reqId, id)

    } yield {

      initDetails.value shouldBe TransferDetails(
        id,
        TransferStatus.Pending,
        request,
        None,
        None,
        None
      )

      postSubmitDetails.value shouldBe TransferDetails(
        id,
        TransferStatus.Submitted,
        request,
        Some(submitTime),
        None,
        None
      )

      postUpdateDetails.value shouldBe TransferDetails(
        id,
        TransferStatus.Succeeded,
        request,
        Some(submitTime),
        Some(updateTime),
        Some(json"1")
      )
    }

    checks.unsafeRunSync()
  }

  it should "not fail if querying details for a nonexistent transfer" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List(json"""{ "i": 1 }"""))

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      reqId <- client.recordTransferRequest(queueId, request)
      details <- client.lookupTransferDetails(queueId, reqId, UUID.randomUUID())
    } yield {
      details shouldBe None
    }

    checks.unsafeRunSync()
  }

  it should "submit batches of pending transfers" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val queueId2 = UUID.randomUUID()
    val queue2 =
      queue.copy(name = "foo", requestTopic = "requests2", maxConcurrentTransfers = 10)

    val request1 = BulkRequest(List.tabulate(queue.maxConcurrentTransfers * 2) { i =>
      json"""{ "i": $i }"""
    })
    val request2 = BulkRequest(List.tabulate(queue2.maxConcurrentTransfers * 2) { i =>
      json"""{ "i": $i }"""
    })

    val getSubmitTimes =
      sql"select distinct submitted_at from transfers where submitted_at is not null"
        .query[OffsetDateTime]

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      _ <- client.createQueue(queueId2, queue2)

      batch0 <- client.submitTransfers(IO.pure)
      submitTime0 <- getSubmitTimes.option.transact(transactor)

      reqId1 <- client.recordTransferRequest(queueId, request1)
      reqId2 <- client.recordTransferRequest(queueId2, request2)
      ids1 <- sql"select id from transfers where request_id = $reqId1"
        .query[UUID]
        .to[List]
        .transact(transactor)
      ids2 <- sql"select id from transfers where request_id = $reqId2"
        .query[UUID]
        .to[List]
        .transact(transactor)

      batch1 <- client.submitTransfers(IO.pure)
      submitTime1 <- getSubmitTimes.unique.transact(transactor)
      batch2 <- client.submitTransfers(IO.pure)

      idsToUpdate = List.concat(
        ids1.zipWithIndex.collect { case (id, i) if i % 2 == 0 => id },
        ids2.zipWithIndex.collect { case (id, i) if i % 2 == 0 => id }
      )
      _ <- fakeUpdate.updateMany {
        idsToUpdate.zipWithIndex.map {
          case (id, i) =>
            val status =
              if (i % 2 == 0) TransferStatus.Succeeded else TransferStatus.Failed
            (status, odt(initNow + 2), json"$i", id)
        }
      }.transact(transactor)

      batch3 <- client.submitTransfers(IO.pure)
      submitTimes2 <- getSubmitTimes.to[Set].transact(transactor)
    } yield {
      // Sanity-check: Nothing to submit when no requests persisted.
      batch0 should contain theSameElementsAs List(
        queue.requestTopic -> Nil,
        queue2.requestTopic -> Nil
      )
      submitTime0 shouldBe None

      // Nothing in flight yet, submission batch should have max elements.
      val transfersMap1 = batch1.toMap
      transfersMap1.keySet shouldBe Set(queue.requestTopic, queue2.requestTopic)
      val queue1Batch = transfersMap1(queue.requestTopic)
      val queue2Batch = transfersMap1(queue2.requestTopic)

      queue1Batch should have length queue.maxConcurrentTransfers.toLong
      queue2Batch should have length queue2.maxConcurrentTransfers.toLong

      queue1Batch.map(_.id).toSet.subsetOf(ids1.toSet) shouldBe true
      queue2Batch.map(_.id).toSet.subsetOf(ids2.toSet) shouldBe true

      // Submission time should've been updated.
      submitTime1.toInstant shouldBe Instant.ofEpochMilli(initNow + 1)

      // Sanity-check: Immediate re-submission should pull nothing from DB.
      batch2 should contain theSameElementsAs List(
        queue.requestTopic -> Nil,
        queue2.requestTopic -> Nil
      )

      // Post-completion: New IDs should be pulled for submission.
      val transfersMap2 = batch3.toMap
      transfersMap2.keySet shouldBe Set(queue.requestTopic, queue2.requestTopic)
      val queue1Batch2 = transfersMap2(queue.requestTopic)
      val queue2Batch2 = transfersMap2(queue2.requestTopic)

      queue1Batch2 should have length queue.maxConcurrentTransfers / 2L
      queue2Batch2 should have length queue2.maxConcurrentTransfers / 2L

      queue1Batch2.map(_.id).toSet.subsetOf(ids1.toSet) shouldBe true
      queue2Batch2.map(_.id).toSet.subsetOf(ids2.toSet) shouldBe true

      queue1Batch2.toSet.intersect(queue1Batch.toSet) shouldBe empty
      queue2Batch2.toSet.intersect(queue2Batch.toSet) shouldBe empty

      // Submission time should be new.
      submitTimes2.map(_.toInstant) shouldBe Set(initNow + 1, initNow + 3)
        .map(Instant.ofEpochMilli)
    }

    checks.unsafeRunSync()
  }

  it should "not crash if more than max concurrent transfers are running during the sweep" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(queue.maxConcurrentTransfers * 2) { i =>
      json"""{ "i": $i }"""
    })

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      reqId <- client.recordTransferRequest(queueId, request)
      _ <- sql"""update transfers set status = ${TransferStatus.Submitted: TransferStatus}
                 where request_id = $reqId""".update.run.void.transact(transactor)
      batch <- client.submitTransfers(IO.pure)
    } yield {
      batch shouldBe List(queue.requestTopic -> Nil)
    }

    checks.unsafeRunSync()
  }

  it should "not mark transfers as submitted if the submission step fails" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(queue.maxConcurrentTransfers * 2) { i =>
      json"""{ "i": $i }"""
    })

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      _ <- client.recordTransferRequest(queueId, request)
      attempt <- client
        .submitTransfers(_ => IO.raiseError[Unit](new RuntimeException("BOOM")))
        .attempt
      submittedIds <- sql"select id from transfers where status = 'submitted'"
        .query[UUID]
        .to[List]
        .transact(transactor)
      submitTime <- sql"select distinct submitted_at from transfers where submitted_at is not null"
        .query[OffsetDateTime]
        .option
        .transact(transactor)
    } yield {
      attempt.isLeft shouldBe true
      submittedIds shouldBe empty
      submitTime shouldBe None
    }

    checks.unsafeRunSync()
  }

  it should "not double-submit transfers on concurrent access" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(queue.maxConcurrentTransfers * 2) { i =>
      json"""{ "i": $i }"""
    })

    val checks = for {
      submittedStream <- FSQueue.noneTerminated[IO, UUID]
      _ <- client.createQueue(queueId, queue)
      _ <- client.recordTransferRequest(queueId, request)

      doSubmit = client.submitTransfers { transfers =>
        Stream
          .emits(transfers.flatMap(_._2.map(_.id)))
          .map(Some(_))
          .through(submittedStream.enqueue)
          .compile
          .drain
      }
      _ <- (doSubmit, doSubmit, doSubmit).parTupled.void
      _ <- submittedStream.enqueue1(None)
      submittedIds <- submittedStream.dequeue.compile.toList
    } yield {
      submittedIds should have length queue.maxConcurrentTransfers.toLong
      submittedIds.distinct shouldBe submittedIds
    }

    checks.unsafeRunSync()
  }
}
