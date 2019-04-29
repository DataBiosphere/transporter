package org.broadinstitute.transporter.db

import java.time.{Instant, OffsetDateTime}
import java.util.UUID

import cats.data.NonEmptyList
import cats.effect.concurrent.Ref
import cats.effect.{Clock, ContextShift, IO}
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.queue.QueueSchema
import org.broadinstitute.transporter.queue.api.Queue
import org.broadinstitute.transporter.transfer.api.{BulkRequest, TransferMessage}
import org.broadinstitute.transporter.transfer.{
  TransferResult,
  TransferStatus,
  TransferSummary
}
import org.scalatest.{EitherValues, OptionValues}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.TimeUnit

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

  private val schema = json"{}".as[QueueSchema].right.value
  private val queue = Queue("test-queue", "requests", "progress", "responses", schema)
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

  it should "create, lookup, and delete transfer queues" in {

    val client = new DbClient.Impl(testTransactor(container.password))

    val check = for {
      res <- client.lookupQueue(queue.name)
      _ <- client.createQueue(queueId, queue)
      res2 <- client.lookupQueue(queue.name)
      _ <- client.deleteQueue(queueId)
      res3 <- client.lookupQueue(queue.name)
    } yield {
      res shouldBe None
      val (outId, outQueue) = res2.value
      outId shouldBe queueId
      outQueue shouldBe queue
      res3 shouldBe None
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

  it should "record and delete new transfer requests under a queue" in {
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
      (requestId, _) <- client.recordTransferRequest(queueId, requests)
      (postReqs, postTransfers) <- countsQuery.transact(transactor)
      _ <- client.deleteTransferRequest(requestId)
      (finalReqs, finalTransfers) <- countsQuery.transact(transactor)
    } yield {
      initReqs shouldBe 0
      initTransfers shouldBe 0
      postReqs shouldBe 1
      postTransfers shouldBe 10
      finalReqs shouldBe 0
      finalTransfers shouldBe 0
    }

    checks.unsafeRunSync()
  }

  it should "add submission times to new transfers" in {

    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.fill(10)(json"{}"))

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      (_, reqs) <- client.recordTransferRequest(queueId, request)
      ids <- NonEmptyList
        .fromList(reqs.map(_._1))
        .liftTo[IO](new IllegalStateException("No IDs returned"))
      submitTimes <- Fragments
        .in(fr"select submitted_at from transfers where id", ids)
        .query[OffsetDateTime]
        .to[List]
        .transact(transactor)
    } yield {
      submitTimes.map(_.toInstant) shouldBe List.fill(10)(Instant.ofEpochMilli(initNow))
    }

    checks.unsafeRunSync()
  }

  it should "update recorded status, info, and updated times for transfers" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(10)(i => json"""{ "i": $i }"""))

    val checks = for {
      // Setup rows for request.
      _ <- client.createQueue(queueId, queue)
      (reqId, reqs) <- client.recordTransferRequest(queueId, request)
      query = sql"""select id, status, info, updated_at from transfers where request_id = $reqId"""
        .query[(UUID, TransferStatus, Option[Json], Option[OffsetDateTime])]
        .to[List]
        .transact(transactor)

      // Get "before" info.
      preInfo <- query

      // Run a fake update on parts of the request.
      results = reqs.zipWithIndex.collect {
        case ((id, _), i) if i % 3 == 0 =>
          TransferSummary(
            TransferResult.values(i % 2),
            json"""{ "i+1": ${i + 1} }""",
            id,
            reqId
          )
      }
      _ <- client.updateTransfers(results)

      // Get "after" info.
      postInfo <- query
    } yield {
      // IDs should match before and after update.
      preInfo.map(_._1) should contain theSameElementsAs postInfo.map(_._1)

      // Statuses, infos, and update times should have been updated.
      preInfo.map(_._2).toSet should contain only TransferStatus.Submitted
      postInfo.map(_._2).toSet should contain theSameElementsAs TransferStatus.values

      preInfo.flatMap(_._3) shouldBe empty
      postInfo.flatMap(_._3) should contain theSameElementsAs results.map(_.info)

      preInfo.flatMap(_._4) shouldBe empty
      postInfo
        .flatMap(_._4)
        .map(_.toInstant) shouldBe Iterable.fill(4)(Instant.ofEpochMilli(initNow + 1))
    }

    checks.unsafeRunSync()
  }

  it should "summarize transfers in a request by status" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(10)(i => json"""{ "i": $i }"""))

    val checks = for {
      _ <- client.createQueue(queueId, queue)
      (reqId, reqs) <- client.recordTransferRequest(queueId, request)
      results1 = reqs.zipWithIndex.collect {
        case ((id, _), i) if i % 3 == 0 =>
          TransferSummary(
            TransferResult.values(i % 2),
            json"""{ "i+1": ${i + 1} }""",
            id,
            reqId
          )
      }
      _ <- client.updateTransfers(results1)
      results2 = reqs.zipWithIndex.collect {
        case ((id, _), i) if i % 3 == 2 =>
          TransferSummary(
            TransferResult.values(i % 2),
            json"""{ "i+1": ${i + 1} }""",
            id,
            reqId
          )
      }
      _ <- client.updateTransfers(results2)
      summary <- client.summarizeTransfersByStatus(queueId, reqId)
    } yield {
      summary.keySet shouldBe TransferStatus.values.toSet

      val (submittedCount, firstSubmittedSubmission, lastSubmittedUpdated) =
        summary(TransferStatus.Submitted)
      val (succeededCount, firstSucceededSubmitted, lastSucceededUpdated) =
        summary(TransferStatus.Succeeded)
      val (failedCount, firstFailedSubmitted, lastFailedUpdated) =
        summary(TransferStatus.Failed)

      submittedCount shouldBe 3
      succeededCount shouldBe 4
      failedCount shouldBe 3

      Set(firstSubmittedSubmission, firstSucceededSubmitted, firstFailedSubmitted).flatten
        .map(_.toInstant) shouldBe Set(Instant.ofEpochMilli(initNow))

      lastSubmittedUpdated shouldBe None

      Set(lastSucceededUpdated, lastFailedUpdated).flatten
        .map(_.toInstant) shouldBe Set(Instant.ofEpochMilli(initNow + 2))
    }

    checks.unsafeRunSync()
  }

  it should "lookup messages for transfers with a certain status" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val request = BulkRequest(List.tabulate(10)(i => json"""{ "i": $i }"""))

    val checks = for {
      // Setup rows for request.
      _ <- client.createQueue(queueId, queue)
      (reqId, reqs) <- client.recordTransferRequest(queueId, request)

      // Get "before" info.
      preOutputs <- client.lookupTransferMessages(
        queueId,
        reqId,
        TransferStatus.Succeeded
      )
      preFailures <- client.lookupTransferMessages(queueId, reqId, TransferStatus.Failed)

      // Run a fake update on parts of the request.
      results = reqs.zipWithIndex.collect {
        case ((id, _), i) if i % 3 == 0 =>
          TransferSummary(
            TransferResult.values(i % 2),
            json"""{ "i+1": ${i + 1} }""",
            id,
            reqId
          )
      }
      _ <- client.updateTransfers(results)

      // Get "after" info.
      postOutputs <- client.lookupTransferMessages(
        queueId,
        reqId,
        TransferStatus.Succeeded
      )
      postFailures <- client.lookupTransferMessages(queueId, reqId, TransferStatus.Failed)
    } yield {
      val succeeded = results.filter(_.result == TransferResult.Success).map { res =>
        TransferMessage(res.id, res.info)
      }
      val failed = results.filter(_.result == TransferResult.FatalFailure).map { res =>
        TransferMessage(res.id, res.info)
      }

      preOutputs shouldBe empty
      preFailures shouldBe empty

      postOutputs should contain theSameElementsAs succeeded
      postFailures should contain theSameElementsAs failed
    }

    checks.unsafeRunSync()
  }
}
