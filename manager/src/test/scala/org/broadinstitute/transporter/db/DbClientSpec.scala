package org.broadinstitute.transporter.db

import cats.effect.{ContextShift, IO}
import doobie.implicits._
import doobie.postgres.circe.Instances.JsonInstances
import doobie.util.transactor.Transactor
import io.chrisdavenport.fuuid.FUUID
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.queue.{Queue, QueueSchema}
import org.broadinstitute.transporter.transfer.{
  TransferRequest,
  TransferResult,
  TransferStatus,
  TransferSummary
}
import org.scalatest.{EitherValues, OptionValues}

import scala.concurrent.ExecutionContext

class DbClientSpec
    extends PostgresSpec
    with EitherValues
    with OptionValues
    with JsonInstances {

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private def testTransactor(password: String): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      container.driverClassName,
      container.jdbcUrl,
      container.username,
      password
    )

  private val schema = json"{}".as[QueueSchema].right.value
  private val queue = Queue("test-queue", "requests", "progress", "responses", schema)

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
      id <- FUUID.randomFUUID[IO]
      res <- client.lookupQueue(queue.name)
      _ <- client.createQueue(id, queue)
      res2 <- client.lookupQueue(queue.name)
      _ <- client.deleteQueue(id)
      res3 <- client.lookupQueue(queue.name)
    } yield {
      res shouldBe None
      val (outId, outQueue) = res2.value
      outId shouldBe id
      outQueue shouldBe queue
      res3 shouldBe None
    }

    check.unsafeRunSync()
  }

  it should "fail to double-create a queue by name" in {
    val client = new DbClient.Impl(testTransactor(container.password))

    val tryInsert = for {
      id <- FUUID.randomFUUID[IO]
      _ <- client.createQueue(id, queue)
      _ <- client.createQueue(id, queue)
    } yield ()

    tryInsert.attempt.unsafeRunSync().isLeft shouldBe true
  }

  it should "no-op when deleting a nonexistent queue" in {
    val client = new DbClient.Impl(testTransactor(container.password))
    for {
      id <- FUUID.randomFUUID[IO]
      _ <- client.deleteQueue(id)
    } yield succeed
  }

  it should "record and delete new transfer requests under a queue" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val requests = TransferRequest(List.fill(10)(json"{}"))

    val countsQuery = for {
      requestCount <- sql"select count(*) from transfer_requests".query[Long].unique
      transferCount <- sql"select count(*) from transfers".query[Long].unique
    } yield {
      (requestCount, transferCount)
    }

    val checks = for {
      queueId <- FUUID.randomFUUID[IO]
      _ <- client.createQueue(queueId, queue)
      (initReqs, initTransfers) <- countsQuery.transact(transactor)
      (requestId, _) <- client.recordTransferRequest(queueId, requests)
      (postReqs, postTransfers) <- countsQuery.transact(transactor)
      _ <- client.deleteTransferRequest(requestId)
      (finalReqs, finalTransfers) <- countsQuery.transact(transactor)
      _ <- client.deleteQueue(queueId)
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

  it should "update recorded status and info for transfers" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val requests = TransferRequest(List.tabulate(10)(i => json"""{ "i": $i }"""))
    val results =
      List.tabulate(10) { i =>
        TransferSummary(
          // Success: 0, 2, 4, 6, 8
          // FatalFailure: 1, 3, 5, 7, 9
          TransferResult.values(i % 2),
          // Success: 0+1, 6+1
          // FatalFailure: 3+1, 9+1
          Some(json"""{ "i+1": ${i + 1} }""").filter(_ => i % 3 == 0)
        )
      }

    val checks = for {
      queueId <- FUUID.randomFUUID[IO]
      _ <- client.createQueue(queueId, queue)
      (reqId, reqs) <- client.recordTransferRequest(queueId, requests)
      preResults <- client.lookupTransfers(queueId, reqId)
      _ <- client.updateTransfers(reqs.map(_._1).zip(results))
      postResults <- client.lookupTransfers(queueId, reqId)
      _ <- client.deleteQueue(queueId)
    } yield {
      preResults shouldBe Map((TransferStatus.Submitted, (10, Vector.empty[Json])))
      postResults.keys should contain only (TransferStatus.Succeeded, TransferStatus.Failed)

      val (successCount, successInfo) = postResults(TransferStatus.Succeeded)
      val (failCount, failInfo) = postResults(TransferStatus.Failed)

      successCount shouldBe 5
      failCount shouldBe 5

      successInfo should contain only (json"""{ "i+1": 1 }""", json"""{ "i+1": 7 }""")
      failInfo should contain only (json"""{ "i+1": 4 }""", json"""{ "i+1": 10 }""")
    }

    checks.unsafeRunSync()
  }

}
