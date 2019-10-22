package org.broadinstitute.transporter.transfer

import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.UUID

import cats.effect.{Clock, IO}
import cats.implicits._
import doobie._
import doobie.implicits._
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.error.ApiError._
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.config.TransferSchema
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues

import scala.concurrent.duration.TimeUnit

class TransferControllerSpec extends PostgresSpec with MockFactory with EitherValues {
  import org.broadinstitute.transporter.db.DoobieInstances._

  val nowMillis = 1234L

  private implicit val clk: Clock[IO] = new Clock[IO] {
    override def realTime(unit: TimeUnit): IO[Long] = IO.pure(nowMillis)
    override def monotonic(unit: TimeUnit): IO[Long] = IO.pure(nowMillis)
  }

  private val requestSchema =
    json"""{ "type": "object" }""".as[TransferSchema].right.value

  private val request1Id = UUID.randomUUID()

  private val request1Transfers = List.tabulate(10) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val request2Id = UUID.randomUUID()

  private val request2Transfers = List.tabulate(20) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  def withController(test: (Transactor[IO], TransferController) => IO[Any]): Unit = {
    val tx = transactor
    test(tx, new TransferController(requestSchema, tx)).unsafeRunSync()
    ()
  }

  def withRequest(test: (Transactor[IO], TransferController) => IO[Any]): Unit =
    withController { (tx, controller) =>
      val setup = for {
        _ <- List(request1Id, request2Id).zipWithIndex.traverse_ {
          case (id, i) =>
            val ts = Timestamp.from(Instant.ofEpochMilli(nowMillis + i))
            sql"INSERT INTO transfer_requests (id, received_at) VALUES ($id, $ts)".update.run.void
        }
        _ <- request1Transfers.traverse_ {
          case (id, body) =>
            sql"""INSERT INTO transfers
                  (id, request_id, body, status, steps_run, priority)
                  VALUES
                  ($id, $request1Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 0)""".update.run.void
        }
        _ <- request2Transfers.traverse_ {
          case (id, body) =>
            sql"""INSERT INTO transfers
                  (id, request_id, body, status, steps_run, priority)
                  VALUES
                  ($id, $request2Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 0)""".update.run.void
        }
      } yield ()

      setup.transact(tx).flatMap(_ => test(tx, controller))
    }

  private val countRequests = fr"SELECT COUNT(*) FROM transfer_requests"
  private val countTransfers = fr"SELECT COUNT(*) FROM transfers"

  behavior of "TransferController"

  it should "record new transfers with well-formed requests" in withController {
    (tx, controller) =>
      val transferCount = 10
      val request = BulkRequest(
        List.tabulate(transferCount)(i => TransferRequest(json"""{ "i": $i }""", None))
      )

      for {
        (initRequests, initTransfers) <- (
          countRequests.query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
        ack <- controller.recordRequest(request)
        (postRequests, postTransfers) <- (
          (countRequests ++ fr"WHERE id = ${ack.id}")
            .query[Long]
            .unique,
          (countTransfers ++ fr"WHERE request_id = ${ack.id}").query[Long].unique
        ).tupled.transact(tx)
      } yield {
        initRequests shouldBe 0
        initTransfers shouldBe 0
        ack.updatedCount shouldBe transferCount
        postRequests shouldBe 1
        postTransfers shouldBe transferCount
      }
  }

  it should "not record new transfers with malformed requests" in withController {
    (tx, controller) =>
      val transferCount = 10
      val request = BulkRequest(
        List.tabulate(transferCount)(i => TransferRequest(json"""[$i]""", None))
      )

      for {
        (initRequests, initTransfers) <- (
          countRequests.query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
        ackOrErr <- controller.recordRequest(request).attempt
        (postRequests, postTransfers) <- (
          countRequests.query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
      } yield {
        initRequests shouldBe 0
        initTransfers shouldBe 0
        ackOrErr.left.value shouldBe an[InvalidRequest]
        postRequests shouldBe 0
        postTransfers shouldBe 0
      }
  }

  it should "merge defaults correctly into transfer requests" in withController {
    (tx, controller) =>
      val transferCount = 10
      val j = 2
      val altJ = 5
      val k = 3
      val pri = 3.toShort
      val altPri = 2.toShort
      val request = BulkRequest(
        List.tabulate(transferCount) { i =>
          TransferRequest(
            if (i % 2 == 0) json"""{ "i": $i, "j": $j }""" else json"""{ "i": $i }""",
            if (i % 2 == 0) Some(pri) else None
          )
        },
        Some(TransferRequest(json"""{ "j": $altJ, "k": $k }""", Some(altPri)))
      )

      val expected = List.tabulate(transferCount) { i =>
        if (i % 2 == 0)
          (json"""{ "i": $i, "j": $j, "k": $k }""", pri)
        else
          (json"""{ "i": $i, "j": $altJ, "k": $k }""", altPri)
      }

      for {
        _ <- controller.recordRequest(request)
        actual <- sql"""SELECT body, priority FROM transfers"""
          .query[(Json, Int)]
          .to[List]
          .transact(tx)
      } yield {
        actual should contain theSameElementsAs expected
      }
  }

  it should "count all tracked requests" in withRequest { (_, controller) =>
    controller.countRequests.map(_ shouldBe 2)
  }

  it should "not fail counting when no requests are tracked" in withController {
    (_, controller) =>
      controller.countRequests.map(_ shouldBe 0)
  }

  it should "get summaries for all tracked requests in order" in withRequest {
    (tx, controller) =>
      for {
        _ <- request1Transfers.zipWithIndex.traverse_ {
          case ((id, _), i) =>
            val status = TransferStatus.values(i % TransferStatus.values.length)
            sql"UPDATE transfers SET status = $status WHERE id = $id".update.run
        }.transact(tx)
        _ <- request2Transfers.zipWithIndex.traverse_ {
          case ((id, _), i) =>
            val status = TransferStatus.values(i % TransferStatus.values.length)
            sql"UPDATE transfers SET status = $status WHERE id = $id".update.run
        }.transact(tx)
        oldestToNewest <- controller.listRequests(
          offset = 0,
          limit = 2,
          newestFirst = false
        )
        newestToOldest <- controller.listRequests(
          offset = 0,
          limit = 2,
          newestFirst = true
        )
      } yield {
        oldestToNewest should have length 2
        newestToOldest should have length 2
        oldestToNewest.reverse shouldBe newestToOldest

        val req1 = oldestToNewest.head
        val req2 = oldestToNewest.last

        req1.id shouldBe request1Id
        req1.statusCounts shouldBe Map(
          TransferStatus.Pending -> 2,
          TransferStatus.Submitted -> 2,
          TransferStatus.InProgress -> 2,
          TransferStatus.Failed -> 2,
          TransferStatus.Succeeded -> 1,
          TransferStatus.Expanded -> 1
        )
        req2.id shouldBe request2Id
        req2.statusCounts shouldBe Map(
          TransferStatus.Pending -> 4,
          TransferStatus.Submitted -> 4,
          TransferStatus.InProgress -> 3,
          TransferStatus.Succeeded -> 3,
          TransferStatus.Failed -> 3,
          TransferStatus.Expanded -> 3
        )
      }
  }

  it should "paginate list of tracked summaries" in withRequest { (tx, controller) =>
    for {
      _ <- request1Transfers.zipWithIndex.traverse_ {
        case ((id, _), i) =>
          val status = TransferStatus.values(i % TransferStatus.values.length)
          sql"UPDATE transfers SET status = $status WHERE id = $id".update.run
      }.transact(tx)
      _ <- request2Transfers.zipWithIndex.traverse_ {
        case ((id, _), i) =>
          val status = TransferStatus.values(i % TransferStatus.values.length)
          sql"UPDATE transfers SET status = $status WHERE id = $id".update.run
      }.transact(tx)
      // Order oldest to newest, skip the first one, expect to get the newest
      out1 <- controller.listRequests(offset = 1, limit = 10, newestFirst = false)
      // Order newest to oldest, skip the first one, expect to get the oldest
      out2 <- controller.listRequests(offset = 1, limit = 10, newestFirst = true)
      // Skip past everything to make sure we don't crash on nonsense limits
      out3 <- controller.listRequests(offset = 100, limit = 1, newestFirst = true)
    } yield {
      out1 should have length 1
      out2 should have length 1
      out3 shouldBe empty

      out1.head.id shouldBe request2Id
      out1.head.statusCounts shouldBe Map(
        TransferStatus.Pending -> 4,
        TransferStatus.Submitted -> 4,
        TransferStatus.InProgress -> 3,
        TransferStatus.Succeeded -> 3,
        TransferStatus.Failed -> 3,
        TransferStatus.Expanded -> 3
      )
      out2.head.id shouldBe request1Id
      out2.head.statusCounts shouldBe Map(
        TransferStatus.Pending -> 2,
        TransferStatus.Submitted -> 2,
        TransferStatus.InProgress -> 2,
        TransferStatus.Failed -> 2,
        TransferStatus.Succeeded -> 1,
        TransferStatus.Expanded -> 1
      )
    }
  }

  it should "get summaries for registered transfers" in withRequest { (tx, controller) =>
    val now = Instant.now
    val fakeSubmitted = request1Transfers.take(3).map(_._1)
    val fakeInProgress = request1Transfers(6)._1
    val fakeSucceeded = request1Transfers.takeRight(2).map(_._1)
    val fakeFailed = request1Transfers.slice(3, 6).map(_._1)

    val transaction = for {
      _ <- fakeSubmitted.traverse_ { id =>
        sql"""UPDATE transfers SET
                status = ${TransferStatus.Submitted: TransferStatus},
                submitted_at = ${Timestamp.from(now)}
                WHERE id = $id""".update.run
      }
      _ <- sql"""UPDATE transfers SET
                   status = ${TransferStatus.InProgress: TransferStatus},
                   submitted_at = ${Timestamp.from(now)},
                   updated_at = ${Timestamp.from(now.plusMillis(10000))}
                   WHERE id = $fakeInProgress""".update.run
      _ <- fakeSucceeded.traverse_ { id =>
        sql"""UPDATE transfers SET
                status = ${TransferStatus.Succeeded: TransferStatus},
                submitted_at = ${Timestamp.from(now.minusMillis(30000))},
                updated_at = ${Timestamp.from(now)}
                WHERE id = $id""".update.run
      }
      _ <- fakeFailed.traverse_ { id =>
        sql"""UPDATE transfers SET
                status = ${TransferStatus.Failed: TransferStatus},
                submitted_at = ${Timestamp.from(now.plusMillis(30000))},
                updated_at = ${Timestamp.from(now.minusMillis(30000))}
                WHERE id = $id""".update.run
      }
    } yield ()

    for {
      _ <- transaction.transact(tx)
      summary <- controller.lookupRequestStatus(request1Id)
    } yield {
      summary shouldBe RequestSummary(
        request1Id,
        OffsetDateTime.ofInstant(Instant.ofEpochMilli(nowMillis), ZoneId.of("UTC")),
        Map(
          TransferStatus.Pending -> 1,
          TransferStatus.Submitted -> 3,
          TransferStatus.Failed -> 3,
          TransferStatus.Succeeded -> 2,
          TransferStatus.InProgress -> 1,
          TransferStatus.Expanded -> 0
        ),
        Some(OffsetDateTime.ofInstant(now.minusMillis(30000), ZoneId.of("UTC"))),
        Some(OffsetDateTime.ofInstant(now.plusMillis(10000), ZoneId.of("UTC")))
      )
    }
  }

  it should "include zero counts for unrepresented statuses in request summaries" in withRequest {
    (_, controller) =>
      for {
        summary1 <- controller.lookupRequestStatus(request1Id)
        summary2 <- controller.lookupRequestStatus(request2Id)
      } yield {
        summary1 shouldBe RequestSummary(
          request1Id,
          OffsetDateTime.ofInstant(Instant.ofEpochMilli(nowMillis), ZoneId.of("UTC")),
          TransferStatus.values.map(_ -> 0L).toMap ++ Map(
            TransferStatus.Pending -> request1Transfers.length.toLong
          ),
          None,
          None
        )
        summary2 shouldBe RequestSummary(
          request2Id,
          OffsetDateTime.ofInstant(Instant.ofEpochMilli(nowMillis + 1), ZoneId.of("UTC")),
          TransferStatus.values.map(_ -> 0L).toMap ++ Map(
            TransferStatus.Pending -> request2Transfers.length.toLong
          ),
          None,
          None
        )
      }
  }

  it should "fail to get summaries for nonexistent requests" in withController {
    (_, controller) =>
      controller
        .lookupRequestStatus(request1Id)
        .attempt
        .map(_.left.value shouldBe NotFound(request1Id))
  }

  it should "reconsider failed transfers in a request" in withRequest {
    (tx, controller) =>
      val failed = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 == 0 => id
      }

      for {
        _ <- failed.traverse_ { id =>
          sql"UPDATE transfers SET status = ${TransferStatus.Failed: TransferStatus} WHERE id = $id".update.run.void
        }.transact(tx)
        ack <- controller.reconsiderRequest(request1Id)
        n <- sql"SELECT COUNT(*) FROM transfers WHERE status = ${TransferStatus.Failed: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        ack shouldBe RequestAck(request1Id, failed.length)
        n shouldBe 0
      }
  }

  it should "not reconsider failures in an unrelated request" in withRequest {
    (tx, controller) =>
      val failed = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 == 0 => id
      }

      for {
        _ <- failed.traverse_ { id =>
          sql"UPDATE transfers SET status = ${TransferStatus.Failed: TransferStatus} WHERE id = $id".update.run.void
        }.transact(tx)
        ack <- controller.reconsiderRequest(request2Id)
        n <- sql"SELECT COUNT(*) FROM transfers WHERE status = ${TransferStatus.Failed: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        ack shouldBe RequestAck(request2Id, 0)
        n shouldBe failed.length
      }
  }

  it should "reconsider a specific failed transfer in a request" in withRequest {
    (tx, controller) =>
      val (id, _) = request1Transfers.head
      for {
        _ <- sql"UPDATE transfers SET status = ${TransferStatus.Failed: TransferStatus} WHERE id = $id".update.run.void
          .transact(tx)
        ack <- controller.reconsiderSingleTransfer(request1Id, id)
        n <- sql"SELECT COUNT(*) FROM transfers WHERE status = ${TransferStatus.Failed: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        ack shouldBe RequestAck(request1Id, 1)
        n shouldBe 0
      }
  }

  it should "fail if the transfer is not a part of the enclosing request" in withController {
    val transferId = UUID.randomUUID()
    (_, controller) =>
      controller
        .reconsiderSingleTransfer(request1Id, transferId)
        .attempt
        .map(_.left.value shouldBe NotFound(request1Id, Some(transferId)))
  }

  it should "not reconsider a transfer that is in any state apart from failure" in withRequest {
    (tx, controller) =>
      val (id, _) = request1Transfers.head
      for {
        _ <- sql"""UPDATE transfers
                SET status = ${TransferStatus.InProgress: TransferStatus}
                WHERE id = $id""".update.run.void
          .transact(tx)
        before <- sql"SELECT COUNT(*) FROM transfers WHERE status = ${TransferStatus.Pending: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
        ack <- controller.reconsiderSingleTransfer(request1Id, id)
        after <- sql"SELECT COUNT(*) FROM transfers WHERE status = ${TransferStatus.Pending: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        ack shouldBe RequestAck(request1Id, 0)
        before shouldBe after
      }
  }

  it should "get details for a single transfer" in withRequest { (tx, controller) =>
    val (id, body) = request1Transfers.head
    val submitted = Instant.now()
    val updated = submitted.plusMillis(30000)

    for {
      initInfo <- controller.lookupTransferDetails(request1Id, id)
      _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Submitted: TransferStatus},
                 submitted_at = ${Timestamp.from(submitted)}
                 WHERE id = $id""".update.run.void.transact(tx)
      submittedInfo <- controller.lookupTransferDetails(request1Id, id)
      _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Succeeded: TransferStatus},
                 updated_at = ${Timestamp.from(updated)},
                 info = '{}',
                 priority = 100
                 WHERE id = $id""".update.run.void.transact(tx)
      updatedInfo <- controller.lookupTransferDetails(request1Id, id)
    } yield {
      initInfo shouldBe TransferDetails(
        id,
        TransferStatus.Pending,
        0.toShort,
        body,
        None,
        None,
        None
      )
      submittedInfo shouldBe TransferDetails(
        id,
        TransferStatus.Submitted,
        0.toShort,
        body,
        Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
        None,
        None
      )
      updatedInfo shouldBe TransferDetails(
        id,
        TransferStatus.Succeeded,
        100.toShort,
        body,
        Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
        Some(OffsetDateTime.ofInstant(updated, ZoneId.of("UTC"))),
        Some(json"{}")
      )
    }
  }

  it should "fail to get details under a nonexistent request" in withController {
    val transferId = UUID.randomUUID()
    (_, controller) =>
      controller
        .lookupTransferDetails(request1Id, transferId)
        .attempt
        .map(_.left.value shouldBe NotFound(request1Id, Some(transferId)))
  }

  it should "fail to get details for a nonexistent transfer" in withRequest {
    (_, controller) =>
      controller
        .lookupTransferDetails(request1Id, request2Transfers.head._1)
        .attempt
        .map(
          _.left.value shouldBe NotFound(request1Id, Some(request2Transfers.head._1))
        )
  }

  it should "count all tracked transfers" in withRequest { (_, controller) =>
    controller.countTransfers(request1Id).map(_ shouldBe 10)
  }

  it should "count all tracked transfers with a given status" in withRequest {
    (tx, controller) =>
      val updatedIds = request1Transfers.take(2).map(_._1)
      for {
        _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Submitted: TransferStatus}
                 WHERE id = ${updatedIds(0)} OR id = ${updatedIds(1)}""".update.run.void
          .transact(tx)
        submittedCount <- controller.countTransfers(
          request1Id,
          Some(TransferStatus.Submitted)
        )
        pendingCount <- controller.countTransfers(
          request1Id,
          Some(TransferStatus.Pending)
        )
      } yield {
        submittedCount shouldBe updatedIds.length
        pendingCount shouldBe 10 - updatedIds.length
      }
  }

  it should "get details for multiple transfers" in withRequest { (tx, controller) =>
    val request1TransfersSorted = request1Transfers.sortBy(_._1.toString)
    val (id1, body1) = request1TransfersSorted(2)
    val (id2, body2) = request1TransfersSorted(3)
    val submitted = Instant.now()
    val updated = submitted.plusMillis(30000)

    for {
      initInfo <- controller.listTransfers(request1Id, 2, 2, sortDesc = false)
      _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Submitted: TransferStatus},
                 submitted_at = ${Timestamp.from(submitted)},
                 priority = 10
                 WHERE id = $id1""".update.run.void.transact(tx)
      _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Submitted: TransferStatus},
                 submitted_at = ${Timestamp.from(submitted)}
                 WHERE id = $id2""".update.run.void.transact(tx)
      submittedInfo <- controller.listTransfers(request1Id, 2, 2, sortDesc = false)
      _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Succeeded: TransferStatus},
                 updated_at = ${Timestamp.from(updated)},
                 info = '{}'
                 WHERE id = $id1""".update.run.void.transact(tx)
      _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Succeeded: TransferStatus},
                 updated_at = ${Timestamp.from(updated)},
                 info = '{}'
                 WHERE id = $id2""".update.run.void.transact(tx)
      updatedInfo <- controller.listTransfers(request1Id, 2, 2, sortDesc = false)
    } yield {
      initInfo shouldBe List(
        TransferDetails(
          id1,
          TransferStatus.Pending,
          0.toShort,
          body1,
          None,
          None,
          None
        ),
        TransferDetails(
          id2,
          TransferStatus.Pending,
          0.toShort,
          body2,
          None,
          None,
          None
        )
      )
      submittedInfo shouldBe List(
        TransferDetails(
          id1,
          TransferStatus.Submitted,
          10.toShort,
          body1,
          Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
          None,
          None
        ),
        TransferDetails(
          id2,
          TransferStatus.Submitted,
          0.toShort,
          body2,
          Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
          None,
          None
        )
      )
      updatedInfo shouldBe List(
        TransferDetails(
          id1,
          TransferStatus.Succeeded,
          10.toShort,
          body1,
          Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
          Some(OffsetDateTime.ofInstant(updated, ZoneId.of("UTC"))),
          Some(json"{}")
        ),
        TransferDetails(
          id2,
          TransferStatus.Succeeded,
          0.toShort,
          body2,
          Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
          Some(OffsetDateTime.ofInstant(updated, ZoneId.of("UTC"))),
          Some(json"{}")
        )
      )
    }
  }

  it should "return an empty list if the limit is 0" in withRequest { (tx, controller) =>
    val (id1, _) = request1Transfers.head
    val submitted = Instant.now()

    for {
      _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Submitted: TransferStatus},
                 submitted_at = ${Timestamp.from(submitted)}
                 WHERE id = $id1""".update.run.void.transact(tx)
      theInfo <- controller.listTransfers(request1Id, 2, 0, sortDesc = false)
    } yield {
      theInfo shouldBe List()
    }
  }

  it should "return an empty list if the offset is beyond what exists in the table" in withRequest {
    (tx, controller) =>
      val (id1, _) = request1Transfers.head
      val submitted = Instant.now()

      for {
        _ <- sql"""UPDATE transfers SET
                 status = ${TransferStatus.Submitted: TransferStatus},
                 submitted_at = ${Timestamp.from(submitted)}
                 WHERE id = $id1""".update.run.void.transact(tx)
        theInfo <- controller.listTransfers(request1Id, 100, 2, sortDesc = false)
      } yield {
        theInfo shouldBe List()
      }
  }

  it should "fail to get details for multiple transfers under a nonexistent request" in withController {
    val requestId = UUID.randomUUID()
    (_, controller) =>
      controller
        .listTransfers(requestId, 2, 2, sortDesc = true)
        .attempt
        .map(_.left.value shouldBe NotFound(requestId))
  }

  it should "only list transfers filtered to a specified status" in withRequest {
    (_, controller) =>
      val limit = 5
      for {
        info <- controller.listTransfers(
          request1Id,
          0,
          limit.toLong,
          sortDesc = false,
          Option(TransferStatus.Pending)
        )
        emptyInfo <- controller.listTransfers(
          request1Id,
          0,
          limit.toLong,
          sortDesc = false,
          Option(TransferStatus.Expanded)
        )
      } yield {
        info.length shouldBe List(request1Transfers.length, limit).min
        emptyInfo.isEmpty shouldBe true
      }
  }

  it should "update the priority for all transfers in a request" in withRequest {
    (tx, controller) =>
      for {
        _ <- controller.updateRequestPriority(request1Id, 2)
        changePriorities <- sql"""SELECT priority FROM transfers WHERE request_id = $request1Id"""
          .query[Short]
          .to[List]
          .transact(tx)
        unchangedPriorities <- sql"""SELECT priority FROM transfers WHERE request_id != $request1Id"""
          .query[Short]
          .to[List]
          .transact(tx)
      } yield {
        changePriorities.foreach { _ shouldBe 2 }
        unchangedPriorities.foreach { _ shouldBe 0 }
      }
  }

  it should "fail to update the priority for transfers if the request ID does not exist" in withController {
    val requestId = UUID.randomUUID()
    (_, controller) =>
      controller
        .updateRequestPriority(requestId, 2)
        .attempt
        .map(_.left.value shouldBe NotFound(requestId))
  }

  it should "fail to update the priority for transfers in a request if the transfers are no longer pending" in withRequest {
    (tx, controller) =>
      for {
        _ <- sql"""UPDATE transfers
                SET status = ${TransferStatus.Submitted: TransferStatus}
                WHERE request_id = $request1Id""".update.run.void
          .transact(tx)
      } yield {
        controller
          .updateRequestPriority(request1Id, 2)
          .attempt
          .map(_.left.value shouldBe Conflict(request1Id))
      }
  }

  it should "return 0 rows updated if the priority updater no-ops on transfers under a request" in withRequest {
    (tx, controller) =>
      for {
        _ <- sql"""UPDATE transfers SET priority = 5 WHERE request_id = $request1Id""".update.run.void
          .transact(tx)
        myOutput <- controller.updateRequestPriority(request1Id, 5)
      } yield {
        myOutput.updatedCount shouldBe 0
      }
  }

  it should "update the priority for a specific transfer" in withRequest {
    (tx, controller) =>
      val (tId, _) = request1Transfers.head
      for {
        _ <- controller.updateTransferPriority(request1Id, tId, 2)
        changedPriorities <- sql"""SELECT priority FROM transfers WHERE request_id = $request1Id AND id = $tId"""
          .query[Short]
          .to[List]
          .transact(tx)
        unchangedPriorities <- sql"""SELECT priority FROM transfers WHERE id != $tId"""
          .query[Short]
          .to[List]
          .transact(tx)
      } yield {
        changedPriorities.foreach { _ shouldBe 2 }
        unchangedPriorities.foreach { _ shouldBe 0 }
      }
  }

  it should "fail to update the priority for a transfer if the transfer ID does not exist" in withController {
    val transferId = UUID.randomUUID()
    (_, controller) =>
      controller
        .updateTransferPriority(request1Id, transferId, 2)
        .attempt
        .map(_.left.value shouldBe NotFound(request1Id, Some(transferId)))
  }

  it should "fail to update the priority for a specific transfer if the transfer is no longer pending" in withRequest {
    val (tId, _) = request1Transfers.head
    (tx, controller) =>
      for {
        _ <- sql"""UPDATE transfers
                SET status = ${TransferStatus.Submitted: TransferStatus}
                WHERE request_id = $request1Id AND id = $tId""".update.run.void
          .transact(tx)
      } yield {
        controller
          .updateTransferPriority(request1Id, tId, 2)
          .attempt
          .map(_.left.value shouldBe Conflict(request1Id, Some(tId)))
      }
  }

  it should "return 0 rows updated if the priority updater no-ops on a specific transfer" in withRequest {
    val (tId, _) = request1Transfers.head
    (tx, controller) =>
      for {
        _ <- sql"""UPDATE transfers SET priority = 5 WHERE id = $tId""".update.run.void
          .transact(tx)
        myOutput <- controller.updateTransferPriority(request1Id, tId, 5)
      } yield {
        myOutput.updatedCount shouldBe 0
      }
  }
}
