package org.broadinstitute.transporter.transfer

import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.UUID

import cats.effect.IO
import cats.implicits._
import doobie._
import doobie.implicits._
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.error.ApiError._
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.config.TransferSchema
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues

class TransferControllerSpec extends PostgresSpec with MockFactory with EitherValues {
  import org.broadinstitute.transporter.db.DoobieInstances._

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
        _ <- List(request1Id, request2Id).traverse_ { id =>
          sql"insert into transfer_requests (id) values ($id)".update.run.void
        }
        _ <- request1Transfers.traverse_ {
          case (id, body) =>
            sql"""insert into transfers
                  (id, request_id, body, status, steps_run)
                  values
                  ($id, $request1Id, $body, ${TransferStatus.Pending: TransferStatus}, 0)""".update.run.void
        }
        _ <- request2Transfers.traverse_ {
          case (id, body) =>
            sql"""insert into transfers
                  (id, request_id, body, status, steps_run)
                  values
                  ($id, $request2Id, $body, ${TransferStatus.Pending: TransferStatus}, 0)""".update.run.void
        }
      } yield ()

      setup.transact(tx).flatMap(_ => test(tx, controller))
    }

  private val countRequests = fr"select count(*) from transfer_requests"
  private val countTransfers = fr"select count(*) from transfers"

  behavior of "TransferController"

  it should "record new transfers with well-formed requests" in withController {
    (tx, controller) =>
      val transferCount = 10
      val request = BulkRequest(List.tabulate(transferCount)(i => json"""{ "i": $i }"""))

      for {
        (initRequests, initTransfers) <- (
          countRequests.query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
        ack <- controller.recordRequest(request)
        (postRequests, postTransfers) <- (
          (countRequests ++ fr"where id = ${ack.id}")
            .query[Long]
            .unique,
          (countTransfers ++ fr"where request_id = ${ack.id}").query[Long].unique
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
      val request = BulkRequest(List.tabulate(transferCount)(i => json"""[$i]"""))

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

  it should "get summaries for registered transfers" in withRequest { (tx, controller) =>
    val now = Instant.now
    val fakeSubmitted = request1Transfers.take(3).map(_._1)
    val fakeInProgress = request1Transfers(6)._1
    val fakeSucceeded = request1Transfers.takeRight(2).map(_._1)
    val fakeFailed = request1Transfers.slice(3, 6).map(_._1)

    val transaction = for {
      _ <- fakeSubmitted.traverse_ { id =>
        sql"""update transfers set
                status = ${TransferStatus.Submitted: TransferStatus},
                submitted_at = ${Timestamp.from(now)}
                where id = $id""".update.run
      }
      _ <- sql"""update transfers set
                   status = ${TransferStatus.InProgress: TransferStatus},
                   submitted_at = ${Timestamp.from(now)},
                   updated_at = ${Timestamp.from(now.plusMillis(10000))}
                   where id = $fakeInProgress""".update.run
      _ <- fakeSucceeded.traverse_ { id =>
        sql"""update transfers set
                status = ${TransferStatus.Succeeded: TransferStatus},
                submitted_at = ${Timestamp.from(now.minusMillis(30000))},
                updated_at = ${Timestamp.from(now)}
                where id = $id""".update.run
      }
      _ <- fakeFailed.traverse_ { id =>
        sql"""update transfers set
                status = ${TransferStatus.Failed: TransferStatus},
                submitted_at = ${Timestamp.from(now.plusMillis(30000))},
                updated_at = ${Timestamp.from(now.minusMillis(30000))}
                where id = $id""".update.run
      }
    } yield ()

    for {
      _ <- transaction.transact(tx)
      summary <- controller.lookupRequestStatus(request1Id)
    } yield {
      summary shouldBe RequestSummary(
        request1Id,
        Map(
          TransferStatus.Pending -> 1,
          TransferStatus.Submitted -> 3,
          TransferStatus.Failed -> 3,
          TransferStatus.Succeeded -> 2,
          TransferStatus.InProgress -> 1
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
          TransferStatus.values.map(_ -> 0L).toMap ++ Map(
            TransferStatus.Pending -> request1Transfers.length.toLong
          ),
          None,
          None
        )
        summary2 shouldBe RequestSummary(
          request2Id,
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

  it should "get outputs of successful transfers for a request" in withRequest {
    (tx, controller) =>
      val infos = List.tabulate(5)(i => json"""{ "i+1": ${i + 1} }""")
      val toUpdate = request1Transfers.map(_._1).take(5).zip(infos)

      val transaction = for {
        _ <- toUpdate.traverse_ {
          case (id, info) =>
            sql"""update transfers set
                  info = $info,
                  status = ${TransferStatus.Succeeded: TransferStatus}
                  where id = $id""".update.run.void
        }
        _ <- request1Transfers.takeRight(5).traverse_ {
          case (id, _) =>
            sql"""update transfers set
                status = ${TransferStatus.Failed: TransferStatus},
                info = '[]'
                where id = $id""".update.run.void
        }
      } yield ()

      for {
        _ <- transaction.transact(tx)
        outputs <- controller.lookupRequestOutputs(request1Id)
      } yield {
        outputs.id shouldBe request1Id
        outputs.info should contain theSameElementsAs toUpdate.map {
          case (id, info) => TransferInfo(id, info)
        }
      }
  }

  it should "fail to get outputs for nonexistent requests" in withController {
    (_, controller) =>
      controller
        .lookupRequestOutputs(request1Id)
        .attempt
        .map(_.left.value shouldBe NotFound(request1Id))
  }

  it should "get outputs of failed transfers for a request" in withRequest {
    (tx, controller) =>
      val infos = List.tabulate(5)(i => json"""{ "i+1": ${i + 1} }""")
      val toUpdate = request1Transfers.map(_._1).take(5).zip(infos)

      val transaction = for {
        _ <- toUpdate.traverse_ {
          case (id, info) =>
            sql"""update transfers set
                  info = $info,
                  status = ${TransferStatus.Failed: TransferStatus}
                  where id = $id""".update.run.void
        }
        _ <- request1Transfers.takeRight(5).traverse_ {
          case (id, _) =>
            sql"""update transfers set
                status = ${TransferStatus.Succeeded: TransferStatus},
                info = '[]'
                where id = $id""".update.run.void
        }
      } yield ()

      for {
        _ <- transaction.transact(tx)
        outputs <- controller.lookupRequestFailures(request1Id)
      } yield {
        outputs.id shouldBe request1Id
        outputs.info should contain theSameElementsAs toUpdate.map {
          case (id, info) => TransferInfo(id, info)
        }
      }
  }

  it should "fail to get failures for nonexistent requests" in withController {
    (_, controller) =>
      controller
        .lookupRequestFailures(request1Id)
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
          sql"update transfers set status = ${TransferStatus.Failed: TransferStatus} where id = $id".update.run.void
        }.transact(tx)
        ack <- controller.reconsiderRequest(request1Id)
        n <- sql"select count(*) from transfers where status = ${TransferStatus.Failed: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        ack shouldBe RequestAck(request1Id, failed.length)
        n shouldBe 0
      }
  }

  it should "not reconsider failed transfers in a nonexistent request" in withController {
    (_, controller) =>
      controller
        .lookupRequestFailures(request1Id)
        .attempt
        .map(_.left.value shouldBe NotFound(request1Id))
  }

  it should "not reconsider failures in an unrelated request" in withRequest {
    (tx, controller) =>
      val failed = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 == 0 => id
      }

      for {
        _ <- failed.traverse_ { id =>
          sql"update transfers set status = ${TransferStatus.Failed: TransferStatus} where id = $id".update.run.void
        }.transact(tx)
        ack <- controller.reconsiderRequest(request2Id)
        n <- sql"select count(*) from transfers where status = ${TransferStatus.Failed: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        ack shouldBe RequestAck(request2Id, 0)
        n shouldBe failed.length
      }
  }

  it should "get details for a single transfer" in withRequest { (tx, controller) =>
    val (id, body) = request1Transfers.head
    val submitted = Instant.now()
    val updated = submitted.plusMillis(30000)

    for {
      initInfo <- controller.lookupTransferDetails(request1Id, id)
      _ <- sql"""update transfers set
                 status = ${TransferStatus.Submitted: TransferStatus},
                 submitted_at = ${Timestamp.from(submitted)}
                 where id = $id""".update.run.void.transact(tx)
      submittedInfo <- controller.lookupTransferDetails(request1Id, id)
      _ <- sql"""update transfers set
                 status = ${TransferStatus.Succeeded: TransferStatus},
                 updated_at = ${Timestamp.from(updated)},
                 info = '{}'
                 where id = $id""".update.run.void.transact(tx)
      updatedInfo <- controller.lookupTransferDetails(request1Id, id)
    } yield {
      initInfo shouldBe TransferDetails(
        id,
        TransferStatus.Pending,
        body,
        None,
        None,
        None
      )
      submittedInfo shouldBe TransferDetails(
        id,
        TransferStatus.Submitted,
        body,
        Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
        None,
        None
      )
      updatedInfo shouldBe TransferDetails(
        id,
        TransferStatus.Succeeded,
        body,
        Some(OffsetDateTime.ofInstant(submitted, ZoneId.of("UTC"))),
        Some(OffsetDateTime.ofInstant(updated, ZoneId.of("UTC"))),
        Some(json"{}")
      )
    }
  }

  it should "fail to get details under a nonexistent request" in withController {
    (_, controller) =>
      controller
        .lookupTransferDetails(request1Id, UUID.randomUUID())
        .attempt
        .map(_.left.value shouldBe NotFound(request1Id))
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
}
