package org.broadinstitute.transporter.transfer

import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime, ZoneId}
import java.util.UUID

import cats.effect.{IO, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.error._
import org.broadinstitute.transporter.kafka.KafkaProducer
import org.broadinstitute.transporter.transfer.api._
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext

class TransferControllerSpec extends PostgresSpec with MockFactory with EitherValues {
  import org.broadinstitute.transporter.db.DoobieInstances._

  private val queueId = UUID.randomUUID()
  private val queueName = "the-queue"
  private val queueSchema = json"""{ "type": "object" }"""
  private val queueConcurrency = 3

  private val request1Id = UUID.randomUUID()
  private val request1Transfers = List.tabulate(10) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val request2Id = UUID.randomUUID()
  private val request2Transfers = List.tabulate(20) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val kafka = mock[KafkaProducer[Json]]

  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  def withController(test: (Transactor[IO], TransferController) => IO[Any]): Unit = {
    val tx = transactor
    test(tx, new TransferController(tx, kafka)).unsafeRunSync()
    ()
  }

  def withQueue(test: (Transactor[IO], TransferController) => IO[Any]): Unit =
    withController { (tx, controller) =>
      sql"""insert into queues
          (id, name, request_topic, progress_topic, response_topic, request_schema, max_in_flight, partition_count)
          values
          ($queueId, $queueName, 'requests', 'progress', 'responses', $queueSchema, $queueConcurrency, 1)""".update.run
        .transact(tx)
        .void
        .flatMap(_ => test(tx, controller))
    }

  def withRequest(test: (Transactor[IO], TransferController) => IO[Any]): Unit =
    withQueue { (tx, controller) =>
      val setup = for {
        _ <- List(request1Id, request2Id).traverse_ { id =>
          sql"insert into transfer_requests (id, queue_id) values ($id, $queueId)".update.run.void
        }
        _ <- request1Transfers.traverse_ {
          case (id, body) =>
            sql"""insert into transfers
                  (id, request_id, body, status)
                  values
                  ($id, $request1Id, $body, ${TransferStatus.Pending: TransferStatus})""".update.run.void
        }
        _ <- request2Transfers.traverse_ {
          case (id, body) =>
            sql"""insert into transfers
                  (id, request_id, body, status)
                  values
                  ($id, $request2Id, $body, ${TransferStatus.Pending: TransferStatus})""".update.run.void
        }
      } yield ()

      setup.transact(tx).flatMap(_ => test(tx, controller))
    }

  private val countRequests = fr"select count(*) from transfer_requests"
  private val countTransfers = fr"select count(*) from transfers"

  behavior of "TransferController"

  it should "record new transfers with well-formed requests" in withQueue {
    (tx, controller) =>
      val transferCount = 10
      val request = BulkRequest(List.tabulate(transferCount)(i => json"""{ "i": $i }"""))

      for {
        (initRequests, initTransfers) <- (
          countRequests.query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
        ack <- controller.recordTransfer(queueName, request)
        (postRequests, postTransfers) <- (
          (countRequests ++ fr"where id = ${ack.id} and queue_id = $queueId")
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

  it should "not record new transfers with malformed requests" in withQueue {
    (tx, controller) =>
      val transferCount = 10
      val request = BulkRequest(List.tabulate(transferCount)(i => json"""[$i]"""))

      for {
        (initRequests, initTransfers) <- (
          countRequests.query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
        ackOrErr <- controller.recordTransfer(queueName, request).attempt
        (postRequests, postTransfers) <- (
          (countRequests ++ fr"where queue_id = $queueId").query[Long].unique,
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

  it should "not record new transfers under nonexistent queues" in withController {
    (tx, controller) =>
      val transferCount = 10
      val request = BulkRequest(List.tabulate(transferCount)(i => json"""[$i]"""))

      for {
        (initRequests, initTransfers) <- (
          countRequests.query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
        ackOrErr <- controller.recordTransfer(queueName, request).attempt
        (postRequests, postTransfers) <- (
          (countRequests ++ fr"where queue_id = $queueId").query[Long].unique,
          countTransfers.query[Long].unique
        ).tupled.transact(tx)
      } yield {
        initRequests shouldBe 0
        initTransfers shouldBe 0
        ackOrErr.left.value shouldBe NoSuchQueue(queueName)
        postRequests shouldBe 0
        postTransfers shouldBe 0
      }
  }

  it should "get summaries for registered transfers" in withRequest { (_, controller) =>
    for {
      summary1 <- controller.lookupRequestStatus(queueName, request1Id)
      summary2 <- controller.lookupRequestStatus(queueName, request2Id)
    } yield {
      summary1 shouldBe RequestStatus(
        request1Id,
        TransferStatus.Pending,
        TransferStatus.values.map(_ -> 0L).toMap ++ Map(
          TransferStatus.Pending -> request1Transfers.length.toLong
        ),
        None,
        None
      )
      summary2 shouldBe RequestStatus(
        request2Id,
        TransferStatus.Pending,
        TransferStatus.values.map(_ -> 0L).toMap ++ Map(
          TransferStatus.Pending -> request2Transfers.length.toLong
        ),
        None,
        None
      )
    }
  }

  it should "prioritize 'inprogress' status over all in request summaries" in withRequest {
    (tx, controller) =>
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
        summary <- controller.lookupRequestStatus(queueName, request1Id)
      } yield {
        summary shouldBe RequestStatus(
          request1Id,
          TransferStatus.InProgress,
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

  it should "prioritize 'submitted' status over 'pending' in request summaries" in withRequest {
    (tx, controller) =>
      val now = Instant.now
      val fakeSubmitted = request1Transfers.take(3).map(_._1)
      val fakeSucceeded = request1Transfers.takeRight(2).map(_._1)
      val fakeFailed = request1Transfers.slice(3, 6).map(_._1)

      val transaction = for {
        _ <- fakeSubmitted.traverse_ { id =>
          sql"""update transfers set
                status = ${TransferStatus.Submitted: TransferStatus},
                submitted_at = ${Timestamp.from(now)}
                where id = $id""".update.run
        }
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
        summary <- controller.lookupRequestStatus(queueName, request1Id)
      } yield {
        summary shouldBe RequestStatus(
          request1Id,
          TransferStatus.Submitted,
          Map(
            TransferStatus.Pending -> 2,
            TransferStatus.Submitted -> 3,
            TransferStatus.Failed -> 3,
            TransferStatus.Succeeded -> 2,
            TransferStatus.InProgress -> 0
          ),
          Some(OffsetDateTime.ofInstant(now.minusMillis(30000), ZoneId.of("UTC"))),
          Some(OffsetDateTime.ofInstant(now, ZoneId.of("UTC")))
        )
      }
  }

  it should "prioritize 'pending' status over terminal statuses in request summaries" in withRequest {
    (tx, controller) =>
      val now = Instant.now
      val fakeSucceeded = request1Transfers.takeRight(4).map(_._1)
      val fakeFailed = request1Transfers.take(4).map(_._1)

      val transaction = for {
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
        summary <- controller.lookupRequestStatus(queueName, request1Id)
      } yield {
        summary shouldBe RequestStatus(
          request1Id,
          TransferStatus.Pending,
          Map(
            TransferStatus.Pending -> 2,
            TransferStatus.Submitted -> 0,
            TransferStatus.Succeeded -> 4,
            TransferStatus.Failed -> 4,
            TransferStatus.InProgress -> 0
          ),
          Some(OffsetDateTime.ofInstant(now.minusMillis(30000), ZoneId.of("UTC"))),
          Some(OffsetDateTime.ofInstant(now, ZoneId.of("UTC")))
        )
      }
  }

  it should "prioritize failures over successes in request summaries" in withRequest {
    (tx, controller) =>
      val now = Instant.now
      val fakeSucceeded = request1Transfers.take(8).map(_._1)
      val fakeFailed = request1Transfers.takeRight(2).map(_._1)

      val transaction = for {
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
        summary <- controller.lookupRequestStatus(queueName, request1Id)
      } yield {
        summary shouldBe RequestStatus(
          request1Id,
          TransferStatus.Failed,
          Map(
            TransferStatus.Pending -> 0,
            TransferStatus.Submitted -> 0,
            TransferStatus.Succeeded -> 8,
            TransferStatus.Failed -> 2,
            TransferStatus.InProgress -> 0
          ),
          Some(OffsetDateTime.ofInstant(now.minusMillis(30000), ZoneId.of("UTC"))),
          Some(OffsetDateTime.ofInstant(now, ZoneId.of("UTC")))
        )
      }
  }

  it should "fail to get summaries for requests under nonexistent queues" in withController {
    (_, controller) =>
      controller
        .lookupRequestStatus(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchQueue(queueName))
  }

  it should "fail to get summaries for nonexistent requests" in withQueue {
    (_, controller) =>
      controller
        .lookupRequestStatus(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchRequest(queueName, request1Id))
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
        outputs <- controller.lookupRequestOutputs(queueName, request1Id)
      } yield {
        outputs.id shouldBe request1Id
        outputs.info should contain theSameElementsAs toUpdate.map {
          case (id, info) => TransferInfo(id, info)
        }
      }
  }

  it should "fail to get outputs for requests under nonexistent queues" in withController {
    (_, controller) =>
      controller
        .lookupRequestOutputs(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchQueue(queueName))
  }

  it should "fail to get outputs for nonexistent requests" in withQueue {
    (_, controller) =>
      controller
        .lookupRequestOutputs(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchRequest(queueName, request1Id))
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
        outputs <- controller.lookupRequestFailures(queueName, request1Id)
      } yield {
        outputs.id shouldBe request1Id
        outputs.info should contain theSameElementsAs toUpdate.map {
          case (id, info) => TransferInfo(id, info)
        }
      }
  }

  it should "fail to get failures for requests under nonexistent queues" in withController {
    (_, controller) =>
      controller
        .lookupRequestFailures(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchQueue(queueName))
  }

  it should "fail to get failures for nonexistent requests" in withQueue {
    (_, controller) =>
      controller
        .lookupRequestFailures(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchRequest(queueName, request1Id))
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
        ack <- controller.reconsiderRequest(queueName, request1Id)
        n <- sql"select count(*) from transfers where status = ${TransferStatus.Failed: TransferStatus}"
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        ack shouldBe RequestAck(request1Id, failed.length)
        n shouldBe 0
      }
  }

  it should "not reconsider failed transfers under a nonexistent queue" in withController {
    (_, controller) =>
      controller
        .reconsiderRequest(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchQueue(queueName))
  }

  it should "not reconsider failed transfers in a nonexistent request" in withQueue {
    (_, controller) =>
      controller
        .lookupRequestFailures(queueName, request1Id)
        .attempt
        .map(_.left.value shouldBe NoSuchRequest(queueName, request1Id))
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
        ack <- controller.reconsiderRequest(queueName, request2Id)
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
      initInfo <- controller.lookupTransferDetails(queueName, request1Id, id)
      _ <- sql"""update transfers set
                 status = ${TransferStatus.Submitted: TransferStatus},
                 submitted_at = ${Timestamp.from(submitted)}
                 where id = $id""".update.run.void.transact(tx)
      submittedInfo <- controller.lookupTransferDetails(queueName, request1Id, id)
      _ <- sql"""update transfers set
                 status = ${TransferStatus.Succeeded: TransferStatus},
                 updated_at = ${Timestamp.from(updated)},
                 info = '{}'
                 where id = $id""".update.run.void.transact(tx)
      updatedInfo <- controller.lookupTransferDetails(queueName, request1Id, id)
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

  it should "fail to get details under a nonexistent queue" in withController {
    (_, controller) =>
      controller
        .lookupTransferDetails(queueName, request1Id, UUID.randomUUID())
        .attempt
        .map(_.left.value shouldBe NoSuchQueue(queueName))
  }

  it should "fail to get details under a nonexistent request" in withQueue {
    (_, controller) =>
      controller
        .lookupTransferDetails(queueName, request1Id, UUID.randomUUID())
        .attempt
        .map(_.left.value shouldBe NoSuchRequest(queueName, request1Id))
  }

  it should "fail to get details for a nonexistent transfer" in withRequest {
    (_, controller) =>
      controller
        .lookupTransferDetails(queueName, request1Id, request2Transfers.head._1)
        .attempt
        .map(
          _.left.value shouldBe NoSuchTransfer(
            queueName,
            request1Id,
            request2Transfers.head._1
          )
        )
  }

  it should "submit batches of eligible transfers to Kafka" in withRequest {
    (tx, controller) =>
      (kafka.submit _)
        .expects(where { list: List[(String, List[(TransferIds, Json)])] =>
          list.length == 1 && {
            val (queueTopic, queueSubmitted) = list.head

            queueTopic == "requests" &&
            queueSubmitted.length == queueConcurrency &&
            queueSubmitted.forall {
              case (TransferIds(queue, request, transfer), body) =>
                queue == queueId &&
                  request == request1Id &&
                  request1Transfers.contains(transfer -> body)
            }
          }
        })
        .returning(IO.unit)
      (kafka.submit _).expects(Nil).returning(IO.unit)

      for {
        _ <- controller.submitEligibleTransfers
        submitted <- sql"""select id, submitted_at
                           from transfers
                           where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[(UUID, Option[OffsetDateTime])]
          .to[List]
          .transact(tx)
        _ <- controller.submitEligibleTransfers
        totalSubmitted <- sql"""select count(*) from transfers
                                where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        submitted.length shouldBe queueConcurrency
        submitted.foreach {
          case (id, submittedAt) =>
            request1Transfers.map(_._1) should contain(id)
            submittedAt.isDefined shouldBe true
        }
        totalSubmitted shouldBe submitted.length
      }
  }

  it should "not mark transfers as submitted if producing to Kafka fails" in withRequest {
    (tx, controller) =>
      val err = new RuntimeException("BOOM")
      (kafka.submit _).expects(*).returning(IO.raiseError(err))

      for {
        attempt <- controller.submitEligibleTransfers.attempt
        totalSubmitted <- sql"""select count(*) from transfers
                                where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        attempt.left.value shouldBe err
        totalSubmitted shouldBe 0
      }
  }

  it should "not double-submit transfers on concurrent access" in withRequest {
    (tx, controller) =>
      (kafka.submit _)
        .expects(where { list: List[(String, List[(TransferIds, Json)])] =>
          list.length == 1 && list.head._2.length == queueConcurrency
        })
        .returning(IO.unit)
      (kafka.submit _).expects(Nil).twice().returning(IO.unit)

      for {
        _ <- List.fill(3)(controller.submitEligibleTransfers).parSequence_
        submitted <- sql"""select id, submitted_at
                           from transfers
                           where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[(UUID, Option[OffsetDateTime])]
          .to[List]
          .transact(tx)
        totalSubmitted <- sql"""select count(*) from transfers
                                where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        submitted.length shouldBe queueConcurrency
        submitted.foreach {
          case (id, submittedAt) =>
            request1Transfers.map(_._1) should contain(id)
            submittedAt.isDefined shouldBe true
        }
        totalSubmitted shouldBe submitted.length
      }
  }

  it should "not crash if more than max concurrent transfers end up being submitted" in withRequest {
    (tx, controller) =>
      val preSubmitted = request1Transfers.map(_._1).take(queueConcurrency * 2)

      (kafka.submit _).expects(Nil).returning(IO.unit)

      for {
        _ <- preSubmitted.traverse_ { id =>
          sql"""update transfers
                set status = ${TransferStatus.Submitted: TransferStatus}
                where id = $id""".update.run.void
        }.transact(tx)
        _ <- controller.submitEligibleTransfers
        totalSubmitted <- sql"""select count(*) from transfers
                                where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        totalSubmitted shouldBe preSubmitted.length
      }
  }

  it should "count in-progress transfers when sweeping for submissions" in withRequest {
    (tx, controller) =>
      val inProgress = request1Transfers.map(_._1).take(queueConcurrency)

      (kafka.submit _).expects(Nil).returning(IO.unit)

      for {
        _ <- inProgress.traverse_ { id =>
          sql"""update transfers
              set status = ${TransferStatus.InProgress: TransferStatus}
              where id = $id""".update.run.void
        }.transact(tx)
        _ <- controller.submitEligibleTransfers
        totalSubmitted <- sql"""select count(*) from transfers
                             where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        totalSubmitted shouldBe 0
      }
  }

  it should "record transfer results" in withRequest { (tx, controller) =>
    val updates = request1Transfers.zipWithIndex.collect {
      case ((id, _), i) if i % 3 != 0 =>
        val result =
          if (i % 3 == 1) TransferResult.Success else TransferResult.FatalFailure
        (TransferIds(queueId, request1Id, id), result -> json"""{ "i+1": $i }""")
    }

    for {
      _ <- controller.recordTransferResults(updates)
      updated <- sql"select id, status, info from transfers where updated_at is not null"
        .query[(UUID, TransferStatus, Json)]
        .to[List]
        .transact(tx)
    } yield {
      updated should contain theSameElementsAs updates.map {
        case (ids, (res, info)) =>
          (
            ids.transfer,
            res match {
              case TransferResult.Success      => TransferStatus.Succeeded
              case TransferResult.FatalFailure => TransferStatus.Failed
            },
            info
          )
      }
    }
  }

  it should "not update results if IDs are mismatched" in withRequest {
    (tx, controller) =>
      val updates = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 != 0 =>
          val result =
            if (i % 3 == 1) TransferResult.Success else TransferResult.FatalFailure
          (TransferIds(queueId, request2Id, id), result -> json"""{ "i+1": $i }""")
      }

      for {
        _ <- controller.recordTransferResults(updates)
        updated <- sql"select id, status, info from transfers where updated_at is not null"
          .query[(UUID, TransferStatus, Json)]
          .to[List]
          .transact(tx)
      } yield {
        updated shouldBe empty
      }
  }

  it should "mark submitted transfers as in progress" in withRequest { (tx, controller) =>
    val updates = request1Transfers.zipWithIndex.collect {
      case ((id, _), i) if i % 3 != 0 =>
        (TransferIds(queueId, request1Id, id), json"""{ "i+1": $i }""")
    }

    for {
      _ <- updates.traverse_ {
        case (ids, _) =>
          sql"update transfers set status = 'submitted' where id = ${ids.transfer}".update.run.void
            .transact(tx)
      }
      _ <- controller.markTransfersInProgress(updates)
      updated <- sql"select id, status, info from transfers where updated_at is not null"
        .query[(UUID, TransferStatus, Json)]
        .to[List]
        .transact(tx)
    } yield {
      updated should contain theSameElementsAs updates.map {
        case (ids, info) => (ids.transfer, TransferStatus.InProgress, info)
      }
    }
  }

  it should "keep the 'updated_at' and 'info' fields of in-progress transfers up-to-date" in withRequest {
    (tx, controller) =>
      val updates = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 != 0 =>
          (TransferIds(queueId, request1Id, id), json"""{ "i+1": $i }""")
      }

      for {
        _ <- updates.traverse_ {
          case (ids, _) =>
            sql"""update transfers set
                  status = 'inprogress',
                  updated_at = TO_TIMESTAMP(0),
                  info = '{}'
                  where id = ${ids.transfer}""".update.run.void
              .transact(tx)
        }
        _ <- controller.markTransfersInProgress(updates)
        updated <- sql"select id, status, info from transfers where updated_at > TO_TIMESTAMP(0)"
          .query[(UUID, TransferStatus, Json)]
          .to[List]
          .transact(tx)
      } yield {
        updated should contain theSameElementsAs updates.map {
          case (ids, info) => (ids.transfer, TransferStatus.InProgress, info)
        }
      }
  }

  it should "not mark transfers in a terminal state to in progress" in withRequest {
    (tx, controller) =>
      val updates = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 != 0 =>
          (TransferIds(queueId, request1Id, id), json"""{ "i+1": $i }""")
      }

      for {
        _ <- updates.traverse_ {
          case (ids, _) =>
            sql"update transfers set status = 'succeeded' where id = ${ids.transfer}".update.run.void
              .transact(tx)
        }
        _ <- controller.markTransfersInProgress(updates)
        updated <- sql"select id, status, info from transfers where updated_at is not null"
          .query[(UUID, TransferStatus, Json)]
          .to[List]
          .transact(tx)
      } yield {
        updated shouldBe empty
      }
  }

  it should "not mark transfers as in progress if IDs are mismatched" in withRequest {
    (tx, controller) =>
      val updates = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 != 0 =>
          (TransferIds(queueId, request2Id, id), json"""{ "i+1": $i }""")
      }

      for {
        _ <- updates.traverse_ {
          case (ids, _) =>
            sql"update transfers set status = 'submitted' where id = ${ids.transfer}".update.run.void
              .transact(tx)
        }
        _ <- controller.markTransfersInProgress(updates)
        updated <- sql"select id, status, info from transfers where updated_at is not null"
          .query[(UUID, TransferStatus, Json)]
          .to[List]
          .transact(tx)
      } yield {
        updated shouldBe empty
      }
  }
}
