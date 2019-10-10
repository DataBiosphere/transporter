package org.broadinstitute.transporter.transfer

import java.sql.Timestamp
import java.time.{Instant, OffsetDateTime}
import java.util.UUID

import cats.effect.{IO, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.kafka.KafkaProducer
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class TransferSubmitterSpec extends PostgresSpec with MockFactory with EitherValues {

  import org.broadinstitute.transporter.db.DoobieInstances._

  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val kafka = mock[KafkaProducer[Json]]

  private val request1Id = UUID.randomUUID()
  private val request1Transfers = List.tabulate(10) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val request2Id = UUID.randomUUID()
  private val request2Transfers = List.tabulate(20) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val theTopic = "request-topic"
  private val parallelism = 3
  private val interval = 500.millis

  def withRequest(test: (Transactor[IO], TransferSubmitter) => IO[Any]): Unit = {
    val tx = transactor
    val submitter =
      new TransferSubmitter(theTopic, parallelism.toLong, interval, tx, kafka)

    val setup = for {
      _ <- List(request1Id, request2Id).zipWithIndex.traverse_ {
        case (id, i) =>
          val ts = Timestamp.from(Instant.ofEpochMilli((i + 1).toLong))
          sql"INSERT INTO transfer_requests (id, received_at) VALUES ($id, $ts)".update.run.void
      }
      _ <- request1Transfers.traverse_ {
        case (id, body) =>
          sql"""INSERT INTO transfers
                  (id, request_id, body, status, steps_run, priority)
                  VALUES
                  ($id, $request1Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 1)""".update.run.void
      }
      _ <- request2Transfers.traverse_ {
        case (id, body) =>
          sql"""INSERT INTO transfers
                  (id, request_id, body, status, steps_run, priority)
                  VALUES
                  ($id, $request2Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 0)""".update.run.void
      }

    } yield ()

    setup.transact(tx).flatMap(_ => test(tx, submitter)).unsafeRunSync()
    ()
  }

  behavior of "TransferSubmitter"

  it should "submit batches of eligible transfers to Kafka" in withRequest {
    (tx, submitter) =>
      (kafka.submit _)
        .expects(where { (topic: String, submitted: List[TransferMessage[Json]]) =>
          topic == theTopic &&
          submitted.length == parallelism &&
          submitted.forall { message =>
            message.ids.request == request1Id &&
            request1Transfers.contains(message.ids.transfer -> message.message)
          }
        })
        .returning(IO.unit)
      (kafka.submit _).expects(theTopic, Nil).returning(IO.unit)

      for {
        _ <- submitter.submitEligibleTransfers
        submitted <- sql"""SELECT id, submitted_at
                           FROM transfers
                           WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[(UUID, Option[OffsetDateTime])]
          .to[List]
          .transact(tx)
        _ <- submitter.submitEligibleTransfers
        totalSubmitted <- sql"""SELECT COUNT(*) FROM transfers
                                WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        submitted.length shouldBe parallelism
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
      (kafka.submit _).expects(theTopic, *).returning(IO.raiseError(err))

      for {
        attempt <- controller.submitEligibleTransfers.attempt
        totalSubmitted <- sql"""SELECT count(*) FROM transfers
                                WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
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
        .expects(where { (topic: String, submitted: List[TransferMessage[Json]]) =>
          topic == theTopic && submitted.length == parallelism
        })
        .returning(IO.unit)
      (kafka.submit _).expects(theTopic, Nil).twice().returning(IO.unit)

      for {
        _ <- List.fill(3)(controller.submitEligibleTransfers).parSequence_
        submitted <- sql"""SELECT id, submitted_at
                           FROM transfers
                           WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[(UUID, Option[OffsetDateTime])]
          .to[List]
          .transact(tx)
        totalSubmitted <- sql"""SELECT COUNT(*) FROM transfers
                                WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        submitted.length shouldBe parallelism
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
      val preSubmitted = request1Transfers.map(_._1).take(parallelism * 2)

      (kafka.submit _).expects(theTopic, Nil).returning(IO.unit)

      for {
        _ <- preSubmitted.traverse_ { id =>
          sql"""UPDATE transfers
                SET status = ${TransferStatus.Submitted: TransferStatus}
                WHERE id = $id""".update.run.void
        }.transact(tx)
        _ <- controller.submitEligibleTransfers
        totalSubmitted <- sql"""SELECT COUNT(*) FROM transfers
                                WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        totalSubmitted shouldBe preSubmitted.length
      }
  }

  it should "count in-progress transfers when sweeping for submissions" in withRequest {
    (tx, controller) =>
      val inProgress = request1Transfers.map(_._1).take(parallelism)

      (kafka.submit _).expects(theTopic, Nil).returning(IO.unit)

      for {
        _ <- inProgress.traverse_ { id =>
          sql"""UPDATE transfers
              SET status = ${TransferStatus.InProgress: TransferStatus}
              WHERE id = $id""".update.run.void
        }.transact(tx)
        _ <- controller.submitEligibleTransfers
        totalSubmitted <- sql"""SELECT COUNT(*) FROM transfers
                             WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        totalSubmitted shouldBe 0
      }
  }

  it should "order transfers by priority" in withRequest { (tx, submitter) =>
    val (tId1, _) = request1Transfers(4)
    val (tId2, _) = request1Transfers(6)
    val (tId3, _) = request1Transfers(8)
    (kafka.submit _)
      .expects(where { (topic: String, submitted: List[TransferMessage[Json]]) =>
        topic == theTopic &&
        submitted.length == parallelism &&
        submitted.last.ids.transfer == tId1 &&
        submitted(submitted.length - 2).ids.transfer == tId2 &&
        submitted(submitted.length - 3).ids.transfer == tId3
      })
      .returning(IO.unit)
    (kafka.submit _).expects(theTopic, Nil).returning(IO.unit)

    for {
      _ <- sql"""UPDATE transfers SET priority = 3 WHERE id = $tId3""".update.run.void
        .transact(tx)
      _ <- sql"""UPDATE transfers SET priority = 4 WHERE id = $tId2""".update.run.void
        .transact(tx)
      _ <- sql"""UPDATE transfers SET priority = 5 WHERE id = $tId1""".update.run.void
        .transact(tx)
      _ <- submitter.submitEligibleTransfers
      submitted <- sql"""SELECT id, submitted_at
                           FROM transfers
                           WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
        .query[(UUID, Option[OffsetDateTime])]
        .to[List]
        .transact(tx)
      _ <- submitter.submitEligibleTransfers
      totalSubmitted <- sql"""SELECT COUNT(*) FROM transfers
                                WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
        .query[Long]
        .unique
        .transact(tx)
    } yield {
      submitted.length shouldBe parallelism
      submitted.foreach {
        case (_, submittedAt) =>
          submittedAt.isDefined shouldBe true
      }
      totalSubmitted shouldBe submitted.length
    }
  }

  it should "submit batches of eligible transfers to Kafka as ordered by priority" in withRequest {
    val (tId0, _) = request1Transfers(9)
    val (tId1, _) = request1Transfers(6)
    val (tId2, _) = request1Transfers(8)
    val (tId3, _) = request1Transfers(7)
    val (tId4, _) = request1Transfers(5)
    val myTransfers = List(tId0, tId1, tId2, tId3, tId4)
    (tx, submitter) =>
      (kafka.submit _)
        .expects(where { (topic: String, submitted: List[TransferMessage[Json]]) =>
          topic == theTopic &&
          submitted.length == parallelism &&
          submitted.forall { message =>
            message.ids.request == request1Id &&
            request1Transfers.contains(message.ids.transfer -> message.message)
          }
        })
        .returning(IO.unit)
      (kafka.submit _).expects(theTopic, Nil).returning(IO.unit)

      for {
        _ <- sql"""UPDATE transfers SET priority = 5 WHERE id in ($tId0, $tId2, $tId3, $tId4)""".update.run.void
          .transact(tx)
        _ <- submitter.submitEligibleTransfers
        submitted <- sql"""SELECT id, submitted_at
                           FROM transfers
                           WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[(UUID, Option[OffsetDateTime])]
          .to[List]
          .transact(tx)
        _ <- submitter.submitEligibleTransfers
        totalSubmitted <- sql"""SELECT count(*) FROM transfers
                                WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        submitted.length shouldBe parallelism
        submitted.foreach {
          case (id, submittedAt) =>
            myTransfers should contain(id)
            submittedAt.isDefined shouldBe true
        }
        submitted.foreach {
          _._1 should not be tId1
        }
        totalSubmitted shouldBe submitted.length
      }
  }

  it should "submit earlier eligible transfers of the same priority first" in withRequest {
    val request3Id = UUID.randomUUID()
    val ts = Timestamp.from(Instant.ofEpochMilli(0.toLong))
    val request3Transfers = List.tabulate(3) { i =>
      UUID.randomUUID() -> json"""{ "i": $i }"""
    }

    (tx, submitter) =>
      (kafka.submit _)
        .expects(where { (topic: String, submitted: List[TransferMessage[Json]]) =>
          topic == theTopic &&
          submitted.length == parallelism &&
          submitted.forall { message =>
            message.ids.request == request3Id &&
            request3Transfers.contains(message.ids.transfer -> message.message)
          }
        })
        .returning(IO.unit)
      for {
        _ <- sql"INSERT INTO transfer_requests (id, received_at) VALUES ($request3Id, $ts)".update.run.void
          .transact(tx)
        _ <- request3Transfers.traverse_ {
          case (id, body) =>
            sql"""INSERT INTO transfers
                  (id, request_id, body, status, steps_run, priority)
                  VALUES
                  ($id, $request3Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 1)""".update.run.void
              .transact(tx)
        }
        _ <- submitter.submitEligibleTransfers
        submitted <- sql"""SELECT count(*) FROM transfers
                                WHERE status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[Long]
          .unique
          .transact(tx)
      } yield {
        submitted shouldBe parallelism
      }
  }

}
