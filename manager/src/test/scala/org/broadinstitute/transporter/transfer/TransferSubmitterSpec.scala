package org.broadinstitute.transporter.transfer

import java.time.OffsetDateTime
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
      _ <- List(request1Id, request2Id).traverse_ { id =>
        sql"insert into transfer_requests (id) values ($id)".update.run.void
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
        submitted <- sql"""select id, submitted_at
                           from transfers
                           where status = ${TransferStatus.Submitted: TransferStatus}"""
          .query[(UUID, Option[OffsetDateTime])]
          .to[List]
          .transact(tx)
        _ <- submitter.submitEligibleTransfers
        totalSubmitted <- sql"""select count(*) from transfers
                                where status = ${TransferStatus.Submitted: TransferStatus}"""
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
        .expects(where { (topic: String, submitted: List[TransferMessage[Json]]) =>
          topic == theTopic && submitted.length == parallelism
        })
        .returning(IO.unit)
      (kafka.submit _).expects(theTopic, Nil).twice().returning(IO.unit)

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
      val inProgress = request1Transfers.map(_._1).take(parallelism)

      (kafka.submit _).expects(theTopic, Nil).returning(IO.unit)

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

}
