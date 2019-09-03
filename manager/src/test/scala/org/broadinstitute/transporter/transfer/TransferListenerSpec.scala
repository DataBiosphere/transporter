package org.broadinstitute.transporter.transfer

import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

import cats.effect.{IO, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import fs2.Chunk
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.kafka.KafkaConsumer
import org.broadinstitute.transporter.transfer.config.TransferSchema
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext

class TransferListenerSpec extends PostgresSpec with MockFactory with EitherValues {
  import org.broadinstitute.transporter.db.DoobieInstances._

  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val progress = mock[KafkaConsumer[(Int, Json)]]
  private val results = mock[KafkaConsumer[(TransferResult, Json)]]

  private val requestSchema =
    json"""{ "type": "object" }""".as[TransferSchema].right.value

  private val request1Id = UUID.randomUUID()
  private val request1Transfers = List.tabulate(50) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val request2Id = UUID.randomUUID()
  private val request2Transfers = List.tabulate(20) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val request3Id = UUID.randomUUID()
  private val request3Transfers = List.tabulate(2) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  def withRequest(test: (Transactor[IO], TransferListener) => IO[Any]): Unit = {
    val tx = transactor
    val listener = new TransferListener(
      tx,
      progress,
      results,
      new TransferController(requestSchema, tx)
    )

    val setup = for {
      _ <- List(request1Id, request2Id, request3Id).zipWithIndex.traverse_ {
        case (id, i) =>
          val ts = Timestamp.from(Instant.ofEpochMilli(i.toLong))
          sql"insert into transfer_requests (id, received_at) values ($id, $ts)".update.run.void
      }
      _ <- request1Transfers.traverse_ {
        case (id, body) =>
          sql"""insert into transfers
                  (id, request_id, body, status, steps_run, priority)
                  values
                  ($id, $request1Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 0)""".update.run.void
      }
      _ <- request2Transfers.traverse_ {
        case (id, body) =>
          sql"""insert into transfers
                  (id, request_id, body, status, steps_run, priority)
                  values
                  ($id, $request2Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 0)""".update.run.void
      }
      _ <- request3Transfers.traverse_ {
        case (id, body) =>
          sql"""insert into transfers
                  (id, request_id, body, status, steps_run, priority)
                  values
                  ($id, $request3Id, $body, ${TransferStatus.Pending: TransferStatus}, 0, 2)""".update.run.void
      }
    } yield ()

    setup.transact(tx).flatMap(_ => test(tx, listener)).unsafeRunSync()
    ()
  }

  behavior of "TransferListener"

  it should "record transfer results" in withRequest { (tx, listener) =>
    val updates = request1Transfers.zipWithIndex.collect {
      case ((id, _), i) if i % 3 != 0 =>
        val result =
          if (i % 3 == 1) TransferResult.Success else TransferResult.FatalFailure
        TransferMessage(TransferIds(request1Id, id), result -> json"""{ "i+1": $i }""")
    }

    for {
      _ <- listener.recordTransferResults(Chunk.seq(updates))
      updated <- sql"select id, status, info from transfers where updated_at is not null"
        .query[(UUID, TransferStatus, Json)]
        .to[List]
        .transact(tx)
    } yield {
      updated should contain theSameElementsAs updates.map {
        case TransferMessage(ids, (res, info)) =>
          (
            ids.transfer,
            res match {
              case TransferResult.Success      => TransferStatus.Succeeded
              case TransferResult.FatalFailure => TransferStatus.Failed
              case TransferResult.Expanded     => TransferStatus.Expanded
            },
            info
          )
      }
    }
  }

  it should "not update results if IDs are mismatched" in withRequest { (tx, listener) =>
    val updates = request1Transfers.zipWithIndex.collect {
      case ((id, _), i) if i % 3 != 0 =>
        val result =
          if (i % 3 == 1) TransferResult.Success else TransferResult.FatalFailure
        TransferMessage(TransferIds(request2Id, id), result -> json"""{ "i+1": $i }""")
    }

    for {
      _ <- listener.recordTransferResults(Chunk.seq(updates))
      updated <- sql"select id, status, info from transfers where updated_at is not null"
        .query[(UUID, TransferStatus, Json)]
        .to[List]
        .transact(tx)
    } yield {
      updated shouldBe empty
    }
  }

  it should "mark submitted transfers as in progress" in withRequest { (tx, listener) =>
    val updates = request1Transfers.zipWithIndex.collect {
      case ((id, _), i) if i % 3 != 0 =>
        TransferMessage(TransferIds(request1Id, id), i -> json"""{ "i+1": $i }""")
    }

    for {
      _ <- updates.traverse_ {
        case TransferMessage(ids, _) =>
          sql"update transfers set status = 'submitted' where id = ${ids.transfer}".update.run.void
            .transact(tx)
      }
      numUpdated <- listener.markTransfersInProgress(Chunk.seq(updates))
      updated <- sql"select id, status, info from transfers where updated_at is not null"
        .query[(UUID, TransferStatus, Json)]
        .to[List]
        .transact(tx)
    } yield {
      numUpdated shouldBe updated.length
      updated should contain theSameElementsAs updates.map {
        case TransferMessage(ids, (_, info)) =>
          (ids.transfer, TransferStatus.InProgress, info)
      }
    }
  }

  it should "keep the 'updated_at' and 'info' fields of in-progress transfers up-to-date" in withRequest {
    (tx, listener) =>
      val updates = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 != 0 =>
          TransferMessage(TransferIds(request1Id, id), i -> json"""{ "i+1": $i }""")
      }

      for {
        _ <- updates.traverse_ {
          case TransferMessage(ids, _) =>
            sql"""update transfers set
                  status = 'inprogress',
                  updated_at = TO_TIMESTAMP(0),
                  info = '{}'
                  where id = ${ids.transfer}""".update.run.void
              .transact(tx)
        }
        numUpdated <- listener.markTransfersInProgress(Chunk.seq(updates))
        updated <- sql"select id, status, info from transfers where updated_at > TO_TIMESTAMP(0)"
          .query[(UUID, TransferStatus, Json)]
          .to[List]
          .transact(tx)
      } yield {
        numUpdated shouldBe updates.length
        updated should contain theSameElementsAs updates.map {
          case TransferMessage(ids, (_, info)) =>
            (ids.transfer, TransferStatus.InProgress, info)
        }
      }
  }

  it should "not mark transfers in a terminal state to in progress" in withRequest {
    (tx, listener) =>
      val updates = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 != 0 =>
          TransferMessage(TransferIds(request1Id, id), i -> json"""{ "i+1": $i }""")
      }

      for {
        _ <- updates.traverse_ {
          case TransferMessage(ids, _) =>
            sql"update transfers set status = 'succeeded' where id = ${ids.transfer}".update.run.void
              .transact(tx)
        }
        numUpdated <- listener.markTransfersInProgress(Chunk.seq(updates))
        updated <- sql"select id, status, info from transfers where updated_at is not null"
          .query[(UUID, TransferStatus, Json)]
          .to[List]
          .transact(tx)
      } yield {
        numUpdated shouldBe 0
        updated shouldBe empty
      }
  }

  it should "not mark transfers as in progress if IDs are mismatched" in withRequest {
    (tx, listener) =>
      val updates = request1Transfers.zipWithIndex.collect {
        case ((id, _), i) if i % 3 != 0 =>
          TransferMessage(TransferIds(request2Id, id), i -> json"""{ "i+1": $i }""")
      }

      for {
        _ <- updates.traverse_ {
          case TransferMessage(ids, _) =>
            sql"update transfers set status = 'submitted' where id = ${ids.transfer}".update.run.void
              .transact(tx)
        }
        numUpdated <- listener.markTransfersInProgress(Chunk.seq(updates))
        updated <- sql"select id, status, info from transfers where updated_at is not null"
          .query[(UUID, TransferStatus, Json)]
          .to[List]
          .transact(tx)
      } yield {
        numUpdated shouldBe 0
        updated shouldBe empty
      }
  }

  it should "use the latest message if multiple updates are in a batch for one transfer" in withRequest {
    (tx, listener) =>
      val id = request1Transfers.head._1
      val updates = List.tabulate(3) { i =>
        TransferMessage(
          TransferIds(request1Id, id),
          (3 - i) -> json"""{ "i+1": $i }"""
        )
      }

      for {
        _ <- sql"update transfers set status = 'submitted' where id = $id".update.run
          .transact(tx)
        numUpdated <- listener.markTransfersInProgress(Chunk.seq(updates))
        recorded <- sql"select info from transfers where id = $id"
          .query[Json]
          .unique
          .transact(tx)
      } yield {
        numUpdated shouldBe 1
        recorded shouldBe json"""{ "i+1": 0 }"""
      }
  }

  it should "use the latest message on concurrent updates" in withRequest {
    (tx, listener) =>
      val id = request1Transfers.head._1
      val stepCounts = List(1, 2, 3, 4, 3, 2, 1)
      val updates = stepCounts.map { i =>
        TransferMessage(
          TransferIds(request1Id, id),
          i -> json"""{ "step": $i }"""
        )
      }

      for {
        _ <- sql"update transfers set status = 'submitted' where id = $id".update.run
          .transact(tx)
        _ <- updates.parTraverse_ { message =>
          listener.markTransfersInProgress(Chunk.singleton(message))
        }
        recorded <- sql"select info from transfers where id = $id"
          .query[Json]
          .unique
          .transact(tx)
      } yield {
        recorded shouldBe json"""{ "step": 4 }"""
      }
  }

  it should "not deadlock on concurrent batch updates" in withRequest { (tx, listener) =>
    val ids = request1Transfers.map(_._1)
    val updates = ids.zipWithIndex.map {
      case (id, i) =>
        TransferMessage(
          TransferIds(request1Id, id),
          i -> json"""{ "step": $i }"""
        )
    }
    val results = ids.reverse.map { id =>
      TransferMessage(
        TransferIds(request1Id, id),
        (TransferResult.Success: TransferResult) -> json"""{ "id": $id }"""
      )
    }

    val doUpdate = listener.markTransfersInProgress(Chunk.seq(updates))
    val doRecord = listener.recordTransferResults(Chunk.seq(results))

    for {
      _ <- ids.traverse_ { id =>
        sql"update transfers set status = 'submitted' where id = $id".update.run
      }.transact(tx)
      _ <- List(doUpdate, doRecord, doRecord, doUpdate, doRecord, doUpdate).parSequence_
      statuses <- sql"select count(1) from transfers where status = 'succeeded'"
        .query[Long]
        .unique
        .transact(tx)
    } yield {
      statuses shouldBe ids.length
    }
  }

  it should "create new transfers from an expanded transfer request, using its request ID and priority" in withRequest {
    (tx, listener) =>
      val updates = request3Transfers.zipWithIndex.collect {
        case ((id, _), i) =>
          val result =
            if (i < 3) TransferResult.Expanded
            else if (i == 10) TransferResult.FatalFailure
            else TransferResult.Success
          TransferMessage(
            TransferIds(request3Id, id),
            result -> json"""[{ "i+1": $i }]"""
          )
      }

      val (tId1, _) = request3Transfers.head
      val (tId2, _) = request3Transfers(1)

      for {
        _ <- listener.recordTransferResults(Chunk.seq(updates))
        newTransfers <- sql"select id, priority from transfers where request_id = $request3Id and id not in ($tId1, $tId2)"
          .query[(UUID, Short)]
          .to[List]
          .transact(tx)
        originalTransfers <- sql"select info, priority from transfers where status = ${TransferStatus.Expanded: TransferStatus}"
          .query[(Json, Short)]
          .to[List]
          .transact(tx)
      } yield {
        val finalTransfers = newTransfers.map {
          case (transfer, priority) => (json"""[$transfer]""", priority)
        }
        finalTransfers should contain theSameElementsAs originalTransfers
      }
  }
}
