package org.broadinstitute.transporter.transfer

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
import org.scalamock.scalatest.MockFactory
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext

class TransferListenerSpec extends PostgresSpec with MockFactory with EitherValues {
  import org.broadinstitute.transporter.db.DoobieInstances._

  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val progress = mock[KafkaConsumer[(Int, Json)]]
  private val results = mock[KafkaConsumer[(TransferResult, Json)]]

  private val request1Id = UUID.randomUUID()
  private val request1Transfers = List.tabulate(50) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  private val request2Id = UUID.randomUUID()
  private val request2Transfers = List.tabulate(20) { i =>
    UUID.randomUUID() -> json"""{ "i": $i }"""
  }

  def withRequest(test: (Transactor[IO], TransferListener) => IO[Any]): Unit = {
    val tx = transactor
    val listener = new TransferListener(tx, progress, results)

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

    for {
      _ <- ids.traverse_ { id =>
        sql"update transfers set status = 'submitted' where id = $id".update.run
      }.transact(tx)
      _ <- (
        listener.markTransfersInProgress(Chunk.seq(updates)),
        listener.recordTransferResults(Chunk.seq(results))
      ).parMapN {
        case (_, _) => ()
      }
      statuses <- sql"select count(1) from transfers where status = 'succeeded'"
        .query[Long]
        .unique
        .transact(tx)
    } yield {
      statuses shouldBe ids.length
    }
  }
}
