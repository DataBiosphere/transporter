package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO}
import io.chrisdavenport.fuuid.FUUID
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaConsumer
import org.broadinstitute.transporter.transfer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class ResultListenerSpec extends FlatSpec with Matchers with MockFactory {

  private val db = mock[DbClient]
  private val consumer = mock[KafkaConsumer[FUUID, TransferSummary[Option[Json]]]]

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private def listener = new transfer.ResultListener.Impl(consumer, db)

  private def fuuid() = FUUID.randomFUUID[IO].unsafeRunSync()

  behavior of "ResultListener"

  it should "record successes and fatal failures" in {
    val results = List[TransferSummary[Option[Json]]](
      TransferSummary(TransferResult.Success, Some(json"{}")),
      TransferSummary(TransferResult.FatalFailure, None),
      TransferSummary(TransferResult.Success, None),
      TransferSummary(TransferResult.FatalFailure, Some(json"[]"))
    ).map(fuuid() -> _)

    (db.updateTransfers _).expects(results).returning(IO.unit)

    listener.processBatch(results.map(Right(_))).unsafeRunSync()
  }

  it should "not crash if Kafka receives malformed data" in {
    val results = List[TransferSummary[Option[Json]]](
      TransferSummary(TransferResult.Success, Some(json"{}")),
      TransferSummary(TransferResult.FatalFailure, None),
      TransferSummary(TransferResult.Success, None),
      TransferSummary(TransferResult.FatalFailure, Some(json"[]"))
    ).map(fuuid() -> _)

    val batch = List.concat(
      List(Left(new IllegalStateException("WAT"))),
      results.map(Right(_)),
      List(Left(new IllegalStateException("WOT")))
    )

    (db.updateTransfers _).expects(results).returning(IO.unit)

    listener.processBatch(batch).unsafeRunSync()
  }
}
