package org.broadinstitute.transporter.transfer

import java.util.UUID

import cats.effect.{ContextShift, IO}
import io.circe.Json
import io.circe.literal._
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaConsumer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class ResultListenerSpec extends FlatSpec with Matchers with MockFactory {

  private val db = mock[DbClient]
  private val consumer = mock[KafkaConsumer[TransferSummary[Json]]]

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private val requestId = UUID.randomUUID()
  private val results = List(
    TransferSummary(
      TransferResult.Success,
      json"{}",
      id = UUID.randomUUID(),
      requestId = requestId
    ),
    TransferSummary(
      TransferResult.FatalFailure,
      json"1",
      id = UUID.randomUUID(),
      requestId = requestId
    ),
    TransferSummary(
      TransferResult.Success,
      json""""hey"""",
      id = UUID.randomUUID(),
      requestId = requestId
    ),
    TransferSummary(
      TransferResult.FatalFailure,
      json"[]",
      id = UUID.randomUUID(),
      requestId = requestId
    ),
    TransferSummary(
      TransferResult.Success,
      json"null",
      id = UUID.randomUUID(),
      requestId = requestId
    )
  )

  private def listener = new ResultListener(consumer, db)

  behavior of "ResultListener"

  it should "record successes and fatal failures" in {
    (db.updateTransfers _).expects(results).returning(IO.unit)
    listener.processBatch(results.map(Right(_))).unsafeRunSync()
  }

  it should "not crash if Kafka receives malformed data" in {
    val batch = List.concat(
      List(Left(new IllegalStateException("WAT"))),
      results.map(Right(_)),
      List(Left(new IllegalStateException("WOT")))
    )

    (db.updateTransfers _).expects(results).returning(IO.unit)
    listener.processBatch(batch).unsafeRunSync()
  }
}
