package org.broadinstitute.transporter.info

import cats.effect.{ContextShift, IO}
import org.broadinstitute.transporter.db.DbClient
import org.broadinstitute.transporter.kafka.KafkaClient
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class InfoControllerSpec extends FlatSpec with Matchers with MockFactory {

  private val version = "0.0.0-TEST"

  private val db = mock[DbClient]
  private val kafka = mock[KafkaClient]

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  private def controller = new InfoController(version, db, kafka)

  behavior of "InfoController"

  it should "check the DB and Kafka for status" in {
    (db.checkReady _).expects().returning(IO.pure(true))
    (kafka.checkReady _).expects().returning(IO.pure(true))

    controller.status.unsafeRunSync() shouldBe ManagerStatus(
      ok = true,
      systems = Map(
        "db" -> SystemStatus(ok = true, messages = Nil),
        "kafka" -> SystemStatus(ok = true, messages = Nil)
      )
    )
  }

  it should "report not-OK when the DB isn't ready" in {
    (db.checkReady _).expects().returning(IO.pure(false))
    (kafka.checkReady _).expects().returning(IO.pure(true))

    controller.status.unsafeRunSync() shouldBe ManagerStatus(
      ok = false,
      systems = Map(
        "db" -> SystemStatus(ok = false, messages = List("Can't connect to DB")),
        "kafka" -> SystemStatus(ok = true, messages = Nil)
      )
    )
  }

  it should "report not-OK when Kafka isn't ready" in {
    (db.checkReady _).expects().returning(IO.pure(true))
    (kafka.checkReady _).expects().returning(IO.pure(false))

    controller.status.unsafeRunSync() shouldBe ManagerStatus(
      ok = false,
      systems = Map(
        "db" -> SystemStatus(ok = true, messages = Nil),
        "kafka" -> SystemStatus(ok = false, messages = List("Can't connect to Kafka"))
      )
    )
  }

  it should "pass through the app version" in {
    controller.version.unsafeRunSync() shouldBe ManagerVersion(version)
  }
}
