package org.broadinstitute.transporter.info

import cats.effect.IO
import org.broadinstitute.transporter.PostgresSpec
import org.broadinstitute.transporter.kafka.KafkaAdminClient
import org.junit.runner.Description
import org.scalamock.scalatest.MockFactory

class InfoControllerSpec extends PostgresSpec with MockFactory {

  private val version = "0.0.0-TEST"

  private val kafka = mock[KafkaAdminClient]

  def withController(test: InfoController => Any): Unit = {
    test(new InfoController(version, transactor, kafka))
    ()
  }

  behavior of "InfoController"

  it should "check the DB and Kafka for status" in {
    (kafka.checkEnoughBrokers _).expects().returning(IO.pure(true))

    new InfoController(version, transactor, kafka).status
      .unsafeRunSync() shouldBe ManagerStatus(
      ok = true,
      systems = Map(
        "db" -> SystemStatus(ok = true, messages = Nil),
        "kafka" -> SystemStatus(ok = true, messages = Nil)
      )
    )
  }

  it should "report not-OK when the DB is unreachable" in {
    (kafka.checkEnoughBrokers _).expects().returning(IO.pure(true))

    val controller = new InfoController(version, transactor, kafka)
    container.finished()(Description.EMPTY)

    controller.status
      .unsafeRunSync() shouldBe ManagerStatus(
      ok = false,
      systems = Map(
        "db" -> SystemStatus(ok = false, messages = List("Can't connect to DB")),
        "kafka" -> SystemStatus(ok = true, messages = Nil)
      )
    )
  }

  it should "report not-OK when Kafka doesn't have enough brokers" in {
    (kafka.checkEnoughBrokers _).expects().returning(IO.pure(false))

    new InfoController(version, transactor, kafka).status
      .unsafeRunSync() shouldBe ManagerStatus(
      ok = false,
      systems = Map(
        "db" -> SystemStatus(ok = true, messages = Nil),
        "kafka" -> SystemStatus(
          ok = false,
          messages = List("Not enough Kafka brokers in cluster")
        )
      )
    )
  }

  it should "report not-OK when Kafka is unreachable" in {
    (kafka.checkEnoughBrokers _)
      .expects()
      .returning(IO.raiseError(new RuntimeException("BOOM")))

    new InfoController(version, transactor, kafka).status
      .unsafeRunSync() shouldBe ManagerStatus(
      ok = false,
      systems = Map(
        "db" -> SystemStatus(ok = true, messages = Nil),
        "kafka" -> SystemStatus(ok = false, messages = List("Can't connect to Kafka"))
      )
    )
  }

  it should "pass through the app version" in {
    new InfoController(version, transactor, kafka).version
      .unsafeRunSync() shouldBe ManagerVersion(version)
  }
}
