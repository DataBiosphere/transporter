package org.broadinstitute.transporter.info

import cats.effect.IO
import doobie.util.transactor.Transactor
import org.broadinstitute.transporter.PostgresSpec
import org.scalamock.scalatest.MockFactory

class InfoControllerSpec extends PostgresSpec with MockFactory {

  private val version = "0.0.0-TEST"

  behavior of "InfoController"

  it should "check the DB for status" in {

    new InfoController(version, transactor).status
      .unsafeRunSync() shouldBe ManagerStatus(
      ok = true,
      systems = Map(
        "db" -> SystemStatus(ok = true, messages = Nil)
      )
    )
  }

  it should "report not-OK when the DB is unreachable" in {
    val badTransactor = Transactor.fromDriverManager[IO](
      container.driverClassName,
      container.jdbcUrl,
      container.username,
      "nope"
    )

    val controller = new InfoController(version, badTransactor)

    controller.status
      .unsafeRunSync() shouldBe ManagerStatus(
      ok = false,
      systems = Map(
        "db" -> SystemStatus(ok = false, messages = List("Can't connect to DB"))
      )
    )
  }

  it should "pass through the app version" in {
    new InfoController(version, transactor).version shouldBe ManagerVersion(version)
  }
}
