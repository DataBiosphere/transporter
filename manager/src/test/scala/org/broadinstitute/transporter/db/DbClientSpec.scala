package org.broadinstitute.transporter.db

import java.nio.file.Paths
import java.sql.DriverManager

import cats.effect.{ContextShift, IO, Resource}
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import doobie.util.transactor.Transactor
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.FileSystemResourceAccessor
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

class DbClientSpec extends FlatSpec with ForAllTestContainer with Matchers {

  override val container: PostgreSQLContainer = PostgreSQLContainer("postgres:9.6.10")

  private val changelogDir = Paths.get("db")

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  override def afterStart(): Unit = {
    val acquireConn = for {
      _ <- IO(Class.forName(container.driverClassName))
      jdbc <- IO(
        DriverManager
          .getConnection(container.jdbcUrl, container.username, container.password)
      )
    } yield {
      new JdbcConnection(jdbc)
    }

    val releaseConn = (c: JdbcConnection) => IO.delay(c.close())

    Resource
      .make(acquireConn)(releaseConn)
      .use { liquibaseConn =>
        val accessor =
          new FileSystemResourceAccessor(changelogDir.toAbsolutePath.toString)
        val liquibase = new Liquibase("changelog.xml", accessor, liquibaseConn)
        IO.delay(liquibase.update(new Contexts()))
      }
      .unsafeRunSync()
  }

  private def baseConfig = DbConfig(
    container.driverClassName,
    container.jdbcUrl,
    container.username,
    container.password
  )

  private def testTransactor(config: DbConfig): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      config.driverClassname,
      config.connectURL,
      config.username,
      config.password
    )

  behavior of "DbClient"

  it should "report ready on good configuration" in {
    val client = new DbClient(testTransactor(baseConfig))
    client.checkReady.unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val client = new DbClient(testTransactor(baseConfig.copy(password = "nope")))
    client.checkReady.unsafeRunSync() shouldBe false
  }
}
