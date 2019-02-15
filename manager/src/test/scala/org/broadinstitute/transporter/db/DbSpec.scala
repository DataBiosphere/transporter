package org.broadinstitute.transporter.db

import java.nio.file.Paths
import java.sql.DriverManager

import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.FileSystemResourceAccessor
import org.scalatest.FlatSpec

class DbSpec extends FlatSpec with ForAllTestContainer {

  override val container: PostgreSQLContainer = PostgreSQLContainer("postgres:9.6.10")

  private val changelogDir = Paths.get("db")

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

  "Liquibase" should "run migrations" in {
    // No-op because if the test makes it this far, migrations didn't crash.
    succeed
  }
}
