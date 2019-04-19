package org.broadinstitute.transporter

import java.sql.DriverManager

import cats.effect.{IO, Resource}
import com.dimafeng.testcontainers.{ForEachTestContainer, PostgreSQLContainer}
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.scalatest.{FlatSpec, Matchers}

trait PostgresSpec extends FlatSpec with Matchers with ForEachTestContainer {

  override val container: PostgreSQLContainer = PostgreSQLContainer("postgres:9.6.10")

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
        val accessor = new ClassLoaderResourceAccessor()
        val liquibase = new Liquibase("changelog.xml", accessor, liquibaseConn)
        IO.delay(liquibase.update(new Contexts()))
      }
      .unsafeRunSync()
  }
}
