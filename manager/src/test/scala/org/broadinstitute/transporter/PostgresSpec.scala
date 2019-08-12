package org.broadinstitute.transporter

import java.sql.DriverManager

import cats.effect.{ContextShift, IO, Resource}
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import doobie.util.transactor.Transactor
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}

import scala.concurrent.ExecutionContext

trait PostgresSpec
    extends FlatSpec
    with Matchers
    with ForAllTestContainer
    with BeforeAndAfterEach {

  protected implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  override val container: PostgreSQLContainer = PostgreSQLContainer("postgres:9.6.10")

  override def beforeEach(): Unit = {
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
      .use { conn =>
        val liquibase =
          new Liquibase("changelog.xml", new ClassLoaderResourceAccessor(), conn)
        IO.delay {
          liquibase.dropAll()
          liquibase.update(new Contexts())
        }
      }
      .unsafeRunSync()
  }

  def transactor: Transactor[IO] = Transactor.fromDriverManager[IO](
    container.driverClassName,
    container.jdbcUrl,
    container.username,
    container.password
  )
}
