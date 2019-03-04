package org.broadinstitute.transporter.db

import java.nio.file.Paths
import java.sql.DriverManager

import cats.effect.{ContextShift, IO, Resource}
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import doobie.util.transactor.Transactor
import io.circe.literal._
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.FileSystemResourceAccessor
import org.broadinstitute.transporter.queue.{Queue, QueueSchema}
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

  private def testTransactor(password: String): Transactor[IO] =
    Transactor.fromDriverManager[IO](
      container.driverClassName,
      container.jdbcUrl,
      container.username,
      password
    )

  behavior of "DbClient"

  it should "report ready on good configuration" in {
    val client = new DbClient.Impl(testTransactor(container.password))
    client.checkReady.unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val client = new DbClient.Impl(testTransactor("nope"))
    client.checkReady.unsafeRunSync() shouldBe false
  }

  it should "insert, lookup, and delete transfer queues" in {
    val queue = Queue(
      "test-queue",
      "test-queue.requests",
      "test-queue.responses",
      json"{}".as[QueueSchema].right.get
    )

    val client = new DbClient.Impl(testTransactor(container.password))

    val check = for {
      res <- client.lookupQueue(queue.name)
      _ <- client.insertQueue(queue)
      res2 <- client.lookupQueue(queue.name)
      _ <- client.deleteQueue(queue.name)
      res3 <- client.lookupQueue(queue.name)
    } yield {
      res shouldBe None
      res2 shouldBe Some(queue)
      res3 shouldBe None
    }

    check.unsafeRunSync()
  }

  it should "fail to double-insert a queue by name" in {
    val queue = Queue(
      "test-queue",
      "test-queue.requests",
      "test-queue.responses",
      json"{}".as[QueueSchema].right.get
    )

    val client = new DbClient.Impl(testTransactor(container.password))

    val tryInsert = for {
      _ <- client.insertQueue(queue)
      _ <- client.insertQueue(queue)
    } yield ()

    tryInsert.attempt.unsafeRunSync().isLeft shouldBe true
  }

  it should "no-op when deleting a nonexistent queue" in {
    val client = new DbClient.Impl(testTransactor(container.password))
    client.deleteQueue("nope").unsafeRunSync()
  }
}
