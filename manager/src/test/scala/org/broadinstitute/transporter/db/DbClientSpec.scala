package org.broadinstitute.transporter.db

import java.nio.file.Paths
import java.sql.DriverManager

import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import doobie.implicits._
import doobie.util.transactor.Transactor
import io.circe.literal._
import liquibase.{Contexts, Liquibase}
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.FileSystemResourceAccessor
import org.broadinstitute.transporter.queue.{QueueRequest, QueueSchema}
import org.broadinstitute.transporter.transfer.TransferRequest
import org.scalatest.{EitherValues, FlatSpec, Matchers, OptionValues}

import scala.concurrent.ExecutionContext

class DbClientSpec
    extends FlatSpec
    with ForAllTestContainer
    with Matchers
    with EitherValues
    with OptionValues {

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

  private val schema = json"{}".as[QueueSchema].right.value

  behavior of "DbClient"

  it should "report ready on good configuration" in {
    val client = new DbClient.Impl(testTransactor(container.password))
    client.checkReady.unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val client = new DbClient.Impl(testTransactor("nope"))
    client.checkReady.unsafeRunSync() shouldBe false
  }

  it should "create, lookup, and delete transfer queues" in {
    val request = QueueRequest("test-queue", schema)

    val client = new DbClient.Impl(testTransactor(container.password))

    val check = for {
      res <- client.lookupQueueInfo(request.name)
      _ <- client.createQueue(request)
      res2 <- client.lookupQueueInfo(request.name)
      _ <- client.deleteQueue(request.name)
      res3 <- client.lookupQueueInfo(request.name)
    } yield {
      res shouldBe None
      val (_, _, _, schema) = res2.value
      schema shouldBe request.schema
      res3 shouldBe None
    }

    check.unsafeRunSync()
  }

  it should "fail to double-create a queue by name" in {
    val request = QueueRequest("test-queue2", schema)

    val client = new DbClient.Impl(testTransactor(container.password))

    val tryInsert = for {
      _ <- client.createQueue(request)
      _ <- client.createQueue(request)
    } yield ()

    tryInsert.attempt.unsafeRunSync().isLeft shouldBe true
  }

  it should "no-op when deleting a nonexistent queue" in {
    val client = new DbClient.Impl(testTransactor(container.password))
    client.deleteQueue("nope").unsafeRunSync()
  }

  it should "record and delete new transfer requests under a queue" in {
    val transactor = testTransactor(container.password)
    val client = new DbClient.Impl(transactor)

    val requests = TransferRequest(List.fill(10)(json"{}"))

    val countsQuery = for {
      requestCount <- sql"select count(*) from transfer_requests".query[Long].unique
      transferCount <- sql"select count(*) from transfers".query[Long].unique
    } yield {
      (requestCount, transferCount)
    }

    val checks = for {
      _ <- client.createQueue(QueueRequest("foo", schema))
      (queueId, _, _, _) <- client
        .lookupQueueInfo("foo")
        .flatMap(_.liftTo[IO](new IllegalStateException("Queue not found")))
      (initReqs, initTransfers) <- countsQuery.transact(transactor)
      (requestId, _) <- client.recordTransferRequest(queueId, requests)
      (postReqs, postTransfers) <- countsQuery.transact(transactor)
      _ <- client.deleteTransferRequest(requestId)
      (finalReqs, finalTransfers) <- countsQuery.transact(transactor)
      _ <- client.deleteQueue("foo")
    } yield {
      initReqs shouldBe 0
      initTransfers shouldBe 0
      postReqs shouldBe 1
      postTransfers shouldBe 10
      finalReqs shouldBe 0
      finalTransfers shouldBe 0
    }

    checks.unsafeRunSync()
  }

}
