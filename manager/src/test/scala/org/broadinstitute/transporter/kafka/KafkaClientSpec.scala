package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Timer}
import com.dimafeng.testcontainers.{Container, ForAllTestContainer, TestContainerProxy}
import doobie.util.ExecutionContexts
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.KafkaContainer

import scala.concurrent.ExecutionContext

class KafkaClientSpec extends FlatSpec with ForAllTestContainer with Matchers {

  private val baseContainer = new KafkaContainer("5.1.1")

  override val container: Container = new TestContainerProxy[KafkaContainer] {
    override val container: KafkaContainer = baseContainer
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val blockingEc = ExecutionContexts.cachedThreadPool[IO]

  behavior of "KafkaClient"

  it should "report ready on good configuration" in {
    val settings =
      KafkaConfig(baseContainer.getBootstrapServers.split(',').toList, "test-admin")

    val clientResource = for {
      ec <- blockingEc
      client <- KafkaClient.resource(settings, ec)
    } yield {
      client
    }

    clientResource.use(_.checkReady).unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val settings = KafkaConfig(
      baseContainer.getBootstrapServers.dropRight(1).split(',').toList,
      "test-admin"
    )

    val clientResource = for {
      ec <- blockingEc
      client <- KafkaClient.resource(settings, ec)
    } yield {
      client
    }

    clientResource.use(_.checkReady).unsafeRunSync() shouldBe false
  }
}
