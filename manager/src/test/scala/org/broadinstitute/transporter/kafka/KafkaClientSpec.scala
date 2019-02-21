package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO}
import com.dimafeng.testcontainers.{Container, ForAllTestContainer, TestContainerProxy}
import fs2.kafka.AdminClientSettings
import org.scalatest.{FlatSpec, Matchers}
import org.testcontainers.containers.KafkaContainer

import scala.concurrent.ExecutionContext

class KafkaClientSpec extends FlatSpec with ForAllTestContainer with Matchers {

  private val baseContainer = new KafkaContainer("5.1.1")

  override val container: Container = new TestContainerProxy[KafkaContainer] {
    override val container: KafkaContainer = baseContainer
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  behavior of "KafkaClient"

  it should "report ready on good configuration" in {
    val settings =
      AdminClientSettings.Default.withBootstrapServers(baseContainer.getBootstrapServers)

    fs2.kafka
      .adminClientResource[IO](settings)
      .map(new KafkaClient(_))
      .use(_.checkReady)
      .unsafeRunSync() shouldBe true
  }

  it should "report not ready on bad configuration" in {
    val settings =
      AdminClientSettings.Default.withBootstrapServers(
        baseContainer.getBootstrapServers.dropRight(1)
      )

    fs2.kafka
      .adminClientResource[IO](settings)
      .map(new KafkaClient(_))
      .use(_.checkReady)
      .unsafeRunSync() shouldBe false
  }
}
