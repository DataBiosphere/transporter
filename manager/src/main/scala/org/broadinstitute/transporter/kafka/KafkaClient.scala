package org.broadinstitute.transporter.kafka

import cats.effect._
import cats.implicits._
import fs2.kafka.{AdminClientSettings, KafkaAdminClient}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import scala.concurrent.ExecutionContext

/**
  * Client responsible for sending requests to / parsing responses from
  * Transporter's backing Kafka cluster.
  *
  * @param adminClient Kafka client which can query and modify
  *                    cluster-level information (i.e. existing topics)
  * @param blockingEc execution context which should run all blocking I/O
  *                   required by the underlying Kafka clients
  * @param cs proof of the ability to shift IO-wrapped computations
  *           onto other threads
  */
class KafkaClient private[kafka] (
  adminClient: KafkaAdminClient[IO],
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {

  private val logger = Slf4jLogger.getLogger[IO]

  /** Check if the client can interact with the backing cluster. */
  def checkReady: IO[Boolean] = {

    val okCheck = for {
      _ <- logger.info("Running status check against Kafka cluster...")
      id <- cs.evalOn(blockingEc)(adminClient.describeCluster.clusterId)
      _ <- logger.debug(s"Got cluster ID $id")
    } yield {
      true
    }

    okCheck.handleErrorWith { err =>
      logger.error(err)("Kafka status check hit error").as(false)
    }
  }
}

object KafkaClient {

  /**
    * Construct a Kafka client, wrapped in logic which will:
    *   1. Automatically set up underlying Kafka client instances
    *      in separate thread pools on startup, and
    *   2. Automatically clean up the underlying clients and their
    *      pools on shutdown
    *
    * @param config settings for the underlying Kafka clients powering
    *               our client
    * @param blockingEc execution context which should run all blocking I/O
    *                   required by the underlying Kafka clients
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  def resource(config: KafkaConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaClient] = {
    val settings = AdminClientSettings.Default
      .withBootstrapServers(config.bootstrapServers.mkString(","))
      .withClientId(config.clientId)
      .withRequestTimeout(config.timeouts.requestTimeout)
      .withCloseTimeout(config.timeouts.closeTimeout)

    fs2.kafka.adminClientResource[IO](settings).map(new KafkaClient(_, blockingEc))
  }
}
