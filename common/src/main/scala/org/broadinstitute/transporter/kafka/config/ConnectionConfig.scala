package org.broadinstitute.transporter.kafka.config

import cats.data.NonEmptyList
import cats.implicits._
import fs2.kafka._
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Configuration determining how Transporter should interact with its backing Kafka cluster. */
case class ConnectionConfig(
  bootstrapServers: NonEmptyList[String],
  clientId: String,
  requestTimeout: FiniteDuration,
  closeTimeout: FiniteDuration
) {

  // Underlying Kafka libs want everything as a string...
  private lazy val bootstrapString = bootstrapServers.mkString_(",")

  /** Convert this config into settings for a [[org.apache.kafka.clients.admin.AdminClient]]. */
  def adminSettings: AdminClientSettings =
    AdminClientSettings.Default
    // Required to connect to Kafka at all.
      .withBootstrapServers(bootstrapString)
      // For debugging on the Kafka server; adds an ID to the logs.
      .withClientId(clientId)
      // No "official" recommendation on these values, we can tweak as we see fit.
      .withRequestTimeout(requestTimeout)
      .withCloseTimeout(closeTimeout)
}

object ConnectionConfig {
  // Don't listen to IntelliJ; needed for deriving the NonEmptyList reader.
  import pureconfig.module.cats._

  implicit val reader: ConfigReader[ConnectionConfig] = deriveReader
}
