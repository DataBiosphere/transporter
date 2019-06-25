package org.broadinstitute.transporter.kafka.config

import cats.data.NonEmptyList
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Configuration determining how Transporter should connect to its backing Kafka cluster. */
case class ConnectionConfig(
  bootstrapServers: NonEmptyList[String],
  clientId: String,
  requestTimeout: FiniteDuration,
  closeTimeout: FiniteDuration,
  tls: Option[TlsConfig]
)

object ConnectionConfig {
  // Don't listen to IntelliJ; needed for deriving the NonEmptyList reader.
  import pureconfig.module.cats._

  implicit val reader: ConfigReader[ConnectionConfig] = deriveReader
}
