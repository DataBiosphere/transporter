package org.broadinstitute.transporter.kafka.config

import cats.data.NonEmptyList
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Configuration determining how Transporter should connect to its backing Kafka cluster. */
case class ConnectionConfig(
  bootstrapServers: NonEmptyList[String],
  clientId: String,
  requestTimeout: FiniteDuration,
  closeTimeout: FiniteDuration,
  tls: Option[TlsConfig],
  scramSha: Option[ScramConfig]
)

object ConnectionConfig {
  // Don't listen to IntelliJ; needed for deriving the NonEmptyList reader.
  import pureconfig.module.cats._

  implicit val reader: ConfigReader[ConnectionConfig] = deriveReader

  /**
    * Build a map of security-related properties from related (possibly missing)
    * configuration objects, to pass on to Kafka's APIs.
    *
    * @param maybeTls optional config describing how the client should connect to Kafka over TLS
    * @param maybeScramSha optional config describing how the client should authenticate to Kafka
    *                      using SASL SCRAM-SHA-256
    */
  def securityProperties(
    maybeTls: Option[TlsConfig],
    maybeScramSha: Option[ScramConfig]
  ): Map[String, String] = (maybeTls, maybeScramSha) match {
    case (None, None) =>
      Map(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.PLAINTEXT.name)
    case (Some(tls), None) =>
      tls.asMap + (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SSL.name)
    case (None, Some(scram)) =>
      scram.asMap + (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SASL_PLAINTEXT.name)
    case (Some(tls), Some(scram)) =>
      tls.asMap ++ scram.asMap + (CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SASL_SSL.name)
  }
}
