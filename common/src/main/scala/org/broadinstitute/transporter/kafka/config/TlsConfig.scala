package org.broadinstitute.transporter.kafka.config

import java.nio.file.Path

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Config determining how clients should establish encrypted connections to Kafka brokers.
  *
  * @param truststorePath path to a local JKS file containing the CA certificate of the
  *                       Kafka brokers to connect to
  * @param truststorePassword password for the truststore file
  */
case class TlsConfig(
  truststorePath: Path,
  truststorePassword: String
) {

  /** Convert this config to a map containing the properties required by Kafka's API.  */
  def asMap: Map[String, String] = Map(
    CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SSL.name,
    SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> truststorePath.toAbsolutePath.toString,
    SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> truststorePassword
  )
}

object TlsConfig {
  implicit val reader: ConfigReader[TlsConfig] = deriveReader
}
