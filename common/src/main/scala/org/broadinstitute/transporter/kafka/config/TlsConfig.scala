package org.broadinstitute.transporter.kafka.config

import java.nio.file.Path

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
)

object TlsConfig {
  implicit val reader: ConfigReader[TlsConfig] = deriveReader
}
