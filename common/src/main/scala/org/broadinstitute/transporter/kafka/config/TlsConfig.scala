package org.broadinstitute.transporter.kafka.config

import java.nio.file.Path

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class TlsConfig(
  truststorePath: Path,
  truststorePassword: String
)

object TlsConfig {
  implicit val reader: ConfigReader[TlsConfig] = deriveReader
}
