package org.broadinstitute.transporter.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class RunnerConfig(aws: AwsConfig, gcp: GcpConfig)

object RunnerConfig {
  implicit val reader: ConfigReader[RunnerConfig] = deriveReader
}
