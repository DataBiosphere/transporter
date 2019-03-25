package org.broadinstitute.transporter.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Top-level container for AWS->GCP configuration. */
case class RunnerConfig(aws: AwsConfig, gcp: GcpConfig)

object RunnerConfig {
  implicit val reader: ConfigReader[RunnerConfig] = deriveReader
}
