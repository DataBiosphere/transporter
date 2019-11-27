package org.broadinstitute.transporter.config

import org.broadinstitute.transporter.RetryConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Top-level container for AWS->GCP configuration. */
case class RunnerConfig(
  aws: AwsConfig,
  gcp: GcpConfig,
  timeouts: TimeoutConfig,
  retries: RetryConfig,
  mibPerStep: Int
)

object RunnerConfig {
  implicit val reader: ConfigReader[RunnerConfig] = deriveReader
}
