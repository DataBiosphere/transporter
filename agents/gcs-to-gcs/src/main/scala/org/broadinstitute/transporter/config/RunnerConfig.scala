package org.broadinstitute.transporter.config

import java.nio.file.Path

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Top-level configuration for the GCS-internal Transporter agent. */
case class RunnerConfig(
  serviceAccountJson: Option[Path],
  timeouts: TimeoutConfig,
  retries: RetryConfig
)

object RunnerConfig {
  implicit val reader: ConfigReader[RunnerConfig] = deriveReader
}
