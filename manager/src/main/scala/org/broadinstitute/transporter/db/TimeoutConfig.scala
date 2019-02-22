package org.broadinstitute.transporter.db

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

case class TimeoutConfig(
  connectionTimeout: FiniteDuration,
  maxConnectionLifetime: FiniteDuration,
  connectionValidationTimeout: FiniteDuration,
  leakDetectionThreshold: FiniteDuration
)

object TimeoutConfig {
  implicit val reader: ConfigReader[TimeoutConfig] = deriveReader
}
