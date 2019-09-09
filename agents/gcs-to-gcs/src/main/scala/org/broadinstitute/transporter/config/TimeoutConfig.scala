package org.broadinstitute.transporter.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Config for HTTP timeouts in the GCS-internal agent. */
case class TimeoutConfig(
  responseHeaderTimeout: FiniteDuration,
  requestTimeout: FiniteDuration
)

object TimeoutConfig {
  implicit val reader: ConfigReader[TimeoutConfig] = deriveReader
}
