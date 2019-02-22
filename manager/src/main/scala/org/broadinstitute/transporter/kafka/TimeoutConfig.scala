package org.broadinstitute.transporter.kafka

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

case class TimeoutConfig(
  requestTimeout: FiniteDuration,
  closeTimeout: FiniteDuration
)

object TimeoutConfig {
  implicit val reader: ConfigReader[TimeoutConfig] = deriveReader
}
