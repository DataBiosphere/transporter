package org.broadinstitute.transporter.kafka

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

/**
  * Timeout-related configuration influencing how Transporter
  * interacts with its backing Kafka cluster.
  */
case class TimeoutConfig(
  requestTimeout: FiniteDuration,
  closeTimeout: FiniteDuration
)

object TimeoutConfig {
  implicit val reader: ConfigReader[TimeoutConfig] = deriveReader
}
