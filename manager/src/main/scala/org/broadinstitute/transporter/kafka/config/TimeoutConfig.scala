package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

/**
  * Timeout-related configuration influencing how Transporter
  * interacts with its backing Kafka cluster.
  */
case class TimeoutConfig(
  requestTimeout: FiniteDuration,
  closeTimeout: FiniteDuration,
  topicDiscoveryInterval: FiniteDuration
)

object TimeoutConfig {
  implicit val reader: ConfigReader[TimeoutConfig] = deriveReader
}
