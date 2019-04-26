package org.broadinstitute.transporter

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class RetryConfig(maxRetries: Int, maxDelay: FiniteDuration)

object RetryConfig {
  implicit val reader: ConfigReader[RetryConfig] = deriveReader
}
