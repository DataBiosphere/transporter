package org.broadinstitute.transporter.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/** Config determining retry behavior on failed HTTP requests in the GCS-internal agent. */
case class RetryConfig(maxRetries: Int, maxDelay: FiniteDuration)

object RetryConfig {
  implicit val reader: ConfigReader[RetryConfig] = deriveReader
}
