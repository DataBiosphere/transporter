package org.broadinstitute.transporter

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/**
  * Parameters for tweaking the approach used for retrying transient HTTP failures.
  *
  * @param maxRetries upper bound on number of times a single request can be retried
  * @param maxDelay upper bound on time that can be spent between two retries of a single request
  */
case class RetryConfig(maxRetries: Int, maxDelay: FiniteDuration)

object RetryConfig {
  implicit val reader: ConfigReader[RetryConfig] = deriveReader
}
