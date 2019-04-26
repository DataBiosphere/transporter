package org.broadinstitute.transporter.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/**
  * Timeouts to use for HTTP requests to AWS / GCP.
  *
  * @param responseHeaderTimeout amount of time to wait for headers to arrive for
  *                              a response from AWS / GCP
  * @param requestTimeout amount of time to wait to read the entire body of a response
  *                       from AWS / GCP
  */
case class TimeoutConfig(
  responseHeaderTimeout: FiniteDuration,
  requestTimeout: FiniteDuration
)

object TimeoutConfig {
  implicit val reader: ConfigReader[TimeoutConfig] = deriveReader
}
