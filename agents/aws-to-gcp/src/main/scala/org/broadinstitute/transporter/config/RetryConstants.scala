package org.broadinstitute.transporter.config

import scala.concurrent.duration._

/**
  * Retry configuration for underlying cloud clients used by the transfer runner.
  *
  * These values were originally picked by the GATK team and are also
  * used in Picard and Clio. We've found they make NIO-like operations
  * much less error-prone.
  *
  */
object RetryConstants {

  /** Max time to spend on a single attempt of an cloud operation. */
  val SingleRequestTimeout: FiniteDuration = 3.minutes

  /**
    * Max time to spend trying to complete a cloud operation,
    * covering all retries.
    */
  val TotalRequestTimeout: FiniteDuration = 10.minutes

  /** Max times to try completing a cloud operation. */
  val RetryCount = 15

  /**
    * Time to wait between the first attempt of a cloud operation
    * and the following retry.
    */
  val RetryInitDelay: FiniteDuration = 1.second

  /** Max time to wait between attempts of a cloud operation. */
  val RetryMaxDelay: FiniteDuration = 256.seconds
}
