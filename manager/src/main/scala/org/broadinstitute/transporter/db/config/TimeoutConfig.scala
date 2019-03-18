package org.broadinstitute.transporter.db.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

/**
  * Timeout-related configuration influencing how Transporter
  * interacts with its backing DB.
  *
  * @param connectionTimeout max time the DB client will wait for a
  *                          connection before raising an error
  * @param maxConnectionLifetime maximum lifetime for connection instances
  *                              in the DB's pool
  * @param connectionValidationTimeout max time that a connection will be
  *                                    tested for aliveness
  * @param leakDetectionThreshold max time a connection can spend outside of
  *                               the pool before it is considered to have leaked
  *
  * @see https://github.com/brettwooldridge/HikariCP/blob/dev/README.md#configuration-knobs-baby
  *      for more detailed information on configuring HikariCP
  */
case class TimeoutConfig(
  connectionTimeout: FiniteDuration,
  maxConnectionLifetime: FiniteDuration,
  connectionValidationTimeout: FiniteDuration,
  leakDetectionThreshold: FiniteDuration
)

object TimeoutConfig {
  implicit val reader: ConfigReader[TimeoutConfig] = deriveReader
}
