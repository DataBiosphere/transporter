package org.broadinstitute.transporter.db.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/** Configuration determining how Transporter should interact with its backing DB. */
case class DbConfig(
  driverClassname: String,
  connectURL: String,
  username: String,
  password: String,
  timeouts: TimeoutConfig
)

object DbConfig {
  implicit val reader: ConfigReader[DbConfig] = deriveReader
}
