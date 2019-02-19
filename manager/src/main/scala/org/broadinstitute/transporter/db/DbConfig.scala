package org.broadinstitute.transporter.db

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class DbConfig(
  driverClassname: String,
  connectURL: String,
  username: String,
  password: String
)

object DbConfig {
  implicit val reader: ConfigReader[DbConfig] = deriveReader
}
