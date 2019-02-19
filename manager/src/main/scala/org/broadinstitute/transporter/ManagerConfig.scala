package org.broadinstitute.transporter

import org.broadinstitute.transporter.db.DbConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class ManagerConfig(port: Int, db: DbConfig)

object ManagerConfig {
  implicit val reader: ConfigReader[ManagerConfig] = deriveReader
}
