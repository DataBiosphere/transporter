package org.broadinstitute.transporter

import org.broadinstitute.transporter.db.DbConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class TransporterConfig(port: Int, db: DbConfig)

object TransporterConfig {
  implicit val reader: ConfigReader[TransporterConfig] = deriveReader
}
