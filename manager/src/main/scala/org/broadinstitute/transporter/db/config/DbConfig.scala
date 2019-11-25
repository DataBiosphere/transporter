package org.broadinstitute.transporter.db.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/** Configuration determining how Transporter should interact with its backing DB. */
case class DbConfig(
  host: String,
  dbName: String,
  username: String,
  password: String,
  timeouts: TimeoutConfig
) {
  def connectURL: String = s"jdbc:postgresql://$host/$dbName"
}

object DbConfig {
  implicit val reader: ConfigReader[DbConfig] = deriveReader

  val DriverClassName: String = "org.postgresql.Driver"
}
