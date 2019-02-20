package org.broadinstitute.transporter

import org.broadinstitute.transporter.db.DbConfig
import org.broadinstitute.transporter.kafka.KafkaConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class ManagerConfig(port: Int, db: DbConfig, kafka: KafkaConfig)

object ManagerConfig {
  implicit val reader: ConfigReader[ManagerConfig] = deriveReader
}
