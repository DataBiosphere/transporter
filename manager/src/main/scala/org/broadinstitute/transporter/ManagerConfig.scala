package org.broadinstitute.transporter

import org.broadinstitute.transporter.db.DbConfig
import org.broadinstitute.transporter.kafka.KafkaConfig
import org.broadinstitute.transporter.web.WebConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/** Top-level configuration for the Transporter application. */
case class ManagerConfig(web: WebConfig, db: DbConfig, kafka: KafkaConfig)

object ManagerConfig {
  implicit val reader: ConfigReader[ManagerConfig] = deriveReader
}
