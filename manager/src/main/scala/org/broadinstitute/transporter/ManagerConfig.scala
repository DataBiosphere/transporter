package org.broadinstitute.transporter
import org.broadinstitute.transporter.db.config.DbConfig
import org.broadinstitute.transporter.kafka.config.KafkaConfig
import org.broadinstitute.transporter.web.config.WebConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

/** Top-level configuration for the Transporter application. */
case class ManagerConfig(
  web: WebConfig,
  db: DbConfig,
  kafka: KafkaConfig,
  submissionInterval: FiniteDuration
)

object ManagerConfig {
  implicit val reader: ConfigReader[ManagerConfig] = deriveReader
}
