package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class KafkaConfig(
  connection: ConnectionConfig,
  admin: AdminConfig,
  consumer: ConsumerConfig
)

object KafkaConfig {
  implicit val reader: ConfigReader[KafkaConfig] = deriveReader
}
