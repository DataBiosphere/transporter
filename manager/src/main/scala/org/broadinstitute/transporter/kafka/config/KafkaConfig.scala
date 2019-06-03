package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class KafkaConfig(
  connection: ConnectionConfig,
  consumer: ConsumerConfig,
  topics: TopicConfig
)

object KafkaConfig {
  implicit val reader: ConfigReader[KafkaConfig] = deriveReader
}
