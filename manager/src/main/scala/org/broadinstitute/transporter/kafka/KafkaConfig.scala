package org.broadinstitute.transporter.kafka

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class KafkaConfig(
  bootstrapServers: List[String],
  clientId: String,
  timeouts: TimeoutConfig
)

object KafkaConfig {
  implicit val reader: ConfigReader[KafkaConfig] = deriveReader
}
