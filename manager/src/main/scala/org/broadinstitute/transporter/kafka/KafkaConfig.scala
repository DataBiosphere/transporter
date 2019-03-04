package org.broadinstitute.transporter.kafka

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/** Configuration determining how Transporter should interact with its backing Kafka cluster. */
case class KafkaConfig(
  bootstrapServers: List[String],
  clientId: String,
  topicDefaults: TopicConfig,
  timeouts: TimeoutConfig
)

object KafkaConfig {
  implicit val reader: ConfigReader[KafkaConfig] = deriveReader
}
