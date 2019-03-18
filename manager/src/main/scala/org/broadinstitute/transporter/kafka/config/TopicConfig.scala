package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Default configuration for new Kafka topics created by Transporter.
  *
  * @param partitions partition count to use for new topics
  * @param replicationFactor replication factor to use for new topics
  */
case class TopicConfig(partitions: Int, replicationFactor: Short)

object TopicConfig {
  implicit val reader: ConfigReader[TopicConfig] = deriveReader
}
