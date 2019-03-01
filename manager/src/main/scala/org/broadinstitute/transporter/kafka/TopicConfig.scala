package org.broadinstitute.transporter.kafka

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class TopicConfig(partitions: Int, replicationFactor: Short)

object TopicConfig {
  implicit val reader: ConfigReader[TopicConfig] = deriveReader
}
