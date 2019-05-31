package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class TopicConfig(requestTopic: String, progressTopic: String, resultTopic: String)

object TopicConfig {
  implicit val reader: ConfigReader[TopicConfig] = deriveReader
}
