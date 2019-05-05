package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

case class ConsumerConfig(
  groupId: String,
  maxRecordsPerBatch: Int,
  waitTimePerBatch: FiniteDuration,
  topicMetadataTtl: FiniteDuration
)

object ConsumerConfig {
  implicit val reader: ConfigReader[ConsumerConfig] = deriveReader
}
