package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration for Kafka consumers used by Transporter.
  *
  * @param groupId group ID which should be reported by constructed consumers
  * @param maxRecordsPerBatch maximum number of records consumers should collect
  *                           before emitting to downstream processing
  * @param waitTimePerBatch max time consumers should wait for new records before
  *                         emitting to downstream processing
  * @param topicMetadataTtl TTL for cached topic metadata within consumers. Determines
  *                         how often consumers subscribed to a naming pattern will
  *                         discover new topics.
  */
case class ConsumerConfig(
  groupId: String,
  maxRecordsPerBatch: Int,
  waitTimePerBatch: FiniteDuration,
  topicMetadataTtl: FiniteDuration
)

object ConsumerConfig {
  implicit val reader: ConfigReader[ConsumerConfig] = deriveReader
}
