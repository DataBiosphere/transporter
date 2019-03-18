package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

/**
  * Settings influencing the size of message batches processed by
  * Transporter's internal Kafka consumers.
  *
  * @param maxRecords upper bound on the number of records to include
  *                   in a single processing batch
  * @param waitTime amount of time the record-collection process should
  *                 wait before emitting a batch when fewer than `maxRecords`
  *                 updates have been received
  */
case class ConsumerBatchConfig(maxRecords: Int, waitTime: FiniteDuration)

object ConsumerBatchConfig {
  implicit val reader: ConfigReader[ConsumerBatchConfig] = deriveReader
}
