package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for admin-level operations performed by Transporter in Kafka.
  *
  * @param replicationFactor replication factor to set on all new topics created
  *                          by Transporter. Cannot be larger than the number of
  *                          brokers in the Kafka cluster
  */
case class AdminConfig(replicationFactor: Short)

object AdminConfig {
  implicit val reader: ConfigReader[AdminConfig] = deriveReader
}
