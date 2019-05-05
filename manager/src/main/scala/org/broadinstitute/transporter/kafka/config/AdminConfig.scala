package org.broadinstitute.transporter.kafka.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class AdminConfig(replicationFactor: Short)

object AdminConfig {

  /**
    * Number of partitions to initialize in topics created by Transporter.
    *
    * NOTE: This sets a max parallelism for all transfer queues. We should move
    * this into the DB as a per-queue parameter, and allow users to increase it
    * as necessary.
    */
  val TopicPartitions = 16

  implicit val reader: ConfigReader[AdminConfig] = deriveReader
}
