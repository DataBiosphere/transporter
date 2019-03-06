package org.broadinstitute.transporter

import org.broadinstitute.transporter.kafka.KStreamsConfig
import org.broadinstitute.transporter.queue.QueueConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Top-level configuration for Transporter agent applications.
  *
  * TODO: Figure out the best way to inject agent-specific config into here.
  */
case class AgentConfig(kafka: KStreamsConfig, queue: QueueConfig)

object AgentConfig {
  implicit val reader: ConfigReader[AgentConfig] = deriveReader
}
