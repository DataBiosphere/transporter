package org.broadinstitute.transporter

import org.broadinstitute.transporter.kafka.KStreamsConfig
import org.broadinstitute.transporter.queue.QueueConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class AgentConfig(kafka: KStreamsConfig, queue: QueueConfig)

object AgentConfig {
  implicit val reader: ConfigReader[AgentConfig] = deriveReader
}
