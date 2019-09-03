package org.broadinstitute.transporter

import org.broadinstitute.transporter.kafka.KStreamsConfig
import org.broadinstitute.transporter.web.config.WebConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Top-level configuration for Transporter agent applications.
  */
case class AgentConfig[RC: ConfigReader](
  kafka: KStreamsConfig,
  runnerConfig: RC,
  web: WebConfig
)

object AgentConfig {
  implicit def reader[RC: ConfigReader]: ConfigReader[AgentConfig[RC]] = deriveReader
}
