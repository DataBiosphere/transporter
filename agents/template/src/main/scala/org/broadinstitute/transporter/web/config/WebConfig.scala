package org.broadinstitute.transporter.web.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/**
  * Configuration for the agent's web API.
  *
  * @param host the local hostname the agent should bind to
  * @param port the local port the agent should bind to
  */
case class WebConfig(host: String, port: Int)

object WebConfig {
  implicit val reader: ConfigReader[WebConfig] = deriveReader
}
