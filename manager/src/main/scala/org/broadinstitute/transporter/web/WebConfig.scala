package org.broadinstitute.transporter.web

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/**
  * Configuration for Transporter's web API.
  *
  * @param host the local hostname Transporter should bind to
  * @param port the local port Transporter should bind to
  */
case class WebConfig(host: String, port: Int)

object WebConfig {
  implicit val reader: ConfigReader[WebConfig] = deriveReader
}
