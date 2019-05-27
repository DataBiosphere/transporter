package org.broadinstitute.transporter.web.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/**
  * Configuration for Transporter's web API.
  *
  * @param host the local hostname Transporter should bind to
  * @param port the local port Transporter should bind to
  * @param googleOauth optional config specifying how auth should be
  *                    added to Transporter's Swagger UI
  */
case class WebConfig(host: String, port: Int, googleOauth: Option[OAuthConfig])

object WebConfig {
  implicit val reader: ConfigReader[WebConfig] = deriveReader
}
