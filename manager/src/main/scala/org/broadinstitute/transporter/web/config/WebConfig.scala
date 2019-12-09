package org.broadinstitute.transporter.web.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.duration.FiniteDuration

/**
  * Configuration for Transporter's web API.
  *
  * @param host the local hostname Transporter should bind to
  * @param port the local port Transporter should bind to
  * @param googleOauth optional config specifying how auth should be
  *                    added to Transporter's Swagger UI
  * @param responseTimeout amount of time the server should process an API
  *                        request before the computation is cancelled
  */
case class WebConfig(
  host: String,
  port: Int,
  googleOauth: Option[OAuthConfig],
  responseTimeout: FiniteDuration
)

object WebConfig {
  implicit val reader: ConfigReader[WebConfig] = deriveReader
}
