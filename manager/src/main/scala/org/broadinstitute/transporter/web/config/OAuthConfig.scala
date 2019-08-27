package org.broadinstitute.transporter.web.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/**
  * Configuration for authentication in Transporter's web API.
  *
  * @param clientId OAuth client ID provided by Google after registering
  *                 a new application in the cloud console
  */
case class OAuthConfig(clientId: String)

object OAuthConfig {
  implicit val reader: ConfigReader[OAuthConfig] = deriveReader

  /** Label to use in Swagger for OAuth-related parameters. */
  val AuthName = "googleoauth"

  /** OAuth scopes which Swagger should request from Google. */
  val AuthScopes = Vector("openid", "email", "profile")
}
