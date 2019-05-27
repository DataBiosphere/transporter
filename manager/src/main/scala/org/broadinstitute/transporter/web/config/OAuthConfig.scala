package org.broadinstitute.transporter.web.config

import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

case class OAuthConfig(clientId: String)

object OAuthConfig {
  implicit val reader: ConfigReader[OAuthConfig] = deriveReader

  val AuthName = "googleoauth"
  val AuthScopes = List("openid", "email", "profile")
}
