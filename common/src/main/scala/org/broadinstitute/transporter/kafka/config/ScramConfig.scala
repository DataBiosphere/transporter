package org.broadinstitute.transporter.kafka.config

import org.apache.kafka.common.config.SaslConfigs
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Config determining how clients should authenticate to Kafka brokers using SCRAM-SHA-256. */
case class ScramConfig(username: String, password: String) {

  /** Convert this config to a map containing the properties required by Kafka's API. */
  def asMap: Map[String, String] = Map(
    SaslConfigs.SASL_MECHANISM -> "SCRAM-SHA-256",
    SaslConfigs.SASL_JAAS_CONFIG -> List(
      "org.apache.kafka.common.security.scram.ScramLoginModule",
      "required",
      s"username=$username",
      s"password=$password"
    ).mkString(" ")
  )
}

object ScramConfig {
  implicit val reader: ConfigReader[ScramConfig] = deriveReader
}
