package org.broadinstitute.transporter.kafka

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

/** Configuration describing how a Transporter agent should set up Kafka streams. */
case class KStreamsConfig(applicationId: String, bootstrapServers: List[String]) {

  /** Convert this config to the properties required by Kafka's Java API. */
  def asJava: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers.mkString(","))
    p
  }
}

object KStreamsConfig {
  implicit val reader: ConfigReader[KStreamsConfig] = deriveReader
}
