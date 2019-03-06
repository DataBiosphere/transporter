package org.broadinstitute.transporter.kafka

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

case class KStreamsConfig(applicationId: String, bootstrapServers: List[String]) {

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
