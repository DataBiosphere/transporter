package org.broadinstitute.transporter.kafka

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.collection.JavaConverters._

/** Configuration describing how a Transporter agent should set up Kafka streams. */
case class KStreamsConfig(applicationId: String, bootstrapServers: List[String]) {

  /**
    * Convert this config to a map mirroring the properties required by Kafka's API.
    *
    * Exposed for use by unit testing libs which try to helpfully perform the
    * Properties generation on their own.
    */
  private[kafka] def asMap: Map[String, String] = Map(
    StreamsConfig.APPLICATION_ID_CONFIG -> applicationId,
    StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers.mkString(","),
    StreamsConfig.PROCESSING_GUARANTEE_CONFIG -> StreamsConfig.EXACTLY_ONCE
  )

  /** Convert this config to the properties required by Kafka's Java API. */
  def asProperties: Properties = {
    val p = new Properties()
    p.putAll(asMap.asJava)
    p
  }
}

object KStreamsConfig {
  implicit val reader: ConfigReader[KStreamsConfig] = deriveReader
}
