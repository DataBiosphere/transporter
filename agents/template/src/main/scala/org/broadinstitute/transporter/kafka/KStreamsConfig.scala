package org.broadinstitute.transporter.kafka

import java.util.Properties

import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler
import org.broadinstitute.transporter.kafka.config.{TlsConfig, TopicConfig}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader

import scala.collection.JavaConverters._

/** Configuration describing how a Transporter agent should set up Kafka streams. */
case class KStreamsConfig(
  applicationId: String,
  bootstrapServers: List[String],
  topics: TopicConfig,
  tls: Option[TlsConfig]
) {

  /**
    * Convert this config to a map containing the properties required by Kafka's API.
    *
    * Exposed for use by unit testing libs which try to helpfully perform the
    * Properties generation on their own.
    */
  private[kafka] def asMap: Map[String, String] = {
    val base = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> applicationId,
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers.mkString(","),
      StreamsConfig.PROCESSING_GUARANTEE_CONFIG -> StreamsConfig.EXACTLY_ONCE,
      /*
       * Kafka comes with two built-in mechanisms for handling messages of an unexpected
       * shape in stream input: skip the malformed input, or halt processing with an error.
       * We never want to lose track of a submitted transfer, so we use the halt-on-error
       * approach to force us to deal with schema mismatches.
       */
      StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG -> classOf[
        LogAndFailExceptionHandler
      ].getName
    )

    tls.fold(base)(_.asMap ++ base)
  }

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
