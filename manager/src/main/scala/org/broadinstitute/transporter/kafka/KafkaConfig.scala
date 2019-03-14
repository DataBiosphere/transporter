package org.broadinstitute.transporter.kafka

import fs2.kafka._
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

/** Configuration determining how Transporter should interact with its backing Kafka cluster. */
case class KafkaConfig(
  bootstrapServers: List[String],
  clientId: String,
  topicDefaults: TopicConfig,
  timeouts: TimeoutConfig
) {

  /** Convert this config into settings for a [[org.apache.kafka.clients.admin.AdminClient]]. */
  def adminSettings: AdminClientSettings =
    AdminClientSettings.Default
      .withBootstrapServers(bootstrapServers.mkString(","))
      .withClientId(clientId)
      .withRequestTimeout(timeouts.requestTimeout)
      .withCloseTimeout(timeouts.closeTimeout)

  /**
    * Convert this config into settings for a [[org.apache.kafka.clients.producer.Producer]].
    *
    * Some settings in the output are hard-coded to prevent silent data loss in the producer,
    * which isn't acceptable for our use-case.
    */
  def producerSettings[K, V](
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] =
    ProducerSettings(keySerializer, valueSerializer)
      .withBootstrapServers(bootstrapServers.mkString(","))
      .withClientId(clientId)
      .withRetries(Int.MaxValue)
      .withEnableIdempotence(true)
      .withRequestTimeout(timeouts.requestTimeout)
      .withCloseTimeout(timeouts.closeTimeout)
}

object KafkaConfig {
  implicit val reader: ConfigReader[KafkaConfig] = deriveReader
}
