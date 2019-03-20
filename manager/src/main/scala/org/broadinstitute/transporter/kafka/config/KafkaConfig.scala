package org.broadinstitute.transporter.kafka.config

import cats.data.NonEmptyList
import cats.implicits._
import fs2.kafka._
import pureconfig.ConfigReader
import pureconfig.generic.semiauto._

import scala.concurrent.ExecutionContext

/** Configuration determining how Transporter should interact with its backing Kafka cluster. */
case class KafkaConfig(
  bootstrapServers: NonEmptyList[String],
  clientId: String,
  batchParams: ConsumerBatchConfig,
  topicDefaults: TopicConfig,
  timeouts: TimeoutConfig
) {

  // Underlying Kafka libs want everything as a string...
  private lazy val bootstrapString = bootstrapServers.mkString_(",")

  /** Convert this config into settings for a [[org.apache.kafka.clients.admin.AdminClient]]. */
  def adminSettings: AdminClientSettings =
    AdminClientSettings.Default
    // Required to connect to Kafka at all.
      .withBootstrapServers(bootstrapString)
      // For debugging on the Kafka server; adds an ID to the logs.
      .withClientId(clientId)
      // No "official" recommendation on these values, we can tweak as we see fit.
      .withRequestTimeout(timeouts.requestTimeout)
      .withCloseTimeout(timeouts.closeTimeout)

  /**
    * Convert this config into settings for a [[org.apache.kafka.clients.producer.Producer]].
    *
    * Some settings in the output are hard-coded to prevent silent data loss in the producer,
    * which isn't acceptable for our use-case.
    */
  def producerSettings[K, V](
    implicit
    keySerializer: Serializer[K],
    valueSerializer: Serializer[V]
  ): ProducerSettings[K, V] =
    ProducerSettings(keySerializer, valueSerializer)
    // Required to connect to Kafka at all.
      .withBootstrapServers(bootstrapString)
      // For debugging on the Kafka server; adds an ID to the logs.
      .withClientId(clientId)
      // Recommended for apps where it's not acceptable to lose messages.
      .withRetries(Int.MaxValue)
      // Recommended for apps where it's not acceptable to double-send messages.
      .withEnableIdempotence(true)
      // No "official" recommendation on these values, we can tweak as we see fit.
      .withRequestTimeout(timeouts.requestTimeout)
      .withCloseTimeout(timeouts.closeTimeout)

  /**
    * Convert this config into settings for a [[org.apache.kafka.clients.consumer.Consumer]].
    *
    * Some settings in the output are hard-coded to prevent silent data loss in the consumer,
    * which isn't acceptable for our use-case.
    *
    * @param actorEc single-threaded pool which can be taken over to run polling ops against
    *                the Kafka cluster. NOTE: If a pool with > 1 thread is given, the Kafka
    *                libs will detect the possibility of concurrent modification and throw an
    *                exception.
    */
  def consumerSettings[K, V](actorEc: ExecutionContext)(
    implicit
    keyDeserializer: Deserializer[K],
    valueDeserializer: Deserializer[V]
  ): ConsumerSettings[K, V] =
    ConsumerSettings[K, V](keyDeserializer, valueDeserializer, actorEc)
    // Required to connect to Kafka at all.
      .withBootstrapServers(bootstrapString)
      /*
       * Required to be the same across:
       *   1. All running instances of the Transporter manager, to avoid processing a single
       *      message multiple times in parallel
       *   2. All deployments of a single instance of the manager, to avoid reprocessing the
       *      entire history of messages on each reboot.
       */
      .withGroupId(KafkaConfig.ManagerGroup)
      // For debugging on the Kafka server; adds an ID to the logs.
      .withClientId(clientId)
      // Force manual commits to avoid accidental data loss.
      .withEnableAutoCommit(false)
      /*
       * When subscribing to a topic for the first time, start from the earliest message
       * instead of the latest. Prevents accidentally losing data if an agent somehow writes
       * to a result topic before the manager notices & subscribes to it.
       */
      .withAutoOffsetReset(AutoOffsetReset.Earliest)
      // Sets the frequency at which the Kafka libs re-query the cluster to discover new topics.
      .withProperty(
        "metadata.max.age.ms",
        timeouts.topicDiscoveryInterval.toMillis.toString
      )
      // No "official" recommendation on these values, we can tweak as we see fit.
      .withRequestTimeout(timeouts.requestTimeout)
      .withCloseTimeout(timeouts.closeTimeout)
}

object KafkaConfig {
  // Don't listen to IntelliJ; needed for deriving the NonEmptyList reader.
  import pureconfig.module.cats._

  /**
    * Consumer group ID to use in every instance of the Transporter manager.
    *
    * Having a consistent group ID prevents double-processing of data, so we hard-code it here.
    */
  val ManagerGroup = "transporter-manager"

  val RequestTopicPrefix = "transporter.requests."

  val ResponseTopicPrefix = "transporter.responses."

  implicit val reader: ConfigReader[KafkaConfig] = deriveReader
}
