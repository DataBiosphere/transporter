package org.broadinstitute.transporter.kafka

import cats.effect.{ContextShift, IO, Resource}
import fs2.kafka.{AdminClientSettings, KafkaAdminClient}

class KafkaClient private[kafka] (adminClient: KafkaAdminClient[IO]) {

  def checkReady: IO[Boolean] =
    adminClient.listTopics.names.attempt.map(_.fold(_ => false, _ => true))
}

object KafkaClient {

  def resource(config: KafkaConfig)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaClient] = {
    val settings = AdminClientSettings.Default
      .withBootstrapServers(config.bootstrapServers.mkString(","))
      .withClientId(config.clientId)

    fs2.kafka
      .adminClientResource[IO](settings)
      .map(new KafkaClient(_))
  }
}
