package org.broadinstitute.transporter.info

import cats.effect.IO
import org.apache.kafka.streams.KafkaStreams

/**
  * Component responsible for handling status checks.
  *
  * @param streamApp the Kafka Streams app executing the agent's
  *                  transfer logic
  */
class InfoController(streamApp: KafkaStreams) {

  def status: IO[AgentStatus] =
    streamStatus.map(stream => AgentStatus(stream.ok, Map("kafka" -> stream)))

  private def streamStatus: IO[SystemStatus] = IO.delay {
    val state = streamApp.state()
    SystemStatus(state.isRunning, List(s"Kafka Stream in state: ${state.toString}"))
  }
}
