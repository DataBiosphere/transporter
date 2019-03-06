package org.broadinstitute.transporter.kafka
import java.nio.ByteBuffer

import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe._
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder
import org.broadinstitute.transporter.queue.Queue
import org.broadinstitute.transporter.transfer.{
  TransferResult,
  TransferRunner,
  TransferStatus
}

/**
  * Kafka stream defining how the "request" and "response" topics
  * of a Transporter queue should be plugged together.
  */
object TransferStream {
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._

  /**
    * Construct a stream which, when started, will pull requests
    * from a Transporter queue, execute the requests using a runner,
    * then push the results back to the Transporter manager.
    *
    * @param config configuration for the underlying Kafka components
    *               running the stream
    * @param queue Transporter queue describing the Kafka topics to
    *              pull from / push into
    * @param runner component which can actually perform data transfer
    *
    * @tparam In expected model for messages pulled from the "request"
    *            topic of the given queue
    */
  def build[In: Decoder](
    config: KStreamsConfig,
    queue: Queue,
    runner: TransferRunner[In]
  ): IO[KafkaStreams] = {
    val builder = new StreamsBuilder()
    val jsonParser = new JawnParser()
    val logger = Slf4jLogger.getLogger[IO]

    /*
     * Kafka Streams supports deserializers for higher-level types than `Array[Byte]`,
     * but the interface for those serializers doesn't allow for transforming decoding
     * errors into output messages.
     *
     * Since we rely on the agent reporting a status for every message it processes,
     * we use the lowest-level deserializer at the stream entrypoint, then roll our own
     * decoding / error handling logic.
     */
    builder
      .stream[String, Array[Byte]](queue.requestTopic)
      .map { (id, requestBytes) =>
        val attemptTransfer = for {
          json <- jsonParser.parseByteBuffer(ByteBuffer.wrap(requestBytes)).liftTo[IO]
          request <- json.as[In].liftTo[IO]
          result <- IO.delay(runner.transfer(request))
        } yield {
          result
        }

        val recovered = attemptTransfer.handleErrorWith { err =>
          val (log, detail) = err match {
            case ParsingFailure(msg, _) =>
              ("Could not parse request as JSON", msg)
            case DecodingFailure(msg, _) =>
              ("Could not decode request to expected model", msg)
            case e =>
              ("Unhandled exception during transfer execution", e.getMessage)
          }
          val message = s"Hit exception processing request $id: $log"

          val resultInfo = JsonObject(
            "message" -> message.asJson,
            "detail" -> detail.asJson
          )

          logger
            .error(err)(log)
            .as(TransferResult(TransferStatus.FatalFailure, Some(resultInfo)))
        }

        id -> recovered.map(_.asJson.noSpaces.getBytes).unsafeRunSync()
      }
      .to(queue.responseTopic)

    for {
      _ <- logger.info(
        s"Building transfer stream from ${queue.requestTopic} to ${queue.responseTopic}"
      )
      topology <- IO.delay(builder.build())
      stream <- IO.delay(new KafkaStreams(topology, config.asJava))
    } yield {
      stream
    }
  }
}
