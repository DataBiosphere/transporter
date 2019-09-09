package org.broadinstitute.transporter.kafka

import java.nio.ByteBuffer

import cats.effect.IO
import cats.implicits._
import io.circe._
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.{StreamsBuilder, Serdes => KSerdes}
import org.broadinstitute.transporter.kafka.config.TopicConfig
import org.broadinstitute.transporter.transfer.{TransferMessage, TransferRunner}

class TransferStreamBuilder[I: Decoder, P: Encoder: Decoder, O: Encoder](
  topics: TopicConfig,
  runner: TransferRunner[I, P, O]
) {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import KSerdes._
  import TransferStreamBuilder._

  /**
    * Construct a stream topology which, when started, will listen for & perform
    * transfer requests sent by the Transporter Manager.
    */
  def build: IO[Topology] = {
    val builder = new StreamsBuilder()

    for {
      withInitStream <- buildInitStream(builder)
      withStepStream <- buildStepStream(withInitStream)
      topology <- IO.delay(withStepStream.build())
    } yield {
      topology
    }
  }

  /**
    * Add a stream topology to the given builder which, when started, will
    * pull requests from the configured "requests" topic, initializing each.
    *
    * The results of successful initialization will be pushed onto the configured
    * "progress" topic. If initialization raises an exception or signals that no
    * transfer is needed, a payload will be pushed onto the configured "results"
    * topic instead.
    */
  private def buildInitStream(builder: StreamsBuilder): IO[StreamsBuilder] = {
    val logger = org.log4s.getLogger("transfer-init-logger")

    for {
      Array(zeros, earlyExits) <- IO.delay {
        builder
          .stream[Array[Byte], TransferMessage[Json]](topics.requestTopic)
          .mapValues { transfer =>
            val ids = transfer.ids
            val payload = transfer.message

            logger.info(
              s"Received transfer ${ids.transfer} from request ${ids.request}"
            )

            logger.debug(
              s"Attempting to parse payload for ${ids.transfer} into expected model: ${payload.spaces2}"
            )

            val initializedOrError = for {
              input <- payload.as[I]
              _ = logger.info(
                s"Initializing transfer ${ids.transfer} from request ${ids.request}"
              )
              initialized <- Either.catchNonFatal(runner.initialize(input))
            } yield {
              initialized
            }

            ids -> initializedOrError.fold(
              err => Failure(err),
              identity
            )
          }
          .branch((_, attempt) => !attempt._2.isDone, (_, attempt) => attempt._2.isDone)
      }
      _ <- IO.delay(
        zeros
          .mapValues(notDone => TransferMessage(notDone._1, (0, notDone._2.message)))
          .to(topics.progressTopic)
      )
      _ <- IO.delay(
        earlyExits
          .mapValues(done => TransferMessage(done._1, done._2.message))
          .to(topics.resultTopic)
      )
    } yield {
      builder
    }
  }

  /**
    * Add a stream topology to the given builder which, when started, will
    * pull requests from the configured "progress" topic, transferring a chunk
    * of data for each.
    *
    * The results of each step will be pushed onto either the "progress" topic or
    * the "result" topic, depending on how the agent reports the output. Exceptions
    * will also be caught and pushed onto the "result" topic.
    */
  private def buildStepStream(builder: StreamsBuilder): IO[StreamsBuilder] = {
    val logger = org.log4s.getLogger("transfer-step-logger")

    for {
      Array(progresses, summaries) <- IO.delay {
        builder
          .stream[Array[Byte], TransferMessage[(Int, Json)]](topics.progressTopic)
          .mapValues { progress =>
            val ids = progress.ids

            logger.info(
              s"Received incremental progress for transfer ${ids.transfer} from request ${ids.request}"
            )

            val (stepsSoFar, message) = progress.message
            val theMessage = message
              .as[P]
              .flatMap(parsed => Either.catchNonFatal(runner.step(parsed)))
              .fold(
                err => Failure(err),
                identity
              )

            (ids, stepsSoFar + 1, theMessage)
          }
          .branch((_, attempt) => !attempt._3.isDone, (_, attempt) => attempt._3.isDone)
      }
      _ <- IO.delay(
        progresses
          .mapValues(
            notDone => TransferMessage(notDone._1, (notDone._2, notDone._3.message))
          )
          .to(topics.progressTopic)
      )
      _ <- IO.delay(
        summaries
          .mapValues(done => TransferMessage(done._1, done._3.message))
          .to(topics.resultTopic)
      )
    } yield {
      builder
    }
  }
}

object TransferStreamBuilder {

  private[this] val parser = new JawnParser()

  /**
    * Kafka serde for any type that can be converted to/from JSON.
    *
    * Not the most efficient way to transmit data, but lets us lean on
    * the encoding / decoding logic we're already writing.
    */
  implicit def jsonSerde[A >: Null: Encoder: Decoder]: Serde[A] = KSerdes.fromFn[A](
    (message: A) => message.asJson.noSpaces.getBytes,
    (bytes: Array[Byte]) =>
      parser
        .decodeByteBuffer[A](ByteBuffer.wrap(bytes))
        .fold(
          err => throw new SerializationException(err),
          Some(_)
        )
  )

  /** Model used for capturing errors caught by the agent framework. */
  private[kafka] case class UnhandledErrorInfo(message: String, detail: Option[String])
  private[kafka] implicit val encoder: Encoder[UnhandledErrorInfo] =
    io.circe.derivation.deriveEncoder
}
