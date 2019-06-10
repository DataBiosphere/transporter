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
import org.broadinstitute.transporter.transfer.{
  TransferMessage,
  TransferResult,
  TransferRunner
}

class TransferStreamBuilder[In: Decoder, Progress: Encoder: Decoder, Out: Encoder](
  topics: TopicConfig,
  runner: TransferRunner[In, Progress, Out]
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
              input <- payload.as[In]
              _ = logger.info(
                s"Initializing transfer ${ids.transfer} from request ${ids.request}"
              )
              initialized <- Either.catchNonFatal(runner.initialize(input))
            } yield {
              initialized
            }

            initializedOrError.fold(
              err => {
                val message =
                  s"Failed to initialize transfer ${ids.transfer} from request ${ids.request}"
                logger.error(err)(message)
                Right {
                  TransferMessage(
                    ids,
                    (
                      TransferResult.FatalFailure: TransferResult,
                      UnhandledErrorInfo(message, err.getMessage)
                    ).asJson
                  )
                }
              },
              initialized => {
                initialized.bimap(
                  zero => {
                    logger.info(s"Enqueueing zero progress for transfer ${ids.transfer}")
                    TransferMessage(ids, (0, zero.asJson))
                  },
                  earlyExit => {
                    logger.info(s"Returning early for transfer ${ids.transfer}")
                    TransferMessage(
                      ids,
                      (TransferResult.Success: TransferResult, earlyExit).asJson
                    )
                  }
                )

              }
            )
          }
          .branch((_, attempt) => attempt.isLeft, (_, attempt) => attempt.isRight)
      }
      _ <- IO.delay(zeros.mapValues(_.left.get.asJson).to(topics.progressTopic))
      _ <- IO.delay(earlyExits.mapValues(_.right.get.asJson).to(topics.resultTopic))
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

            message
              .as[Progress]
              .flatMap(parsed => Either.catchNonFatal(runner.step(parsed)))
              .fold(
                err => {
                  val message =
                    s"Failed to run step on transfer ${ids.transfer} from request ${ids.request}"
                  logger.error(err)(message)
                  val errSummary = TransferMessage(
                    ids,
                    (
                      TransferResult.FatalFailure: TransferResult,
                      UnhandledErrorInfo(message, err.getMessage)
                    ).asJson
                  )
                  Either.right(errSummary)
                },
                nextStepOrOutput => {
                  logger.info(
                    s"Successfully ran transfer step for ${ids.transfer} from request ${ids.request}"
                  )
                  nextStepOrOutput.bimap(
                    nextStep => TransferMessage(ids, (stepsSoFar + 1) -> nextStep.asJson),
                    output =>
                      TransferMessage(
                        ids,
                        (TransferResult.Success: TransferResult, output).asJson
                      )
                  )
                }
              )
          }
          .branch((_, attempt) => attempt.isLeft, (_, attempt) => attempt.isRight)
      }
      _ <- IO.delay(progresses.mapValues(_.left.get.asJson).to(topics.progressTopic))
      _ <- IO.delay(summaries.mapValues(_.right.get.asJson).to(topics.resultTopic))
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
  private[kafka] case class UnhandledErrorInfo(message: String, detail: String)
  private[kafka] implicit val encoder: Encoder[UnhandledErrorInfo] =
    io.circe.derivation.deriveEncoder
}
