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

class TransferStreamBuilder(topics: TopicConfig) {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import KSerdes._
  import TransferStreamBuilder._

  /**
    * Construct a stream topology which, when started, will pull requests
    * from a Transporter queue, execute the requests using a runner,
    * then push the results back to the Transporter manager.
    */
  def build[In: Decoder, Progress: Encoder: Decoder, Out: Encoder](
    runner: TransferRunner[In, Progress, Out]
  ): IO[Topology] = {
    val builder = new StreamsBuilder()

    for {
      withInitStream <- buildInitStream(builder, runner)
      withStepStream <- buildStepStream(withInitStream, runner)
      topology <- IO.delay(withStepStream.build())
    } yield {
      topology
    }
  }

  private def buildInitStream[In: Decoder, Progress: Encoder](
    builder: StreamsBuilder,
    runner: TransferRunner[In, Progress, _]
  ): IO[StreamsBuilder] = {
    val logger = org.log4s.getLogger("transfer-init-logger")

    for {
      Array(failures, successes) <- IO.delay {
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
              zeroProgress <- Either.catchNonFatal(runner.initialize(input))
            } yield {
              zeroProgress
            }

            initializedOrError.bimap(
              err => {
                val message =
                  s"Failed to initialize transfer ${ids.transfer} from request ${ids.request}"
                logger.error(err)(message)
                TransferMessage(
                  ids,
                  (
                    TransferResult.FatalFailure: TransferResult,
                    UnhandledErrorInfo(message, err.getMessage)
                  ).asJson
                )
              },
              zero => {
                logger.info(s"Enqueueing zero progress for transfer ${ids.transfer}")
                TransferMessage(ids, zero.asJson)
              }
            )
          }
          .branch((_, attempt) => attempt.isLeft, (_, attempt) => attempt.isRight)
      }
      _ <- IO.delay(failures.mapValues(_.left.get.asJson).to(topics.resultTopic))
      _ <- IO.delay(successes.mapValues(_.right.get.asJson).to(topics.progressTopic))
    } yield {
      builder
    }
  }

  private def buildStepStream[Progress: Encoder: Decoder, Out: Encoder](
    builder: StreamsBuilder,
    runner: TransferRunner[_, Progress, Out]
  ): IO[StreamsBuilder] = {
    val logger = org.log4s.getLogger("transfer-step-logger")

    for {
      Array(progresses, summaries) <- IO.delay {
        builder
          .stream[Array[Byte], TransferMessage[Progress]](topics.progressTopic)
          .mapValues { progress =>
            val ids = progress.ids

            logger.info(
              s"Received incremental progress for transfer ${ids.transfer} from request ${ids.request}"
            )

            Either
              .catchNonFatal(runner.step(progress.message))
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
                    nextStep => TransferMessage(ids, nextStep.asJson),
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

/**
  * Kafka stream defining how the "request" and "response" topics
  * of a Transporter queue should be plugged together.
  */
object TransferStreamBuilder {

  private[this] val parser = new JawnParser()

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
