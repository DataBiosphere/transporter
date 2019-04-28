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
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.broadinstitute.transporter.queue.api.Queue
import org.broadinstitute.transporter.transfer.{
  TransferProgress,
  TransferRequest,
  TransferResult,
  TransferRunner,
  TransferSummary
}

class TransferStreamBuilder(queue: Queue) {
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import Serdes._
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
    val logger = org.log4s.getLogger(s"${queue.name}-transfer-init-logger")

    for {
      Array(failures, successes) <- IO.delay {
        builder
          .stream[Array[Byte], TransferRequest[Json]](queue.requestTopic)
          .mapValues { transfer =>
            logger.info(
              s"Received transfer ${transfer.id} from request ${transfer.requestId}"
            )

            val payload = transfer.transfer
            logger.debug(
              s"Attempting to parse payload for ${transfer.id} into expected model: ${payload.spaces2}"
            )

            val initializedOrError = for {
              input <- payload.as[In]
              _ = logger.info(
                s"Initializing transfer ${transfer.id} from request ${transfer.requestId}"
              )
              zeroProgress <- Either.catchNonFatal(runner.initialize(input))
            } yield {
              zeroProgress
            }

            initializedOrError.bimap(
              err => {
                val message =
                  s"Failed to initialize transfer ${transfer.id} from request ${transfer.requestId}"
                logger.error(err)(message)
                TransferSummary(
                  TransferResult.FatalFailure,
                  UnhandledErrorInfo(message, err.getMessage),
                  id = transfer.id,
                  requestId = transfer.requestId
                )
              },
              zero => {
                logger.info(s"Enqueueing zero progress for transfer ${transfer.id}")
                TransferProgress(
                  zero,
                  id = transfer.id,
                  requestId = transfer.requestId
                )
              }
            )
          }
          .branch((_, attempt) => attempt.isLeft, (_, attempt) => attempt.isRight)
      }
      _ <- IO.delay(failures.mapValues(_.left.get.asJson).to(queue.responseTopic))
      _ <- IO.delay(successes.mapValues(_.right.get.asJson).to(queue.progressTopic))
    } yield {
      builder
    }
  }

  private def buildStepStream[Progress: Encoder: Decoder, Out: Encoder](
    builder: StreamsBuilder,
    runner: TransferRunner[_, Progress, Out]
  ): IO[StreamsBuilder] = {
    val logger = org.log4s.getLogger(s"${queue.name}-transfer-step-logger")

    for {
      Array(progresses, summaries) <- IO.delay {
        builder
          .stream[Array[Byte], TransferProgress[Progress]](queue.progressTopic)
          .mapValues { progress =>
            logger.info(
              s"Received incremental progress for transfer ${progress.id} from request ${progress.requestId}"
            )

            Either
              .catchNonFatal(runner.step(progress.progress))
              .fold(
                err => {
                  val message =
                    s"Failed to run step on transfer ${progress.id} from request ${progress.requestId}"
                  logger.error(err)(message)
                  val errSummary = TransferSummary(
                    TransferResult.FatalFailure,
                    UnhandledErrorInfo(message, err.getMessage).asJson,
                    id = progress.id,
                    requestId = progress.requestId
                  )
                  Either.right(errSummary)
                },
                nextStepOrOutput => {
                  logger.info(
                    s"Successfully ran transfer step for ${progress.id} from request ${progress.requestId}"
                  )
                  nextStepOrOutput.bimap(
                    nextStep =>
                      TransferProgress(
                        nextStep,
                        id = progress.id,
                        requestId = progress.requestId
                      ),
                    output =>
                      TransferSummary(
                        TransferResult.Success,
                        output.asJson,
                        id = progress.id,
                        requestId = progress.requestId
                      )
                  )
                }
              )
          }
          .branch((_, attempt) => attempt.isLeft, (_, attempt) => attempt.isRight)
      }
      _ <- IO.delay(progresses.mapValues(_.left.get.asJson).to(queue.progressTopic))
      _ <- IO.delay(summaries.mapValues(_.right.get.asJson).to(queue.responseTopic))
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

  implicit def jsonSerde[A >: Null: Encoder: Decoder]: Serde[A] = Serdes.fromFn[A](
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
