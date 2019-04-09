package org.broadinstitute.transporter.kafka

import java.nio.ByteBuffer

import cats.effect.IO
import cats.implicits._
import io.circe._
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.StreamsBuilder
import org.broadinstitute.transporter.queue.Queue
import org.broadinstitute.transporter.transfer.{
  TransferSummary,
  TransferRunner,
  TransferResult
}

class TransferStreamBuilder(queue: Queue) {
  import org.apache.kafka.streams.scala.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import TransferStreamBuilder._

  /**
    * Construct a stream topology which, when started, will pull requests
    * from a Transporter queue, execute the requests using a runner,
    * then push the results back to the Transporter manager.
    */
  def build(runner: TransferRunner): IO[Topology] = {
    val builder = new StreamsBuilder()

    for {
      withInitStream <- buildInitStream(builder, runner)
      withStepStream <- buildStepStream(withInitStream, runner)
      topology <- IO.delay(withStepStream.build())
    } yield {
      topology
    }
  }

  /*
   * Kafka Streams supports deserializers for higher-level types than `Array[Byte]`,
   * but the interface for those serializers doesn't allow for transforming decoding
   * errors into output messages.
   *
   * Since we rely on the agent reporting a status for every message it processes,
   * we use the lowest-level deserializer at the stream entrypoint, then roll our own
   * decoding / error handling logic.
   */

  private val parser = new JawnParser()

  private def buildInitStream(
    builder: StreamsBuilder,
    runner: TransferRunner
  ): IO[StreamsBuilder] = {
    val logger = org.log4s.getLogger(s"${queue.name}-transfer-init-logger")

    for {
      Array(failures, successes) <- IO.delay {
        builder
          .stream[String, Array[Byte]](queue.requestTopic)
          .mapValues { (id, requestBytes) =>
            logger.info(s"Received initial transfer request with ID: $id")

            for {
              json <- parser.parseByteBuffer(ByteBuffer.wrap(requestBytes))
              _ = logger.debug(
                s"Successfully parsed JSON for request $id: ${json.spaces2}"
              )
              request <- runner.decodeInput(json)
              zeroProgress <- Either.catchNonFatal(runner.initialize(request))
            } yield {
              zeroProgress
            }
          }
          .branch((_, attempt) => attempt.isLeft, (_, attempt) => attempt.isRight)
      }
      _ <- IO.delay {
        failures.mapValues { (id, wrappedFailure) =>
          val failure = wrappedFailure.left.get

          val (log, detail) = failure match {
            case p: ParsingFailure => (ParseFailureMsg, p.message)
            case e                 => (UnhandledErrMsg, e.getMessage)
          }
          logger.error(failure)(log)
          TransferSummary(
            TransferResult.FatalFailure,
            UnhandledErrorInfo(s"Failed to initialize request $id: $log", detail)
          ).asJson.noSpaces
        }.to(queue.responseTopic)
      }
      _ <- IO.delay {
        successes.mapValues { (id, s) =>
          logger.info(s"Enqueueing zero progress for request with ID: $id")
          runner.encodeProgress(s.right.get).noSpaces
        }.to(queue.progressTopic)
      }
    } yield {
      builder
    }
  }

  private def buildStepStream(
    builder: StreamsBuilder,
    runner: TransferRunner
  ): IO[StreamsBuilder] = {
    val logger = org.log4s.getLogger(s"${queue.name}-transfer-step-logger")

    for {
      Array(progresses, summaries) <- IO.delay {
        builder
          .stream[String, Array[Byte]](queue.progressTopic)
          .mapValues { (id, progressBytes) =>
            logger.info(s"Received incremental progress for ID: $id")

            val runStep = for {
              json <- parser.parseByteBuffer(ByteBuffer.wrap(progressBytes))
              _ = logger.debug(s"Successfully parsed progress $id: ${json.spaces2}")
              progress <- runner.decodeProgress(json)
              _ = logger.debug(s"Running next step of $id: ${json.spaces2}")
              stepResult <- Either.catchNonFatal(runner.step(progress))
              _ = logger.info(s"Successfully ran transfer step for request $id")
            } yield {
              stepResult
            }

            runStep.fold(
              failure => {
                val (log, detail) = failure match {
                  case p: ParsingFailure => (ParseFailureMsg, p.message)
                  case e                 => (UnhandledErrMsg, e.getMessage)
                }

                logger.error(failure)(log)

                Either.right(
                  TransferSummary(
                    TransferResult.FatalFailure,
                    UnhandledErrorInfo(
                      s"Failed to run step on request $id: $log",
                      detail
                    ).asJson
                  )
                )
              },
              _.map { out =>
                TransferSummary(TransferResult.Success, runner.encodeOutput(out))
              }
            )
          }
          .branch((_, attempt) => attempt.isLeft, (_, attempt) => attempt.isRight)
      }
      _ <- IO.delay {
        progresses
          .mapValues(p => runner.encodeProgress(p.left.get).noSpaces)
          .to(queue.progressTopic)
      }
      _ <- IO.delay {
        summaries.mapValues(_.right.get.asJson.noSpaces).to(queue.responseTopic)
      }
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

  /**
    * Error message returned to the manager when a non-JSON request is found
    * on the request topic of a Transporter queue.
    */
  private val ParseFailureMsg = "Could not parse request as JSON"

  /**
    * Error message returned to the manager when an exception is raised by
    * the Runner while trying to execute a received request.
    */
  private val UnhandledErrMsg = "Unhandled exception during transfer execution"

  /** Model used for capturing errors caught by the agent framework. */
  private[kafka] case class UnhandledErrorInfo(message: String, detail: String)
  private[kafka] implicit val encoder: Encoder[UnhandledErrorInfo] =
    io.circe.derivation.deriveEncoder
}
