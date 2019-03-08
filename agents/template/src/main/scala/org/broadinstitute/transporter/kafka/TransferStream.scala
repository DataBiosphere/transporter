package org.broadinstitute.transporter.kafka
import java.nio.ByteBuffer

import cats.effect.IO
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import io.circe._
import io.circe.jawn.JawnParser
import io.circe.syntax._
import org.apache.kafka.streams.Topology
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

  private[kafka] val ParseFailureMsg = "Could not parse request as JSON"

  private[kafka] val DecodeFailureMsg = "Could not decode request to expected model"

  private[kafka] val UnhandledErrMsg = "Unhandled exception during transfer execution"

  private[kafka] case class UnhandledErrorInfo(message: String, detail: String)
  private[kafka] implicit val encoder: Encoder[UnhandledErrorInfo] =
    io.circe.derivation.deriveEncoder

  /**
    * Construct a stream which, when started, will pull requests
    * from a Transporter queue, execute the requests using a runner,
    * then push the results back to the Transporter manager.
    *
    * @param queue Transporter queue describing the Kafka topics to
    *              pull from / push into
    * @param runner component which can actually perform data transfer
    * @tparam Req expected model for messages pulled from the "request"
    *             topic of the given queue
    */
  def build[Req: Decoder](
    queue: Queue,
    runner: TransferRunner[Req]
  ): Topology = {
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
          jsonString = json.spaces2
          _ <- logger.info(s"Received request: $jsonString")
          request <- json.as[Req].liftTo[IO]
          _ <- logger.debug(s"Processing request: $jsonString")
          result <- IO.delay(runner.transfer(request))
          _ <- logger.info(s"Successfully processed request: $jsonString")
        } yield {
          result
        }

        val recovered = attemptTransfer.handleErrorWith { err =>
          val (log, detail) = err match {
            case p: ParsingFailure  => (ParseFailureMsg, p.message)
            case d: DecodingFailure => (DecodeFailureMsg, d.message)
            case e                  => (UnhandledErrMsg, e.getMessage)
          }
          val message = s"Failed to process request $id: $log"
          val resultInfo = UnhandledErrorInfo(message, detail)

          logger
            .error(err)(log)
            .as(TransferResult(TransferStatus.FatalFailure, Some(resultInfo.asJson)))
        }

        id -> recovered.map(_.asJson.noSpaces.getBytes).unsafeRunSync()
      }
      .to(queue.responseTopic)

    builder.build()
  }
}
