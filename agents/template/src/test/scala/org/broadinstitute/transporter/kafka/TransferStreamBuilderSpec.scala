package org.broadinstitute.transporter.kafka

import java.net.ServerSocket

import cats.implicits._
import io.circe.{Decoder, Encoder, Json}
import io.circe.derivation.{deriveDecoder, deriveEncoder}
import io.circe.literal._
import io.circe.parser.parse
import io.circe.syntax._
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.broadinstitute.transporter.kafka.TransferStreamBuilder.UnhandledErrorInfo
import org.broadinstitute.transporter.queue.{Queue, QueueSchema}
import org.broadinstitute.transporter.transfer.{
  TransferResult,
  TransferRunner,
  TransferSummary
}
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class TransferStreamBuilderSpec
    extends FlatSpec
    with Matchers
    with EmbeddedKafkaStreamsAllInOne
    with EitherValues {

  import TransferStreamBuilderSpec._

  private val queue = Queue(
    "test-queue",
    "request-topic",
    "progress-topic",
    "response-topic",
    LoopSchema.as[QueueSchema].right.value
  )

  private val builder = new TransferStreamBuilder(queue)

  private def embeddedConfig = {
    // Find unused ports to avoid port clashes between tests.
    // This isn't rock-solid, but it's better than having a hard-coded port.
    val kafkaSocket = new ServerSocket(0)
    val zkSocket = new ServerSocket(0)

    val conf = EmbeddedKafkaConfig(
      kafkaPort = kafkaSocket.getLocalPort,
      zooKeeperPort = zkSocket.getLocalPort,
      customBrokerProperties = Map(
        // Needed to make exactly-once processing work in Kafka Streams
        // when there's only 1 broker in the cluster.
        "transaction.state.log.replication.factor" -> "1",
        "transaction.state.log.min.isr" -> "1"
      )
    )

    kafkaSocket.close()
    zkSocket.close()

    conf
  }

  /**
    * Run a set of requests through an instance of the "echo" stream,
    * and return the decoded results.
    */
  private def roundTripTransfers(
    requests: List[(String, String)],
    failInit: Boolean = false,
    failStep: Boolean = false
  ): List[(String, TransferSummary[Json])] = {
    implicit val baseConfig: EmbeddedKafkaConfig = embeddedConfig

    val topology = builder.build(new LoopRunner(failInit, failStep)).unsafeRunSync()
    val config = KStreamsConfig("test-app", List(s"localhost:${baseConfig.kafkaPort}"))

    runStreams(
      List(queue.requestTopic, queue.progressTopic, queue.responseTopic),
      topology,
      config.asMap
    ) {

      publishToKafka(queue.requestTopic, requests)

      // The default max attempts of 3 sometimes isn't enough on Jenkins.
      implicit val consumerConfig: ConsumerRetryConfig =
        ConsumerRetryConfig(maximumAttempts = 10)

      val consumer = newConsumer[String, String]

      val results = consumer
        .consumeLazily[(String, String)](queue.responseTopic)
        .take(requests.length)
        .toList

      consumer.close()

      results.traverse {
        case (k, v) =>
          parse(v).flatMap(_.as[TransferSummary[Json]]).map(k -> _)
      }.right.value
    }
  }

  behavior of "TransferStream"

  it should "receive requests, execute transfers, and push responses" in {
    val request = LoopRequest(3)
    val expectedResponse = LoopProgress(loopsSoFar = 3, loopsToGo = 0)

    val results = roundTripTransfers(List("message" -> request.asJson.noSpaces))
    results shouldBe List(
      "message" -> TransferSummary(TransferResult.Success, expectedResponse.asJson)
    )
  }

  it should "interleave steps of transfer requests" in {
    val longRunning = LoopRequest(loopCount = 5)
    val quick = LoopRequest(loopCount = 1)

    val messages = List(
      "long-running" -> longRunning.asJson.noSpaces,
      "quick" -> quick.asJson.noSpaces
    )

    // The quick result should arrive before the long-running one.
    roundTripTransfers(messages) shouldBe List(
      "quick" -> TransferSummary(
        TransferResult.Success,
        LoopProgress(quick.loopCount, 0).asJson
      ),
      "long-running" -> TransferSummary(
        TransferResult.Success,
        LoopProgress(longRunning.loopCount, 0).asJson
      )
    )
  }

  it should "push error results if non-JSON values end up on the request topic" in {
    val messages = List(
      "not-json" -> "How did I get here???",
      "ok" -> LoopRequest(0).asJson.noSpaces
    )

    val List((key1, result1), (key2, result2)) = roundTripTransfers(messages)

    key1 shouldBe "not-json"
    result1.result shouldBe TransferResult.FatalFailure
    result1.info.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true

    key2 shouldBe "ok"
    result2 shouldBe TransferSummary(TransferResult.Success, LoopProgress(0, 0).asJson)
  }

  it should "push error results if JSON with a bad schema ends up on the request topic" in {
    val messages = List(
      "wrong-json" -> """{ "problem": "not the right schema" }""",
      "ok" -> LoopRequest(0).asJson.noSpaces
    )

    val List((key1, result1), (key2, result2)) = roundTripTransfers(messages)

    key1 shouldBe "wrong-json"
    result1.result shouldBe TransferResult.FatalFailure
    result1.info.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true

    key2 shouldBe "ok"
    result2 shouldBe TransferSummary(TransferResult.Success, LoopProgress(0, 0).asJson)
  }

  it should "push error results if initializing a transfer fails" in {
    val messages = List(
      "boom1" -> LoopRequest(0).asJson.noSpaces,
      "boom2" -> LoopRequest(0).asJson.noSpaces
    )
    val List((key1, result1), (key2, result2)) =
      roundTripTransfers(messages, failInit = true)

    key1 shouldBe "boom1"
    key2 shouldBe "boom2"

    result1.result shouldBe TransferResult.FatalFailure
    result2.result shouldBe TransferResult.FatalFailure

    List(result1.info, result2.info).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }

  it should "push error results if processing a transfer fails" in {
    val messages = List(
      "boom1" -> LoopRequest(0).asJson.noSpaces,
      "boom2" -> LoopRequest(0).asJson.noSpaces
    )
    val List((key1, result1), (key2, result2)) =
      roundTripTransfers(messages, failStep = true)

    key1 shouldBe "boom1"
    key2 shouldBe "boom2"

    result1.result shouldBe TransferResult.FatalFailure
    result2.result shouldBe TransferResult.FatalFailure

    List(result1.info, result2.info).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }
}

object TransferStreamBuilderSpec {

  val UnhandledError = new RuntimeException("OH NO")

  case class LoopRequest(loopCount: Int)
  case class LoopProgress(loopsSoFar: Int, loopsToGo: Int)

  implicit val reqDecoder: Decoder[LoopRequest] = deriveDecoder
  implicit val reqEncoder: Encoder[LoopRequest] = deriveEncoder

  implicit val progressDecoder: Decoder[LoopProgress] = deriveDecoder
  implicit val progressEncoder: Encoder[LoopProgress] = deriveEncoder

  class LoopRunner(failInit: Boolean, failStep: Boolean) extends TransferRunner {

    override type In = LoopRequest
    override type Progress = Out
    override type Out = LoopProgress

    override def decodeInput(json: Json): Either[Throwable, LoopRequest] =
      json.as[LoopRequest]

    override def encodeProgress(progress: LoopProgress): Json = progress.asJson

    override def decodeProgress(json: Json): Either[Throwable, LoopProgress] =
      json.as[LoopProgress]

    override def encodeOutput(output: LoopProgress): Json = output.asJson

    override def initialize(request: LoopRequest): LoopProgress =
      if (failInit) {
        throw UnhandledError
      } else {
        LoopProgress(0, request.loopCount)
      }

    override def step(progress: LoopProgress): Either[LoopProgress, LoopProgress] =
      if (failStep) {
        throw UnhandledError
      } else {
        Either.cond(
          progress.loopsToGo == 0,
          progress,
          progress.copy(
            loopsSoFar = progress.loopsSoFar + 1,
            loopsToGo = progress.loopsToGo - 1
          )
        )
      }
  }

  val LoopSchema = json"""{
    "type": "object",
    "properties": { "loopCount": { "type": "integer" } },
    "additionalProperties": false
  }"""

  implicit val resDecoder: Decoder[UnhandledErrorInfo] = deriveDecoder
}
