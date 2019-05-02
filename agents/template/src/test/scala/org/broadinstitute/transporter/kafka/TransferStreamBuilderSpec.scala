package org.broadinstitute.transporter.kafka

import java.net.ServerSocket
import java.util.UUID

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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.broadinstitute.transporter.kafka.TransferStreamBuilder.UnhandledErrorInfo
import org.broadinstitute.transporter.queue.QueueSchema
import org.broadinstitute.transporter.queue.api.Queue
import org.broadinstitute.transporter.transfer.{
  TransferRequest,
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
    requests: List[Json],
    failInit: Boolean = false,
    failStep: Boolean = false
  ): List[TransferSummary[Json]] = {
    implicit val baseConfig: EmbeddedKafkaConfig = embeddedConfig

    val topology = builder.build(new LoopRunner(failInit, failStep)).unsafeRunSync()
    val config = KStreamsConfig("test-app", List(s"localhost:${baseConfig.kafkaPort}"))

    runStreams(
      List(queue.requestTopic, queue.progressTopic, queue.responseTopic),
      topology,
      config.asMap
    ) {

      publishToKafka(
        queue.requestTopic,
        requests.map((null: Array[Byte]) -> _.asJson.noSpaces)
      )

      // The default max attempts of 3 sometimes isn't enough on Jenkins.
      implicit val consumerConfig: ConsumerRetryConfig =
        ConsumerRetryConfig(maximumAttempts = 10)

      implicit val decoder: ConsumerRecord[Array[Byte], String] => TransferSummary[Json] =
        record =>
          parse(record.value()).flatMap(_.as[TransferSummary[Json]]).valueOr(throw _)

      val consumer = newConsumer[Array[Byte], String]

      val results = consumer
        .consumeLazily[TransferSummary[Json]](queue.responseTopic)
        .take(requests.length)
        .toList

      consumer.close()

      results
    }
  }

  behavior of "TransferStream"

  it should "receive requests, execute transfers, and push responses" in {
    val request = TransferRequest(
      LoopRequest(3),
      id = UUID.randomUUID(),
      requestId = UUID.randomUUID()
    )
    val expectedResponse = TransferSummary(
      TransferResult.Success,
      LoopProgress(loopsSoFar = 3, loopsToGo = 0).asJson,
      request.id,
      request.requestId
    )

    val results = roundTripTransfers(List(request.asJson))
    results shouldBe List(expectedResponse)
  }

  it should "interleave steps of transfer requests" in {
    val longRunningCount = 5
    val quickCount = 1

    val longRunning = TransferRequest(
      LoopRequest(loopCount = longRunningCount),
      id = UUID.randomUUID(),
      requestId = UUID.randomUUID()
    )
    val quick = TransferRequest(
      LoopRequest(loopCount = quickCount),
      id = UUID.randomUUID(),
      requestId = longRunning.requestId
    )

    val messages = List(longRunning, quick)

    // The quick result should arrive before the long-running one.
    roundTripTransfers(messages.map(_.asJson)) shouldBe List(
      TransferSummary(
        TransferResult.Success,
        LoopProgress(quickCount, 0).asJson,
        quick.id,
        quick.requestId
      ),
      TransferSummary(
        TransferResult.Success,
        LoopProgress(longRunningCount, 0).asJson,
        longRunning.id,
        longRunning.requestId
      )
    )
  }

  it should "bail out if malformed request payloads end up on the request topic" in {
    val messages = List(
      "How did I get here???".asJson,
      TransferRequest(
        LoopRequest(0),
        id = UUID.randomUUID(),
        requestId = UUID.randomUUID()
      ).asJson
    )

    roundTripTransfers(messages) shouldBe empty
  }

  it should "push error results if transfers with a bad schema ends up on the request topic" in {
    val badSchema = TransferRequest(
      json"""{ "problem": "not the right schema" }""",
      id = UUID.randomUUID(),
      requestId = UUID.randomUUID()
    )
    val goodSchema = TransferRequest(
      LoopRequest(0).asJson,
      id = UUID.randomUUID(),
      requestId = UUID.randomUUID()
    )

    val List(result1, result2) =
      roundTripTransfers(List(badSchema, goodSchema).map(_.asJson))

    result1.result shouldBe TransferResult.FatalFailure
    result1.info.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    result1.id shouldBe badSchema.id
    result1.requestId shouldBe badSchema.requestId

    result2 shouldBe TransferSummary(
      TransferResult.Success,
      LoopProgress(0, 0).asJson,
      goodSchema.id,
      goodSchema.requestId
    )
  }

  it should "push error results if initializing a transfer fails" in {
    val messages = List.tabulate(2) { i =>
      TransferRequest(LoopRequest(i), UUID.randomUUID(), UUID.randomUUID()).asJson
    }
    val List(result1, result2) = roundTripTransfers(messages, failInit = true)

    result1.result shouldBe TransferResult.FatalFailure
    result2.result shouldBe TransferResult.FatalFailure

    List(result1.info, result2.info).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }

  it should "push error results if processing a transfer fails" in {
    val messages = List.tabulate(2) { i =>
      TransferRequest(LoopRequest(i), UUID.randomUUID(), UUID.randomUUID()).asJson
    }
    val List(result1, result2) = roundTripTransfers(messages, failStep = true)

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

  class LoopRunner(failInit: Boolean, failStep: Boolean)
      extends TransferRunner[LoopRequest, LoopProgress, LoopProgress] {

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
