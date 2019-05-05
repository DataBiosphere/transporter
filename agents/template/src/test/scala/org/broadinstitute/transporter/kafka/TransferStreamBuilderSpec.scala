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
  TransferIds,
  TransferMessage,
  TransferResult,
  TransferRunner
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
    LoopSchema.as[QueueSchema].right.value,
    2
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
  ): List[TransferMessage[(TransferResult, Json)]] = {
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

      implicit val decoder: ConsumerRecord[Array[Byte], String] => TransferMessage[
        (TransferResult, Json)
      ] =
        record =>
          parse(record.value())
            .flatMap(_.as[TransferMessage[(TransferResult, Json)]])
            .valueOr(throw _)

      val consumer = newConsumer[Array[Byte], String]

      val results = consumer
        .consumeLazily[TransferMessage[(TransferResult, Json)]](queue.responseTopic)
        .take(requests.length)
        .toList

      consumer.close()

      results
    }
  }

  private val ids = TransferIds(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID())

  behavior of "TransferStream"

  it should "receive requests, execute transfers, and push responses" in {
    val request = TransferMessage(ids, LoopRequest(3))
    val expectedResponse = TransferMessage(
      ids,
      (TransferResult.Success, LoopProgress(loopsSoFar = 3, loopsToGo = 0).asJson)
    )

    val results = roundTripTransfers(List(request.asJson))
    results shouldBe List(expectedResponse)
  }

  it should "interleave steps of transfer requests" in {
    val longRunningCount = 5
    val quickCount = 1

    val longRunning = TransferMessage(ids, LoopRequest(loopCount = longRunningCount))
    val quick = TransferMessage(
      ids.copy(transfer = UUID.randomUUID()),
      LoopRequest(loopCount = quickCount)
    )

    val messages = List(longRunning, quick)

    // The quick result should arrive before the long-running one.
    roundTripTransfers(messages.map(_.asJson)) shouldBe List(
      TransferMessage(
        quick.ids,
        (TransferResult.Success, LoopProgress(quickCount, 0).asJson)
      ),
      TransferMessage(
        longRunning.ids,
        (TransferResult.Success, LoopProgress(longRunningCount, 0).asJson)
      )
    )
  }

  it should "bail out if malformed request payloads end up on the request topic" in {
    val messages = List(
      "How did I get here???".asJson,
      TransferMessage(ids, LoopRequest(0)).asJson
    )

    roundTripTransfers(messages) shouldBe empty
  }

  it should "push error results if transfers with a bad schema ends up on the request topic" in {
    val badSchema = TransferMessage(
      ids,
      json"""{ "problem": "not the right schema" }"""
    )
    val goodSchema = TransferMessage(
      ids.copy(transfer = UUID.randomUUID()),
      LoopRequest(0).asJson
    )

    val List(result1, result2) =
      roundTripTransfers(List(badSchema, goodSchema).map(_.asJson))

    result1.ids shouldBe badSchema.ids
    val (res1, info1) = result1.message
    res1 shouldBe TransferResult.FatalFailure
    info1.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true

    result2 shouldBe TransferMessage(
      goodSchema.ids,
      (TransferResult.Success: TransferResult, LoopProgress(0, 0).asJson)
    )
  }

  it should "push error results if initializing a transfer fails" in {
    val messages = List.tabulate(2) { i =>
      TransferMessage(ids.copy(transfer = UUID.randomUUID()), LoopRequest(i)).asJson
    }
    val List(result1, result2) = roundTripTransfers(messages, failInit = true)

    result1.message._1 shouldBe TransferResult.FatalFailure
    result2.message._1 shouldBe TransferResult.FatalFailure

    List(result1.message._2, result2.message._2).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }

  it should "push error results if processing a transfer fails" in {
    val messages = List.tabulate(2) { i =>
      TransferMessage(ids.copy(transfer = UUID.randomUUID()), LoopRequest(i)).asJson
    }
    val List(result1, result2) = roundTripTransfers(messages, failStep = true)

    result1.message._1 shouldBe TransferResult.FatalFailure
    result2.message._1 shouldBe TransferResult.FatalFailure

    List(result1.message._2, result2.message._2).foreach { jsonInfo =>
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
