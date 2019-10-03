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
import org.broadinstitute.transporter.kafka.config.TopicConfig
import org.broadinstitute.transporter.transfer.{
  TransferIds,
  TransferMessage,
  TransferResult,
  TransferRunner
}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

class TransferStreamBuilderSpec
    extends FlatSpec
    with Matchers
    with EmbeddedKafkaStreamsAllInOne
    with MockFactory {

  import TransferStreamBuilderSpec._

  private val topics = TopicConfig(
    "request-topic",
    "progress-topic",
    "result-topic"
  )

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
    runner: LoopRunner
  ): List[TransferMessage[(TransferResult, Json)]] = {
    implicit val baseConfig: EmbeddedKafkaConfig = embeddedConfig

    val builder = new TransferStreamBuilder(topics, runner)
    val topology = builder.build.unsafeRunSync()
    val config = KStreamsConfig(
      "test-app",
      List(s"localhost:${baseConfig.kafkaPort}"),
      None,
      topics,
      tls = None,
      scram = None
    )

    runStreams(
      List(topics.requestTopic, topics.progressTopic, topics.resultTopic),
      topology,
      config.asMap
    ) {

      publishToKafka(
        topics.requestTopic,
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
        .consumeLazily[TransferMessage[(TransferResult, Json)]](topics.resultTopic)
        .take(requests.length)
        .toList

      consumer.close()

      results
    }
  }

  private val ids = TransferIds(UUID.randomUUID(), UUID.randomUUID())

  behavior of "TransferStream"

  it should "receive requests, execute transfers, and push responses" in {
    val request = TransferMessage(ids, LoopRequest(3))
    val expectedResponse =
      TransferMessage(ids, (TransferResult.Success, LoopOutput(3).asJson))

    val runner = mock[LoopRunner]
    (runner.initialize _).expects(request.message).returning(Progress(LoopProgress(0, 3)))
    (0 until 3).foreach { i =>
      (runner.step _)
        .expects(LoopProgress(i, 3 - i))
        .returning(Progress(LoopProgress(i + 1, 3 - i - 1)))
    }
    (runner.step _).expects(LoopProgress(3, 0)).returning(Done(LoopOutput(3)))

    val results = roundTripTransfers(List(request.asJson), runner)
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

    val runner = mock[LoopRunner]

    inSequence {
      (runner.initialize _)
        .expects(longRunning.message)
        .returning(Progress(LoopProgress(0, longRunningCount)))
      (runner.initialize _)
        .expects(quick.message)
        .returning(Progress(LoopProgress(0, quickCount)))
    }

    List(0 until longRunningCount, 0 until quickCount).foreach { range =>
      range.foreach { i =>
        (runner.step _)
          .expects(LoopProgress(i, range.end - i))
          .returning(Progress(LoopProgress(i + 1, range.end - i - 1)))
      }
    }

    (runner.step _).expects(LoopProgress(1, 0)).returning(Done(LoopOutput(1)))
    (runner.step _).expects(LoopProgress(5, 0)).returning(Done(LoopOutput(5)))

    // The quick result should arrive before the long-running one.
    roundTripTransfers(messages.map(_.asJson), runner) shouldBe List(
      TransferMessage(
        quick.ids,
        (TransferResult.Success, LoopOutput(quickCount).asJson)
      ),
      TransferMessage(
        longRunning.ids,
        (TransferResult.Success, LoopOutput(longRunningCount).asJson)
      )
    )
  }

  it should "bail out if malformed request payloads end up on the request topic" in {
    val messages = List(
      "How did I get here???".asJson,
      TransferMessage(ids, LoopRequest(0)).asJson
    )

    val runner = mock[LoopRunner]

    roundTripTransfers(messages, runner) shouldBe empty
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

    val runner = mock[LoopRunner]
    (runner.initialize _).expects(LoopRequest(0)).returning(Progress(LoopProgress(0, 0)))
    (runner.step _).expects(LoopProgress(0, 0)).returning(Done(LoopOutput(0)))

    val List(result1, result2) =
      roundTripTransfers(List(badSchema, goodSchema).map(_.asJson), runner)

    result1.ids shouldBe badSchema.ids
    val (res1, info1) = result1.message
    res1 shouldBe TransferResult.FatalFailure
    info1.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true

    result2 shouldBe TransferMessage(
      goodSchema.ids,
      (TransferResult.Success, LoopOutput(0).asJson)
    )
  }

  it should "push error results if initializing a transfer fails" in {
    val messages = List.tabulate(2) { i =>
      TransferMessage(ids.copy(transfer = UUID.randomUUID()), LoopRequest(i))
    }

    val runner = mock[LoopRunner]
    messages.foreach { msg =>
      (runner.initialize _).expects(msg.message).throwing(new Exception("OH NO"))
    }

    val List(result1, result2) = roundTripTransfers(messages.map(_.asJson), runner)

    result1.message._1 shouldBe TransferResult.FatalFailure
    result2.message._1 shouldBe TransferResult.FatalFailure

    List(result1.message._2, result2.message._2).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }

  it should "not crash on initialization failures with a null message" in {
    val messages = List.tabulate(2) { i =>
      TransferMessage(ids.copy(transfer = UUID.randomUUID()), LoopRequest(i))
    }

    val runner = mock[LoopRunner]
    messages.foreach { msg =>
      (runner.initialize _).expects(msg.message).throwing(new Exception)
    }

    val List(result1, result2) = roundTripTransfers(messages.map(_.asJson), runner)

    result1.message._1 shouldBe TransferResult.FatalFailure
    result2.message._1 shouldBe TransferResult.FatalFailure

    List(result1.message._2, result2.message._2).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }

  it should "push error results if processing a transfer fails" in {
    val messages = List.tabulate(2) { i =>
      TransferMessage(ids.copy(transfer = UUID.randomUUID()), LoopRequest(i))
    }

    val runner = mock[LoopRunner]
    messages.foreach { msg =>
      val zero = LoopProgress(0, msg.message.loopCount)
      (runner.initialize _).expects(msg.message).returning(Progress(zero))
      (runner.step _).expects(zero).throwing(new Exception("OH NO"))
    }

    val List(result1, result2) = roundTripTransfers(messages.map(_.asJson), runner)

    result1.message._1 shouldBe TransferResult.FatalFailure
    result2.message._1 shouldBe TransferResult.FatalFailure

    List(result1.message._2, result2.message._2).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }

  it should "not crash on step failures with a null message" in {
    val messages = List.tabulate(2) { i =>
      TransferMessage(ids.copy(transfer = UUID.randomUUID()), LoopRequest(i))
    }

    val runner = mock[LoopRunner]
    messages.foreach { msg =>
      val zero = LoopProgress(0, msg.message.loopCount)
      (runner.initialize _).expects(msg.message).returning(Progress(zero))
      (runner.step _).expects(zero).throwing(new Exception)
    }

    val List(result1, result2) = roundTripTransfers(messages.map(_.asJson), runner)

    result1.message._1 shouldBe TransferResult.FatalFailure
    result2.message._1 shouldBe TransferResult.FatalFailure

    List(result1.message._2, result2.message._2).foreach { jsonInfo =>
      jsonInfo.as[TransferStreamBuilder.UnhandledErrorInfo].isRight shouldBe true
    }
  }

  it should "support expansion of single transfer requests into multiple transfer requests" in {
    val message = TransferMessage(ids, LoopRequest(0))

    val runner = mock[LoopRunner]
    (runner.initialize _)
      .expects(message.message)
      .returning(Expanded(List(LoopRequest(0), LoopRequest(1))))

    val out = roundTripTransfers(List(message.asJson), runner)
    out should contain only TransferMessage(
      ids,
      (TransferResult.Expanded, List(LoopRequest(0), LoopRequest(1)).asJson)
    )
  }

  it should "support early exits from runner initialization" in {
    val message = TransferMessage(ids, LoopRequest(0))

    val runner = mock[LoopRunner]
    (runner.initialize _).expects(message.message).returning(Done(LoopOutput(0)))

    val out = roundTripTransfers(List(message.asJson), runner)
    out should contain only TransferMessage(
      ids,
      (TransferResult.Success, LoopOutput(0).asJson)
    )
  }
}

object TransferStreamBuilderSpec {

  val UnhandledError = new RuntimeException("OH NO")

  case class LoopRequest(loopCount: Int)
  case class LoopProgress(loopsSoFar: Int, loopsToGo: Int)
  case class LoopOutput(loopCount: Int)

  abstract class LoopRunner extends TransferRunner[LoopRequest, LoopProgress, LoopOutput]

  implicit val reqDecoder: Decoder[LoopRequest] = deriveDecoder
  implicit val reqEncoder: Encoder[LoopRequest] = deriveEncoder

  implicit val progressDecoder: Decoder[LoopProgress] = deriveDecoder
  implicit val progressEncoder: Encoder[LoopProgress] = deriveEncoder

  implicit val outDecoder: Decoder[LoopOutput] = deriveDecoder
  implicit val outEncoder: Encoder[LoopOutput] = deriveEncoder

  implicit val resDecoder: Decoder[UnhandledErrorInfo] = deriveDecoder
}
