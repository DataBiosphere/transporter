package org.broadinstitute.transporter.kafka

import cats.effect.IO
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
import org.broadinstitute.transporter.kafka.TransferStream.UnhandledErrorInfo
import org.broadinstitute.transporter.queue.{Queue, QueueSchema}
import org.broadinstitute.transporter.transfer.{
  TransferResult,
  TransferRunner,
  TransferSummary
}
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class TransferStreamSpec
    extends FlatSpec
    with Matchers
    with EmbeddedKafkaStreamsAllInOne
    with EitherValues {

  import TransferStreamSpec._

  private val queue = Queue(
    "test-queue",
    "request-topic",
    "response-topic",
    EchoSchema.as[QueueSchema].right.value
  )

  private implicit val baseConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig(
      customBrokerProperties = Map(
        // Needed to make exactly-once processing work in Kafka Streams
        // when there's only 1 broker in the cluster.
        "transaction.state.log.replication.factor" -> "1",
        "transaction.state.log.min.isr" -> "1"
      )
    )

  /**
    * Run a set of requests through an instance of the "echo" stream,
    * and return the decoded results.
    */
  private def roundTripTransfers(
    requests: List[(String, String)],
    fail: Boolean = false
  ): List[(String, TransferSummary)] = {
    val topology = TransferStream.build(queue, new EchoRunner(fail))
    val config = KStreamsConfig("test-app", List(s"localhost:${baseConfig.kafkaPort}"))

    runStreams(List(queue.requestTopic, queue.responseTopic), topology, config.asMap) {
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
          parse(v).flatMap(_.as[TransferSummary]).map(k -> _)
      }.right.value
    }
  }

  behavior of "TransferStream"

  it should "receive requests, execute transfers, and push responses" in {
    val expected = List(
      "no-info" -> TransferSummary(TransferResult.Success, None),
      "with-info" -> TransferSummary(
        TransferResult.TransientFailure,
        Some(json"""{"foo": "bar"}""")
      )
    )
    val messages = expected.map {
      case (id, res) => id -> EchoRequest(res).asJson.noSpaces
    }
    val results = roundTripTransfers(messages)
    results shouldBe expected
  }

  it should "push error results if non-JSON values end up on the request topic" in {
    val goodResult = TransferSummary(TransferResult.Success, None)
    val messages = List(
      "not-json" -> "How did I get here???",
      "ok" -> EchoRequest(goodResult).asJson.noSpaces
    )

    val List((key1, result1), (key2, result2)) = roundTripTransfers(messages)

    key1 shouldBe "not-json"
    result1.result shouldBe TransferResult.FatalFailure
    val info = for {
      json <- result1.info.toRight("Received unexpected empty info")
      decoded <- json.as[TransferStream.UnhandledErrorInfo]
    } yield {
      decoded
    }
    info.right.value.message should include(TransferStream.ParseFailureMsg)

    key2 shouldBe "ok"
    result2 shouldBe goodResult
  }

  it should "push error results if JSON with a bad schema ends up on the request topic" in {
    val goodResult = TransferSummary(TransferResult.Success, None)
    val messages = List(
      "wrong-json" -> """{ "problem": "not the right schema" }""",
      "ok" -> EchoRequest(goodResult).asJson.noSpaces
    )

    val List((key1, result1), (key2, result2)) = roundTripTransfers(messages)

    key1 shouldBe "wrong-json"
    result1.result shouldBe TransferResult.FatalFailure
    val info = for {
      json <- result1.info.toRight("Received unexpected empty info")
      decoded <- json.as[TransferStream.UnhandledErrorInfo]
    } yield {
      decoded
    }
    info.right.value.message should include(TransferStream.ValidationFailureMsg)

    key2 shouldBe "ok"
    result2 shouldBe goodResult
  }

  it should "push error results if processing a transfer fails" in {
    val goodResult = TransferSummary(TransferResult.Success, None)
    val messages = List(
      "boom1" -> EchoRequest(goodResult).asJson.noSpaces,
      "boom2" -> EchoRequest(goodResult).asJson.noSpaces
    )
    val List((key1, result1), (key2, result2)) = roundTripTransfers(messages, fail = true)

    key1 shouldBe "boom1"
    key2 shouldBe "boom2"

    result1.result shouldBe TransferResult.FatalFailure
    result2.result shouldBe TransferResult.FatalFailure

    List(result1.info, result2.info).foreach { jsonInfo =>
      val info = for {
        json <- jsonInfo.toRight("Received unexpected empty info")
        decoded <- json.as[TransferStream.UnhandledErrorInfo]
      } yield {
        decoded
      }
      info.right.value.message should include(TransferStream.UnhandledErrMsg)
    }
  }
}

object TransferStreamSpec {

  val UnhandledError = new RuntimeException("OH NO")

  class EchoRunner(fail: Boolean) extends TransferRunner {
    override def transfer(request: Json): IO[TransferSummary] =
      request
        .as[EchoRequest]
        .flatMap(e => if (fail) Left(UnhandledError) else Right(e.result))
        .liftTo[IO]
  }

  case class EchoRequest(result: TransferSummary)

  val EchoSchema = json"""{
    "type": "object",
    "properties": { "result": { "type": "object" } },
    "additionalProperties": false
  }"""

  implicit val decoder: Decoder[EchoRequest] = deriveDecoder
  implicit val encoder: Encoder[EchoRequest] = deriveEncoder

  implicit val resDecoder: Decoder[UnhandledErrorInfo] = deriveDecoder
}
