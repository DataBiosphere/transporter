package org.broadinstitute.transporter.kafka

import java.util.concurrent.{ExecutorService, Executors}

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import com.dimafeng.testcontainers.{Container, ForAllTestContainer, TestContainerProxy}
import fs2.kafka._
import io.circe.{Decoder, Encoder, JsonObject}
import io.circe.derivation.{deriveDecoder, deriveEncoder}
import io.circe.literal._
import io.circe.syntax._
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.streams.KafkaStreams
import org.broadinstitute.transporter.queue.{Queue, QueueSchema}
import org.broadinstitute.transporter.transfer.{
  TransferResult,
  TransferRunner,
  TransferStatus
}
import org.scalatest.{EitherValues, FlatSpec, Matchers}
import org.testcontainers.containers.KafkaContainer

import scala.concurrent.ExecutionContext

class TransferStreamSpec
    extends FlatSpec
    with Matchers
    with ForAllTestContainer
    with EitherValues {

  import TransferStreamSpec._

  private val baseContainer = new KafkaContainer("5.1.1")

  override val container: Container = new TestContainerProxy[KafkaContainer] {
    override val container: KafkaContainer = baseContainer
  }

  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val queue = Queue(
    "test-queue",
    "request-topic",
    "response-topic",
    json"{}".as[QueueSchema].right.value
  )

  // Initialize queue topics to use across all test cases.
  override def afterStart(): Unit = {
    val adminConfig = AdminClientSettings.Default
      .withBootstrapServers(baseContainer.getBootstrapServers)

    fs2.kafka
      .adminClientResource[IO](adminConfig)
      .use { admin =>
        val topics = List(queue.requestTopic, queue.responseTopic).map { name =>
          new NewTopic(name, 1, 1)
        }
        admin.createTopics(topics)
      }
      .unsafeRunSync()
  }

  private def agentConfig =
    KStreamsConfig("test-app", baseContainer.getBootstrapServers.split(',').toList)

  private def producerConfig =
    ProducerSettings[String, String]
      .withBootstrapServers(baseContainer.getBootstrapServers)

  private def consumerConfig(ec: ExecutionContext) =
    ConsumerSettings[String, String](ec)
      .withBootstrapServers(baseContainer.getBootstrapServers)
      .withGroupId("test-consumer")

  private def runner = new TransferRunner[EchoRequest] {
    override def transfer(request: EchoRequest): TransferResult =
      request.result.getOrElse(throw new RuntimeException("OH NO"))
  }

  private def produceRequests(messages: List[(String, String)]) =
    fs2.kafka.producerResource[IO].using(producerConfig).use { producer =>
      val records = messages.map {
        case (id, message) =>
          ProducerRecord(queue.requestTopic, id, message)
      }
      producer.producePassthrough(ProducerMessage(records)).flatten
    }

  private def consumeResponses(n: Long): IO[List[(String, TransferResult)]] = {
    val consumerResource = for {
      actorEc <- fs2.kafka.consumerExecutionContextResource[IO]
      consumer <- fs2.kafka.consumerResource[IO].using(consumerConfig(actorEc))
    } yield {
      consumer
    }

    consumerResource.use { consumer =>
      for {
        _ <- consumer.subscribeTo(queue.responseTopic)
        messages <- consumer.stream.take(n).compile.toList
        parsedMessages <- messages.traverse { m =>
          io.circe.parser
            .parse(m.record.value())
            .flatMap(_.as[TransferResult])
            .map(m.record.key() -> _)
            .liftTo[IO]
        }
      } yield {
        parsedMessages
      }
    }
  }

  private def withRunningAgent[A](run: IO[A]): IO[A] = {
    val runEc = {
      val allocate = IO.delay(Executors.newCachedThreadPool)
      val free = (es: ExecutorService) => IO.delay(es.shutdown())
      Resource.make(allocate)(free).map(ExecutionContext.fromExecutor)
    }

    val useStream = (s: KafkaStreams) =>
      IO.delay(s.start()).flatMap(_ => runEc.use(cs.evalOn(_)(run)))

    TransferStream
      .build(agentConfig, queue, runner)
      .bracket(useStream)(s => IO.delay(s.close()))
  }

  behavior of "TransferStream"

  it should "receive requests, execute transfers, and push responses" in {
    val expected = List(
      "no-info" -> TransferResult(TransferStatus.Success, None),
      "with-info" -> TransferResult(
        TransferStatus.TransientFailure,
        Some(JsonObject("foo" -> "bar".asJson))
      )
    )

    val messages = expected.map {
      case (id, res) => id -> EchoRequest(Some(res)).asJson.noSpaces
    }

    val results = withRunningAgent {
      produceRequests(messages).flatMap(_ => consumeResponses(expected.length.toLong))
    }.unsafeRunSync()

    results shouldBe expected
  }

  it should "push error results if non-JSON values end up on the request topic" in {
    val goodResult = TransferResult(TransferStatus.Success, None)
    val messages = List(
      "not-json" -> "How did I get here???",
      "ok" -> EchoRequest(Some(goodResult)).asJson.noSpaces
    )

    val List((key1, result1), (key2, result2)) = withRunningAgent {
      produceRequests(messages).flatMap(_ => consumeResponses(2))
    }.unsafeRunSync()

    key1 shouldBe "not-json"
    result1.status shouldBe TransferStatus.FatalFailure
    result1.info should not be empty

    key2 shouldBe "ok"
    result2 shouldBe goodResult
  }

  it should "push error results if JSON with a bad schema ends up on the request topic" in {
    val goodResult = TransferResult(TransferStatus.Success, None)
    val messages = List(
      "wrong-json" -> """{ "problem": "not the right schema" }""",
      "ok" -> EchoRequest(Some(goodResult)).asJson.noSpaces
    )

    val List((key1, result1), (key2, result2)) = withRunningAgent {
      produceRequests(messages).flatMap(_ => consumeResponses(2))
    }.unsafeRunSync()

    key1 shouldBe "wrong-json"
    result1.status shouldBe TransferStatus.FatalFailure
    result1.info should not be empty

    key2 shouldBe "ok"
    result2 shouldBe goodResult
  }

  it should "push error results if processing a transfer fails" in {
    val goodResult = TransferResult(TransferStatus.Success, None)
    val messages = List(
      "boom" -> "{}",
      "ok" -> EchoRequest(Some(goodResult)).asJson.noSpaces
    )
    val List((key1, result1), (key2, result2)) = withRunningAgent {
      produceRequests(messages).flatMap(_ => consumeResponses(2))
    }.unsafeRunSync()

    key1 shouldBe "boom"
    result1.status shouldBe TransferStatus.FatalFailure
    result1.info should not be empty

    key2 shouldBe "ok"
    result2 shouldBe goodResult
  }
}

object TransferStreamSpec {
  case class EchoRequest(result: Option[TransferResult])
  implicit val decoder: Decoder[EchoRequest] = deriveDecoder
  implicit val encoder: Encoder[EchoRequest] = deriveEncoder
}
