package org.broadinstitute.transporter.kafka

import java.util.UUID

import cats.effect.{IO, Timer}
import cats.implicits._
import fs2.Stream
import fs2.concurrent.Queue
import io.circe.DecodingFailure
import org.broadinstitute.transporter.kafka.config.ConsumerConfig
import org.broadinstitute.transporter.transfer.{TransferIds, TransferMessage}
import org.scalatest.EitherValues

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaConsumerSpec extends BaseKafkaSpec with EitherValues {
  import Serdes._

  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val topic = "the-topic"

  private val consumerConfig = ConsumerConfig("test-group", 3, 500.millis, None)

  behavior of "KafkaConsumer"

  it should "process well-formed messages already in a topic on startup" in {
    val requestId = UUID.randomUUID()
    val messages = List.fill(5) {
      val transferId = UUID.randomUUID()
      TransferMessage(TransferIds(requestId, transferId), transferId)
    }

    val consumed = withKafka { implicit embeddedConfig =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, TransferMessage[UUID]]
        _ <- IO.delay(createCustomTopic(topic))
        _ <- IO.delay(publishToKafka(topic, messages.map(() -> _)))

        connectionConfig = connConfig(embeddedConfig)
        consumer = KafkaConsumer.ofTopic[UUID](topic, connectionConfig, consumerConfig)

        out <- consumer.use { consumer =>
          val fiber = consumer.stream.flatMap { batch =>
            Stream
              .chunk(batch)
              .covary[IO]
              .evalMap {
                case (message, offset) =>
                  offset.commit.as(message)
              }
              .through(q.enqueue)
          }.compile.drain.start

          fiber.bracket(_ => q.dequeue.take(5).compile.toList)(_.cancel)
        }
      } yield {
        out
      }

      roundTrip.unsafeRunSync()
    }

    consumed shouldBe messages
  }

  it should "process well-formed messages added to a topic post-startup" in {
    val requestId = UUID.randomUUID()
    val messages = List.fill(5) {
      val transferId = UUID.randomUUID()
      TransferMessage(TransferIds(requestId, transferId), transferId)
    }

    val consumed = withKafka { implicit embeddedConfig =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, TransferMessage[UUID]]
        _ <- IO.delay(createCustomTopic(topic))

        connectionConfig = connConfig(embeddedConfig)
        consumer = KafkaConsumer.ofTopic[UUID](topic, connectionConfig, consumerConfig)

        out <- consumer.use { consumer =>
          val fiber = consumer.stream.flatMap { batch =>
            Stream
              .chunk(batch)
              .covary[IO]
              .evalMap {
                case (message, offset) =>
                  offset.commit.as(message)
              }
              .through(q.enqueue)
          }.compile.drain.start

          fiber.bracket({ _ =>
            IO.delay {
              publishToKafka(topic, messages.map(() -> _))
            }.flatMap(_ => q.dequeue.take(5).compile.toList)
          })(_.cancel)
        }
      } yield {
        out
      }

      roundTrip.unsafeRunSync()
    }

    consumed shouldBe messages
  }

  it should "not double-process messages in a topic on restart" in {
    val requestId = UUID.randomUUID()
    val messages = List.fill(5) {
      val transferId = UUID.randomUUID()
      TransferMessage(TransferIds(requestId, transferId), transferId)
    }

    val consumed = withKafka { implicit embeddedConfig =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, TransferMessage[UUID]]
        _ <- IO.delay(createCustomTopic(topic))
        _ <- IO.delay {
          publishToKafka(topic, messages.map(() -> _))
        }

        connectionConfig = connConfig(embeddedConfig)
        consumer = KafkaConsumer.ofTopic[UUID](topic, connectionConfig, consumerConfig)

        out1 <- consumer.use { consumer =>
          val fiber = consumer.stream.flatMap { batch =>
            Stream
              .chunk(batch)
              .covary[IO]
              .evalMap {
                case (message, offset) =>
                  offset.commit.as(message)
              }
              .through(q.enqueue)

          }.compile.drain.start

          fiber.bracket(_ => q.dequeue.take(5).compile.toList)(_.cancel)
        }
        out2 <- consumer.use { consumer =>
          val fiber = consumer.stream.map(_ => ???).compile.drain.start

          fiber.bracket(
            _ => q.dequeue.take(1).compile.toList.timeoutTo(1.second, IO.pure(Nil))
          )(_.cancel)
        }
      } yield {
        out1 ::: out2
      }

      roundTrip.unsafeRunSync()
    }

    consumed shouldBe messages
  }

  it should "not emit malformed messages" in {
    val messages = List.tabulate(5)(_ + 1)

    withKafka { implicit embeddedConfig =>
      val roundTrip = for {
        _ <- IO.delay(createCustomTopic(topic))
        _ <- IO.delay(
          publishToKafka(topic, messages.map(() -> _))
        )

        connectionConfig = connConfig(embeddedConfig)
        consumer = KafkaConsumer.ofTopic[UUID](topic, connectionConfig, consumerConfig)

        consumerAttempt <- consumer.use(_.stream.map(_ => ???).compile.drain.attempt)
        notConsumed <- IO.delay(consumeNumberMessagesFrom[Attempt[Int]](topic, 5))
      } yield {
        consumerAttempt.left.value shouldBe a[DecodingFailure]
        notConsumed shouldBe messages.map(Right(_))
      }

      roundTrip.unsafeRunSync()
    }
  }
}
