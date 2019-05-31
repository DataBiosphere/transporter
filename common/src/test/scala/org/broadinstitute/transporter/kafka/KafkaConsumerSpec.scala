package org.broadinstitute.transporter.kafka

import java.util.UUID

import cats.effect.{IO, Timer}
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
  private val topicPattern = s"$topic.*".r

  private val consumerConfig = ConsumerConfig("test-group", 3, 500.millis, 250.millis)

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
        consumerBuilder = KafkaConsumer.ofTopic[UUID](topic)

        out <- consumerBuilder(connectionConfig, consumerConfig).use { consumer =>
          val fiber = consumer.runForeach { batch =>
            Stream
              .emits(batch)
              .covary[IO]
              .map { case (ids, m) => TransferMessage(ids, m) }
              .through(q.enqueue)
              .compile
              .drain
          }.start

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
        consumerBuilder = KafkaConsumer.ofTopic[UUID](topic)

        out <- consumerBuilder(connectionConfig, consumerConfig).use { consumer =>
          val fiber = consumer.runForeach { batch =>
            Stream
              .emits(batch)
              .covary[IO]
              .map { case (ids, m) => TransferMessage(ids, m) }
              .through(q.enqueue)
              .compile
              .drain
          }.start

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
        consumerBuilder = KafkaConsumer.ofTopic[UUID](topic)

        out1 <- consumerBuilder(connectionConfig, consumerConfig).use { consumer =>
          val fiber = consumer.runForeach { batch =>
            Stream
              .emits(batch)
              .covary[IO]
              .map { case (ids, m) => TransferMessage(ids, m) }
              .through(q.enqueue)
              .compile
              .drain
          }.start

          fiber.bracket(_ => q.dequeue.take(5).compile.toList)(_.cancel)
        }
        out2 <- consumerBuilder(connectionConfig, consumerConfig).use { consumer =>
          val fiber = consumer.runForeach(_ => ???).start

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

  it should "not commit malformed messages" in {
    val messages = List.tabulate(5)(_ + 1)

    withKafka { implicit embeddedConfig =>
      val roundTrip = for {
        _ <- IO.delay(createCustomTopic(topic))
        _ <- IO.delay(
          publishToKafka(topic, messages.map(() -> _))
        )

        connectionConfig = connConfig(embeddedConfig)
        consumerBuilder = KafkaConsumer.ofTopic[UUID](topic)

        consumerAttempt <- consumerBuilder(connectionConfig, consumerConfig)
          .use(_.runForeach(_ => ???).attempt)
        notConsumed <- IO.delay(
          consumeNumberMessagesFrom[Attempt[Int]](topic, 5)
        )
      } yield {
        consumerAttempt.left.value shouldBe a[DecodingFailure]
        notConsumed shouldBe messages.map(Right(_))
      }

      roundTrip.unsafeRunSync()
    }
  }

  it should "find new topics matching a subscription pattern" in {
    val messages1 = {
      val requestId = UUID.randomUUID()
      List.fill(3) {
        val transferId = UUID.randomUUID()
        TransferMessage(TransferIds(requestId, transferId), transferId)
      }
    }
    val messages2 = {
      val requestId = UUID.randomUUID()
      List.fill(5) {
        val transferId = UUID.randomUUID()
        TransferMessage(TransferIds(requestId, transferId), transferId)
      }
    }
    val messages3 = {
      val requestId = UUID.randomUUID()
      List.fill(2) {
        val transferId = UUID.randomUUID()
        TransferMessage(TransferIds(requestId, transferId), transferId)
      }
    }
    val messages4 = {
      val requestId = UUID.randomUUID()
      List.fill(5) {
        val transferId = UUID.randomUUID()
        TransferMessage(TransferIds(requestId, transferId), transferId)
      }
    }

    val consumed = withKafka { implicit embeddedConfig =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, TransferMessage[UUID]]
        _ <- IO.delay(createCustomTopic(topic))
        _ <- IO.delay {
          publishToKafka(topic, messages1.map(() -> _))
        }

        connectionConfig = connConfig(embeddedConfig)
        consumerBuilder = KafkaConsumer.ofTopicPattern[UUID](topicPattern)

        out <- consumerBuilder(connectionConfig, consumerConfig).use { consumer =>
          val fiber = consumer.runForeach { batch =>
            Stream
              .emits(batch)
              .covary[IO]
              .map { case (ids, m) => TransferMessage(ids, m) }
              .through(q.enqueue)
              .compile
              .drain
          }.start

          fiber.bracket({ _ =>
            for {
              _ <- IO.delay(createCustomTopic(s"$topic-2"))
              _ <- IO.delay {
                publishToKafka(s"$topic-2", messages2.map(() -> _))
              }
              _ <- IO.delay {
                publishToKafka("not-our-topic", messages4.map(() -> _))
              }
              _ <- IO.delay {
                publishToKafka(topic, messages3.map(() -> _))
              }
              out <- q.dequeue.take(10).compile.toList
            } yield {
              out
            }
          })(_.cancel)
        }
      } yield {
        out
      }

      roundTrip.unsafeRunSync()
    }

    consumed should contain inOrderElementsOf (messages1 ::: messages3)
    consumed should contain inOrderElementsOf messages2
    consumed should contain noElementsOf messages4
  }
}
