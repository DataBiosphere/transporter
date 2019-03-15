package org.broadinstitute.transporter.kafka

import java.util.UUID

import cats.effect.{IO, Timer}
import fs2.concurrent.Queue
import fs2.kafka.Serializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class KafkaConsumerSpec extends BaseKafkaSpec {

  private implicit val t: Timer[IO] = IO.timer(ExecutionContext.global)

  private val topicPattern = "the-topic.*".r

  behavior of "KafkaConsumer"

  it should "process well-formed messages already in the topic on startup" in {
    val messages = List.tabulate(5)(_ -> UUID.randomUUID())

    val consumed = withKafka { (config, embeddedConfig) =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, (Int, UUID)]
        _ <- IO.delay(createCustomTopic("the-topic")(embeddedConfig))
        _ <- IO.delay {
          publishToKafka("the-topic", messages)(
            embeddedConfig,
            Serializer.int,
            Serializer.uuid
          )
        }
        out <- KafkaConsumer.resource[Int, UUID](topicPattern, config).use { consumer =>
          val fiber = consumer.runForeach {
            case Right(out) => q.enqueue1(out)
            case Left(err)  => IO.raiseError(err)
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

  it should "process well-formed messages added to the topic post-startup" in {
    val messages = List.tabulate(5)(_ -> UUID.randomUUID())

    val consumed = withKafka { (config, embeddedConfig) =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, (Int, UUID)]
        _ <- IO.delay(createCustomTopic("the-topic")(embeddedConfig))
        out <- KafkaConsumer.resource[Int, UUID](topicPattern, config).use { consumer =>
          val fiber = consumer.runForeach {
            case Right(out) => q.enqueue1(out)
            case Left(err)  => IO.raiseError(err)
          }.start

          fiber.bracket({ _ =>
            IO.delay {
              publishToKafka("the-topic", messages)(
                embeddedConfig,
                Serializer.int,
                Serializer.uuid
              )
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

  it should "not double-process messages on restart" in {
    val messages = List.tabulate(5)(_ -> UUID.randomUUID())

    val consumed = withKafka { (config, embeddedConfig) =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, (Int, UUID)]
        _ <- IO.delay(createCustomTopic("the-topic")(embeddedConfig))
        _ <- IO.delay {
          publishToKafka("the-topic", messages)(
            embeddedConfig,
            Serializer.int,
            Serializer.uuid
          )
        }
        out1 <- KafkaConsumer.resource[Int, UUID](topicPattern, config).use { consumer =>
          val fiber = consumer.runForeach {
            case Right(out) => q.enqueue1(out)
            case Left(err)  => IO.raiseError(err)
          }.start

          fiber.bracket(_ => q.dequeue.take(5).compile.toList)(_.cancel)
        }
        out2 <- KafkaConsumer.resource[Int, UUID](topicPattern, config).use { consumer =>
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

  it should "process messages with malformed keys" in {
    val messages = List.tabulate(5)(i => s"a$i" -> UUID.randomUUID())

    val decodingFailures = withKafka { (config, embeddedConfig) =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, Throwable]
        _ <- IO.delay(createCustomTopic("the-topic")(embeddedConfig))
        _ <- IO.delay(
          publishToKafka("the-topic", messages)(
            embeddedConfig,
            Serializer.string,
            Serializer.uuid
          )
        )
        out <- KafkaConsumer.resource[Int, UUID](topicPattern, config).use { consumer =>
          val fiber = consumer.runForeach {
            case Right(out) =>
              IO.raiseError(new IllegalStateException(s"Decoding of $out failed to fail"))
            case Left(err) =>
              q.enqueue1(err)
          }.start

          fiber.bracket(_ => q.dequeue.take(5).compile.toList)(_.cancel)
        }
      } yield {
        out
      }

      roundTrip.unsafeRunSync()
    }

    decodingFailures should have length 5L
  }

  it should "process messages with malformed values" in {
    val messages = List.tabulate(5)(i => i -> (i + 1))

    val decodingFailures = withKafka { (config, embeddedConfig) =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, Throwable]
        _ <- IO.delay(createCustomTopic("the-topic")(embeddedConfig))
        _ <- IO.delay(
          publishToKafka("the-topic", messages)(
            embeddedConfig,
            Serializer.int,
            Serializer.int
          )
        )
        out <- KafkaConsumer.resource[Int, UUID](topicPattern, config).use { consumer =>
          val fiber = consumer.runForeach {
            case Right(out) =>
              IO.raiseError(new IllegalStateException(s"Decoding of $out failed to fail"))
            case Left(err) =>
              q.enqueue1(err)
          }.start

          fiber.bracket(_ => q.dequeue.take(5).compile.toList)(_.cancel)
        }
      } yield {
        out
      }

      roundTrip.unsafeRunSync()
    }

    decodingFailures should have length 5L
  }

  it should "find new topics matching its subscription pattern" in {
    val messages1 = List.tabulate(3)(_ -> UUID.randomUUID())
    val messages2 = List.tabulate(5)(_ -> UUID.randomUUID())
    val messages3 = List.tabulate(2)(_ -> UUID.randomUUID())

    val consumed = withKafka { (config, embeddedConfig) =>
      val roundTrip = for {
        q <- Queue.unbounded[IO, (Int, UUID)]
        _ <- IO.delay(createCustomTopic("the-topic")(embeddedConfig))
        _ <- IO.delay {
          publishToKafka("the-topic", messages1)(
            embeddedConfig,
            Serializer.int,
            Serializer.uuid
          )
        }
        out <- KafkaConsumer.resource[Int, UUID](topicPattern, config).use { consumer =>
          val fiber = consumer.runForeach {
            case Right(out) => q.enqueue1(out)
            case Left(err)  => IO.raiseError(err)
          }.start

          fiber.bracket({ _ =>
            for {
              _ <- IO.delay(createCustomTopic("the-topic2")(embeddedConfig))
              _ <- IO.delay {
                publishToKafka("the-topic2", messages2)(
                  embeddedConfig,
                  Serializer.int,
                  Serializer.uuid
                )
              }
              _ <- IO.delay {
                publishToKafka("the-topic", messages3)(
                  embeddedConfig,
                  Serializer.int,
                  Serializer.uuid
                )
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
  }
}
