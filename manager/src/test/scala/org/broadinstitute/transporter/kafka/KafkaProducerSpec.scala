package org.broadinstitute.transporter.kafka

import cats.effect.IO
import cats.implicits._
import fs2.kafka.{Deserializer, Serializer}

class KafkaProducerSpec extends BaseKafkaSpec {

  behavior of "KafkaProducer"

  it should "submit messages" in {
    val topic = "the-topic"

    implicit val keySerializer: Serializer[Int] = Serializer.int
    implicit val keyDeserializer: Deserializer[Int] =
      Deserializer.int.map(_.valueOr(throw _))
    implicit val valSerializer: Serializer[String] = Serializer.string
    implicit val valDeserializer: Deserializer[String] = Deserializer.string

    val messages = List(1 -> "foo", 2 -> "bar", 3 -> "baz")

    val published = withKafka { config =>
      KafkaProducer
        .resource[Int, String](config)
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic))
            _ <- producer.submit(topic, messages)
            consumed <- IO.delay {
              consumeNumberKeyedMessagesFrom[Int, String](topic, messages.length)
            }
          } yield {
            consumed
          }
        }
        .unsafeRunSync()
    }

    published shouldBe messages
  }
}
