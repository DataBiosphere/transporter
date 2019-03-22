package org.broadinstitute.transporter.kafka

import cats.effect.IO
import cats.implicits._
import fs2.kafka.{Deserializer, Serializer}

class KafkaProducerSpec extends BaseKafkaSpec {

  behavior of "KafkaProducer"

  it should "submit messages" in {
    val topic = "the-topic"

    val messages = List(1 -> "foo", 2 -> "bar", 3 -> "baz")

    val published = withKafka { (config, embeddedConfig) =>
      KafkaProducer
        .resource(config, Serializer.int, Serializer.string)
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic)(embeddedConfig))
            _ <- producer.submit(topic, messages)
            consumed <- IO.delay {
              consumeNumberKeyedMessagesFrom(topic, messages.length)(
                embeddedConfig,
                Deserializer.int.map(_.valueOr(throw _)),
                Deserializer.string
              )
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
