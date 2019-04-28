package org.broadinstitute.transporter.kafka

import cats.effect.IO
import fs2.kafka.{Deserializer, Serializer}

class KafkaProducerSpec extends BaseKafkaSpec {

  behavior of "KafkaProducer"

  it should "submit messages" in {
    val topic = "the-topic"

    val messages = List("foo", "bar", "baz")

    val published = withKafka { (config, embeddedConfig) =>
      KafkaProducer
        .resource(config, Serializer.string)
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic)(embeddedConfig))
            _ <- producer.submit(topic, messages)
            consumed <- IO.delay {
              consumeNumberMessagesFrom(topic, messages.length)(
                embeddedConfig,
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
