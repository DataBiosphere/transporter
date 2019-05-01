package org.broadinstitute.transporter.kafka

import cats.effect.IO
import fs2.kafka.{Deserializer, Serializer}

class KafkaProducerSpec extends BaseKafkaSpec {

  behavior of "KafkaProducer"

  it should "submit messages to one topic" in {
    val topic = "the-topic"

    val messages = List("foo", "bar", "baz")

    val published = withKafka { (config, embeddedConfig) =>
      KafkaProducer
        .resource(config, Serializer.string)
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic)(embeddedConfig))
            _ <- producer.submit(List(topic -> messages))
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

  it should "submit messages to multiple topics" in {
    val topic1 = "the-topic"
    val topic2 = "the-topic2"

    val messages1 = List("foo", "bar", "baz")
    val messages2 = List("zab", "rab", "oof")

    val published = withKafka { (config, embeddedConfig) =>
      KafkaProducer
        .resource(config, Serializer.string)
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic1)(embeddedConfig))
            _ <- IO.delay(createCustomTopic(topic2)(embeddedConfig))
            _ <- producer.submit(List(topic2 -> messages2, topic1 -> messages1))
            consumed <- IO.delay {
              consumeNumberMessagesFromTopics(
                Set(topic1, topic2),
                messages1.length + messages2.length
              )(embeddedConfig, Deserializer.string)
            }
          } yield {
            consumed
          }
        }
        .unsafeRunSync()
    }

    published shouldBe Map(topic1 -> messages1, topic2 -> messages2)
  }

}
