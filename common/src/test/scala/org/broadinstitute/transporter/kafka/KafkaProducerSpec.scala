package org.broadinstitute.transporter.kafka

import java.util.UUID

import cats.effect.IO
import org.broadinstitute.transporter.transfer.{TransferIds, TransferMessage}

class KafkaProducerSpec extends BaseKafkaSpec {
  import Serdes._

  behavior of "KafkaProducer"

  it should "submit messages to one topic" in {
    val topic = "the-topic"

    val messages = List("foo", "bar", "baz").map {
      TransferIds(UUID.randomUUID(), UUID.randomUUID()) -> _
    }

    val published = withKafka { implicit embeddedConfig =>
      KafkaProducer
        .resource[String](connConfig(embeddedConfig))
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic)(embeddedConfig))
            _ <- producer.submit(List(topic -> messages))
            consumed <- IO.delay {
              consumeNumberMessagesFrom[Attempt[TransferMessage[String]]](
                topic,
                messages.length
              )
            }
          } yield {
            consumed
          }
        }
        .unsafeRunSync()

    }

    published shouldBe messages.map { case (id, m) => Right(TransferMessage(id, m)) }
  }

  it should "submit messages to multiple topics" in {
    val topic1 = "the-topic"
    val topic2 = "the-topic2"

    val messages1 = List("foo", "bar", "baz").map {
      TransferIds(UUID.randomUUID(), UUID.randomUUID()) -> _
    }
    val messages2 = List("zab", "rab", "oof").map {
      TransferIds(UUID.randomUUID(), UUID.randomUUID()) -> _
    }

    val published = withKafka { implicit embeddedConfig =>
      KafkaProducer
        .resource[String](connConfig(embeddedConfig))
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic1)(embeddedConfig))
            _ <- IO.delay(createCustomTopic(topic2)(embeddedConfig))
            _ <- producer.submit(List(topic2 -> messages2, topic1 -> messages1))
            consumed <- IO.delay {
              consumeNumberMessagesFromTopics[Attempt[TransferMessage[String]]](
                Set(topic1, topic2),
                messages1.length + messages2.length
              )
            }
          } yield {
            consumed
          }
        }
        .unsafeRunSync()

    }

    published shouldBe Map(
      topic1 -> messages1.map { case (id, m) => Right(TransferMessage(id, m)) },
      topic2 -> messages2.map { case (id, m) => Right(TransferMessage(id, m)) }
    )
  }

}
