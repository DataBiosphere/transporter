package org.broadinstitute.transporter.kafka

import java.util.UUID

import cats.effect.IO
import org.broadinstitute.transporter.transfer.{TransferIds, TransferMessage}

class KafkaProducerSpec extends BaseKafkaSpec {
  import Serdes._

  behavior of "KafkaProducer"

  it should "submit messages to a topic" in {
    val topic = "the-topic"

    val messages = List("foo", "bar", "baz").map { msg =>
      TransferMessage(TransferIds(UUID.randomUUID(), UUID.randomUUID()), msg)
    }

    val published = withKafka { implicit embeddedConfig =>
      KafkaProducer
        .resource[String](connConfig(embeddedConfig))
        .use { producer =>
          for {
            _ <- IO.delay(createCustomTopic(topic))
            _ <- producer.submit(topic, messages)
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

    published shouldBe messages.map(Right(_))
  }
}
