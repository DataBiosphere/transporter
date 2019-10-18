package org.broadinstitute.transporter.kafka

import java.nio.ByteBuffer

import cats.effect.IO
import fs2.kafka.{Deserializer, Serializer}
import io.circe.{Decoder, Encoder}
import io.circe.jawn.JawnParser
import io.circe.syntax._

/** Kafka (de)serializers for types not covered by fs2-kafka. */
object Serdes {

  /** Convenience alias to allow using `Either` in context-bounds. */
  type Attempt[T] = Either[Throwable, T]

  /** Kafka serializer for any type that can be encoded as JSON. */
  implicit def encodingSerializer[A: Encoder]: Serializer[IO, A] =
    Serializer.string[IO].contramap(_.asJson.noSpaces)

  /** Kafka deserializer for any type that can be decoded from JSON. */
  implicit def decodingDeserializer[A: Decoder]: Deserializer[IO, Either[Throwable, A]] = {
    val parser = new JawnParser()
    Deserializer[IO].map { bytes =>
      parser.decodeByteBuffer[A](ByteBuffer.wrap(bytes))
    }
  }
}
