package org.broadinstitute.transporter.kafka

import java.nio.ByteBuffer

import fs2.kafka.{Deserializer, Serializer}
import io.chrisdavenport.fuuid.FUUID
import io.circe.{Decoder, Encoder}
import io.circe.jawn.JawnParser
import io.circe.syntax._

/** Kafka (de)serializers for types not covered by fs2-kafka. */
object Serdes {

  /** Kafka serializer for any type that can be encoded as JSON. */
  def encodingSerializer[A: Encoder]: Serializer[A] =
    Serializer.string.contramap(_.asJson.noSpaces)

  /** Kafka deserializer for any type that can be decoded from JSON. */
  def decodingDeserializer[A: Decoder]: Deserializer.Attempt[A] = {
    val parser = new JawnParser()
    Deserializer.bytes.map { bytes =>
      parser.decodeByteBuffer[A](ByteBuffer.wrap(bytes.get))
    }
  }

  val fuuidSerializer: Serializer[FUUID] = Serializer.uuid.contramap(FUUID.Unsafe.toUUID)

  val fuuidDeserializer: Deserializer.Attempt[FUUID] =
    Deserializer.uuid.map(_.map(FUUID.fromUUID))
}
