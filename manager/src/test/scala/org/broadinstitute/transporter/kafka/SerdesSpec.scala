package org.broadinstitute.transporter.kafka

import java.util.UUID

import fs2.kafka.Headers
import io.circe.derivation.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class SerdesSpec extends FlatSpec with Matchers with EitherValues {
  import SerdesSpec._

  behavior of "Serdes"

  it should "round-trip (de)serialization" in {
    val foo1 = Foo(Some("thing"), 0, Nil)
    val foo2 = Foo(None, -12312234, List.fill(3)(UUID.randomUUID()))
    val foo3 = Foo(
      Some("dfk;sajflk;adsjfsafdsfsadfdsafads"),
      Int.MaxValue,
      List.fill(100)(UUID.randomUUID())
    )

    List(foo1, foo2, foo3).foreach { foo =>
      val roundTrip = for {
        serialized <- Serdes
          .encodingSerializer[Foo]
          .serialize("the-topic", Headers.empty, foo)
        deserialized <- Serdes
          .decodingDeserializer[Foo]
          .deserialize("the-topic", Headers.empty, serialized)
      } yield {
        deserialized
      }

      roundTrip.unsafeRunSync().right.value shouldBe foo
    }
  }
}

object SerdesSpec {
  case class Foo(a: Option[String], b: Int, c: List[UUID])
  implicit val decoder: Decoder[Foo] = deriveDecoder
  implicit val encoder: Encoder[Foo] = deriveEncoder
}
