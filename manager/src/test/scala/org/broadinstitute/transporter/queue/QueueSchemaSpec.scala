package org.broadinstitute.transporter.queue

import io.circe.literal._
import io.circe.syntax._
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class QueueSchemaSpec extends FlatSpec with Matchers with EitherValues {
  behavior of "QueueSchema"

  private val draft4Schema =
    json"""{
      "$$schema": ${QueueSchema.schemaUrl(4)},
      "id": "#draft4-example",
      "type": "object"
    }"""

  private val draft6Schema =
    json"""{
      "$$schema": ${QueueSchema.schemaUrl(6)},
      "$$id": "#draft6-example",
      "type": "object"
    }"""

  private val draft7Schema =
    json"""{
      "$$schema": ${QueueSchema.schemaUrl(7)},
      "$$id": "#draft7-example",
      "type": ["object", "boolean"]
    }"""

  private val cloudCopySchema =
    json"""{
      "type": "object",
      "required": [
        "source",
        "destination"
      ],
      "properties": {
        "source": {
          "type": "string",
          "pattern": "^(gs://|/)[^/].*"
        },
        "destination": {
          "type": "string",
          "pattern": "^(gs://|/)[^/].*"
        },
        "metadata": {
          "type": "object"
        }
      }
    }"""

  it should "parse draft-4 JSON schemas" in {
    val schema = draft4Schema.as[QueueSchema].right.value
    schema.validated(json"{}").isValid shouldBe true
    schema.validated(json"[]").isValid shouldBe false
  }

  it should "parse draft-6 JSON schemas" in {
    val schema = draft6Schema.as[QueueSchema].right.value
    schema.validated(json"{}").isValid shouldBe true
    schema.validated(json"[]").isValid shouldBe false
  }

  it should "parse draft-7 JSON schemas" in {
    val schema = draft7Schema.as[QueueSchema].right.value
    schema.validated(json"{}").isValid shouldBe true
    schema.validated(json"[]").isValid shouldBe false
  }

  it should "parse JSON schemas into validators" in {
    val schema = cloudCopySchema.as[QueueSchema].right.value

    val goodRequest1 =
      json"""{
        "source": "gs://foo/bar",
        "destination": "gs://baz/qux",
        "metadata": {
          "cool-prop": "the-coolest",
          "prop12345": "67890"
        }
      }"""

    val goodRequest2 =
      json"""{
        "source": "gs://foo/bar",
        "destination": "gs://baz/qux"
      }"""

    val badRequest1 =
      json"""{
        "source": "gs://foo/bar",
        "destination": "s3://baz/qux",
        "metadata": {
          "cool-prop": "the-coolest",
          "prop12345": "67890"
        }
      }"""

    val badRequest2 =
      json"""{
        "src": "gs://foo/bar",
        "dst": "gs://baz/qux",
        "metadata": {
          "cool-prop": "the-coolest",
          "prop12345": "67890"
        }
      }"""

    schema.validated(goodRequest1).isValid shouldBe true
    schema.validated(goodRequest2).isValid shouldBe true
    schema.validated(badRequest1).isValid shouldBe false
    schema.validated(badRequest2).isValid shouldBe false
  }

  it should "not lose information when converting JSON types" in {
    cloudCopySchema.as[QueueSchema].right.value.asJson shouldBe cloudCopySchema
  }
}
