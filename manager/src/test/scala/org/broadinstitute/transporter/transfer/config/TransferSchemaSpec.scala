package org.broadinstitute.transporter.transfer.config

import io.circe.literal._
import io.circe.syntax._
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class TransferSchemaSpec extends FlatSpec with Matchers with EitherValues {
  behavior of "QueueSchema"

  private val draft4Schema = json"""{
    "$$schema": ${TransferSchema.schemaUrl(4)},
    "id": "#draft4-example",
    "type": "object"
  }"""

  private val draft6Schema = json"""{
    "$$schema": ${TransferSchema.schemaUrl(6)},
    "$$id": "#draft6-example",
    "type": "object"
  }"""

  private val draft7Schema = json"""{
    "$$schema": ${TransferSchema.schemaUrl(7)},
    "$$id": "#draft7-example",
    "type": ["object", "boolean"]
  }"""

  private val cloudCopySchema = json"""{
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

  private val echoSchema = json"""{
    "$$schema": "http://json-schema.org/draft-04/schema",
    "type": "object",
    "properties": {
      "message": { "type":  "string" },
      "fail": { "type":  "boolean" }
    },
    "required": ["message", "fail"],
    "additionalProperties": false
  }"""

  it should "parse draft-4 JSON schemas" in {
    val schema = draft4Schema.as[TransferSchema].right.value
    schema.validate(json"{}").isValid shouldBe true
    schema.validate(json"[]").isValid shouldBe false
  }

  it should "parse draft-6 JSON schemas" in {
    val schema = draft6Schema.as[TransferSchema].right.value
    schema.validate(json"{}").isValid shouldBe true
    schema.validate(json"[]").isValid shouldBe false
  }

  it should "parse draft-7 JSON schemas" in {
    val schema = draft7Schema.as[TransferSchema].right.value
    schema.validate(json"{}").isValid shouldBe true
    schema.validate(json"[]").isValid shouldBe false
  }

  it should "parse real JSON schemas" in {
    List(cloudCopySchema, echoSchema).foreach { schema =>
      schema.as[TransferSchema].isRight shouldBe true
    }
  }

  it should "parse JSON schemas into validators" in {
    val schema = cloudCopySchema.as[TransferSchema].right.value

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

    schema.validate(goodRequest1).isValid shouldBe true
    schema.validate(goodRequest2).isValid shouldBe true
    schema.validate(badRequest1).isValid shouldBe false
    schema.validate(badRequest2).isValid shouldBe false
  }

  it should "not lose information when converting JSON types" in {
    cloudCopySchema.as[TransferSchema].right.value.asJson shouldBe cloudCopySchema
  }

  it should "validate null" in {
    val schema = json"""{ "type": "null" }""".as[TransferSchema].right.value
    schema.validate(json"null").isValid shouldBe true
    schema.validate(json"{}").isValid shouldBe false
  }

  it should "validate numbers" in {
    val schema = json"""{ "type": "number" }""".as[TransferSchema].right.value
    schema.validate(json"1.234567890").isValid shouldBe true
    schema.validate(json"null").isValid shouldBe false
  }

  it should "validate strings" in {
    val schema = json"""{ "type": "string" }""".as[TransferSchema].right.value
    schema.validate(json""""foo"""").isValid shouldBe true
    schema.validate(json"[]").isValid shouldBe false
  }

  it should "validate arrays" in {
    val schema = json"""{ "type": "array", "items": { "type": "number" } }"""
      .as[TransferSchema]
      .right
      .value
    schema.validate(json"[1, 2, 3, 4]").isValid shouldBe true
    schema.validate(json"""["a", "b", "c", "d"]""").isValid shouldBe false
    schema.validate(json"{}").isValid shouldBe false
  }
}
