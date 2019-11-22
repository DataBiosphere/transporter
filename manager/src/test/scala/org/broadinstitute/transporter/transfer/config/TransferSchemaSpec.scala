package org.broadinstitute.transporter.transfer.config

import io.circe.Json
import io.circe.literal._
import io.circe.syntax._
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class TransferSchemaSpec extends FlatSpec with Matchers with EitherValues {
  behavior of "TransferSchema"

  def schemaUrl(v: Int): Json =
    s"http://json-schema.org/draft-0$v/schema".asJson

  private val draft4Schema = json"""{
    "$$schema": ${schemaUrl(4)},
    "id": "#draft4-example",
    "type": "object"
  }"""

  private val draft6Schema = json"""{
    "$$schema": ${schemaUrl(6)},
    "$$id": "#draft6-example",
    "type": "object"
  }"""

  private val draft7Schema = json"""{
    "$$schema": ${schemaUrl(7)},
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
    draft4Schema.as[TransferSchema].right.value
  }

  it should "parse draft-6 JSON schemas" in {
    draft6Schema.as[TransferSchema].right.value
  }

  it should "parse draft-7 JSON schemas" in {
    draft7Schema.as[TransferSchema].right.value
  }

  it should "parse real JSON schemas" in {
    List(cloudCopySchema, echoSchema).foreach { schema =>
      schema.as[TransferSchema].isRight shouldBe true
    }
  }

  it should "not lose information when converting JSON types" in {
    cloudCopySchema.as[TransferSchema].right.value.asJson shouldBe cloudCopySchema
  }

  it should "convert to an example Json format" in {
    val theJson = json"""{
                       "title": "Example schema",
                       "type": "object",
                       "properties": {
                          "prop1": {
                              "description": "First example property",
                              "type": "string"
                              },
                          "prop2": {
                              "description": "Second example property",
                              "type": "string"
                              }
                          },
                       "required": ["prop1", "prop2"]
                       }"""
    val schema = theJson.as[TransferSchema].right.value
    val targetJson =
      json"""{
          "prop1": "string",
          "prop2": "string"
          }"""
    schema.asExample shouldBe targetJson
  }

  it should "handle example schemas with no properties" in {
    val theJson = json"""{ "type": "object" }"""
    val schema = theJson.as[TransferSchema].right.value
    schema.asExample shouldBe json"{}"
  }
}
