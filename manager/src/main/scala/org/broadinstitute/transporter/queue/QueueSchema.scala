package org.broadinstitute.transporter.queue

import cats.data.Validated
import cats.implicits._
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import org.everit.json.schema.Schema
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONArray, JSONObject, JSONTokener}

class QueueSchema private (
  private val json: JsonObject,
  private[this] val validator: Schema
) {

  def validated(json: Json): Validated[Throwable, Json] =
    Validated.catchNonFatal {
      validator.validate(QueueSchema.circeToEverit(json))
    }.as(json)
}

object QueueSchema {

  private[queue] def schemaUrl(v: Int): Json =
    s"http://json-schema.org/draft-0$v/schema".asJson

  private def circeObjToEverit(js: JsonObject): JSONObject =
    new JSONObject(new JSONTokener(js.asJson.noSpaces))

  private def circeToEverit(js: Json): Object =
    js.fold(
      jsonNull = JSONObject.NULL,
      jsonBoolean = boolean2Boolean,
      jsonNumber = n => n.toLong.fold[Number](double2Double(n.toDouble))(long2Long),
      jsonString = identity,
      jsonArray = _ => new JSONArray(new JSONTokener(js.noSpaces)),
      jsonObject = circeObjToEverit
    )

  implicit val encoder: Encoder[QueueSchema] = _.json.asJson

  implicit val decoder: Decoder[QueueSchema] = new Decoder[QueueSchema] {

    val schemaSchema: Schema = SchemaLoader.load {

      val supportedVersions = List(4, 6, 7)
      val schemaJson = JsonObject(
        "$schema" -> schemaUrl(supportedVersions.last),
        "anyOf" -> supportedVersions.map { v =>
          Json.obj("$ref" -> schemaUrl(v))
        }.asJson
      )

      circeObjToEverit(schemaJson)
    }

    override def apply(cursor: HCursor): Result[QueueSchema] =
      for {
        json <- cursor.as[JsonObject]
        schema <- Either.catchNonFatal {
          val obj = circeObjToEverit(json)
          schemaSchema.validate(obj)
          SchemaLoader.load(obj)
        }.leftMap(DecodingFailure.fromThrowable(_, Nil))
      } yield {
        new QueueSchema(json, schema)
      }
  }
}
