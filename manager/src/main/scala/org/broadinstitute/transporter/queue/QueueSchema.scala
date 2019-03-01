package org.broadinstitute.transporter.queue

import cats.data.Validated
import cats.implicits._
import doobie.postgres.circe.Instances
import doobie.util.{Get, Put}
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import org.everit.json.schema.Schema
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONArray, JSONObject, JSONTokener}

/**
  * JSON schema wrapper facilitating conversions to/from circe's
  * [[Json]] models.
  *
  * @param json underlying JSON model which was used to build this
  *             instance. Stored by this class to make converting the
  *             schema back to circe JSON a simple field lookup
  *             instead of a convoluted toString + parse operation
  * @param validator schema parsed from the value of `json`,
  *                  capable of validating if a JSON payload conforms
  *                  to the specified rules
  */
class QueueSchema private (
  private val json: Json,
  private[this] val validator: Schema
) {

  /** Check if the given JSON conforms to this schema. */
  def validate(json: Json): Validated[Throwable, Json] =
    Validated.catchNonFatal {
      validator.validate(QueueSchema.circeToEverit(json))
    }.as(json)

  /*
   * Delegate `hashCode` and `equals` to `json` for testing.
   *
   * This ONLY works if the schema from `validator` is always
   * derived from the contents of `json`, which we enforce by
   * locking down the constructor.
   */

  override def hashCode(): Int = json.hashCode()
  override def equals(obj: Any): Boolean = obj match {
    case qs: QueueSchema => json.equals(qs.json)
    case _               => false
  }
}

object QueueSchema extends Instances.JsonInstances {

  /** Get the reference URL for the meta-schema of a JSON schema version. */
  private[queue] def schemaUrl(v: Int): Json =
    s"http://json-schema.org/draft-0$v/schema".asJson

  /** Convert a circe JSON object to the class expected by our schema validation lib. */
  private def circeObjToEverit(js: JsonObject): JSONObject =
    new JSONObject(new JSONTokener(js.asJson.noSpaces))

  /**
    * Convert circe JSON to a type which can be validated by our schema validation lib.
    *
    * The validation API takes [[Object]] and checks the input type via reflection,
    * so the output values here were determined by hunting through the source code to
    * see what's allowable.
    *
    * @see https://github.com/everit-org/json-schema/blob/master/core/src/main/java/org/everit/json/schema/ValidatingVisitor.java#L22-L29
    */
  private def circeToEverit(js: Json): Object =
    js.fold(
      jsonNull = JSONObject.NULL,
      jsonBoolean = boolean2Boolean,
      jsonNumber = n => n.toLong.fold[Number](double2Double(n.toDouble))(long2Long),
      jsonString = identity,
      jsonArray = _ => new JSONArray(new JSONTokener(js.noSpaces)),
      jsonObject = circeObjToEverit
    )

  implicit val encoder: Encoder[QueueSchema] = _.json

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
        obj <- cursor.as[JsonObject]
        schema <- Either.catchNonFatal {
          val everitObj = circeObjToEverit(obj)
          schemaSchema.validate(everitObj)
          SchemaLoader.load(everitObj)
        }.leftMap(DecodingFailure.fromThrowable(_, Nil))
      } yield {
        new QueueSchema(obj.asJson, schema)
      }
  }

  implicit val get: Get[QueueSchema] = pgDecoderGet

  implicit val put: Put[QueueSchema] = pgEncoderPut
}
