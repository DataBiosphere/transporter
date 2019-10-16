package org.broadinstitute.transporter.transfer.config

import cats.data.Validated
import cats.implicits._
import com.typesafe.config.{ConfigList, ConfigObject, ConfigValue, ConfigValueType}
import io.circe.Decoder.Result
import io.circe._
import io.circe.syntax._
import org.everit.json.schema.{Schema, ValidationException}
import org.everit.json.schema.loader.SchemaLoader
import org.json.{JSONArray, JSONObject, JSONTokener}
import pureconfig.ConfigReader
import pureconfig.error.{CannotConvert, ConfigReaderFailures, ConvertFailure}

import scala.collection.JavaConverters._

/**
  * JSON schema for transfers submitted through the Transporter manager.
  *
  * @param json underlying JSON model which was used to build this
  *             instance. Stored by this class to make converting the
  *             schema back to circe JSON a simple field lookup
  *             instead of a convoluted toString + parse operation
  * @param validator schema parsed from the value of `json`,
  *                  capable of validating if a JSON payload conforms
  *                  to the specified rules
  */
class TransferSchema private (
  private val json: Json,
  private[this] val validator: Schema
) {

  /** Check if the given JSON conforms to this schema. */
  def validate(json: Json): Validated[ValidationException, Json] =
    Either
      .catchOnly[ValidationException] {
        validator.validate(TransferSchema.circeToEverit(json))
      }
      .toValidated
      .as(json)

  /*
   * Delegate `hashCode` and `equals` to `json` for testing.
   *
   * This ONLY works if the schema from `validator` is always
   * derived from the contents of `json`, which we enforce by
   * locking down the constructor.
   */

  override def hashCode(): Int = json.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case qs: TransferSchema => json.equals(qs.json)
    case _                  => false
  }

  override def toString: String = json.spaces2
}

object TransferSchema {

  /** Get the reference URL for the meta-schema of a JSON schema version. */
  private[transfer] def schemaUrl(v: Int): Json =
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

  implicit val encoder: Encoder[TransferSchema] = _.json

  implicit val decoder: Decoder[TransferSchema] = new Decoder[TransferSchema] {

    /**
      * Meta-schema matching valid draft-4, draft-6, and draft-7 JSON schemas.
      *
      * The supported JSON schema versions were chosen to match what our underlying
      * JSON schema validation library supports.
      */
    val schemaSchema: Schema = SchemaLoader.load {

      val supportedVersions = List(4, 6, 7)
      val schemaJson = JsonObject(
        "$schema" -> schemaUrl(supportedVersions.last),
        // NOTE: I tried using 'oneOf', but that caused spurious failures on
        // payloads that matched multiple drafts of the JSON schema spec.
        "anyOf" -> supportedVersions.map { v =>
          Json.obj("$ref" -> schemaUrl(v))
        }.asJson
      )

      circeObjToEverit(schemaJson)
    }

    override def apply(cursor: HCursor): Result[TransferSchema] =
      for {
        obj <- cursor.as[JsonObject]
        schema <- Either.catchNonFatal {
          val everitObj = circeObjToEverit(obj)
          schemaSchema.validate(everitObj)
          SchemaLoader.load(everitObj)
        }.leftMap(DecodingFailure.fromThrowable(_, Nil))
      } yield {
        new TransferSchema(obj.asJson, schema)
      }
  }

  implicit val reader: ConfigReader[TransferSchema] = cfg => {
    def convertValueUnsafe(value: ConfigValue): Json = value match {
      case obj: ConfigObject =>
        Json.fromFields(obj.asScala.mapValues(convertValueUnsafe))

      case list: ConfigList =>
        Json.fromValues(list.asScala.map(convertValueUnsafe))

      case _ =>
        (value.valueType, value.unwrapped) match {
          case (ConfigValueType.NULL, _) =>
            Json.Null
          case (ConfigValueType.NUMBER, int: java.lang.Integer) =>
            Json.fromInt(int)
          case (ConfigValueType.NUMBER, long: java.lang.Long) =>
            Json.fromLong(long)
          case (ConfigValueType.NUMBER, double: java.lang.Double) =>
            Json.fromDouble(double).getOrElse {
              throw new NumberFormatException(s"Invalid numeric string ${value.render}")
            }
          case (ConfigValueType.BOOLEAN, boolean: java.lang.Boolean) =>
            Json.fromBoolean(boolean)
          case (ConfigValueType.STRING, str: String) =>
            Json.fromString(str)

          case (valueType, _) =>
            throw new RuntimeException(s"No conversion for $valueType with value $value")
        }
    }

    Either
      .catchNonFatal(convertValueUnsafe(cfg.value))
      .flatMap(_.as[TransferSchema])
      .leftMap { err =>
        ConfigReaderFailures(
          ConvertFailure(
            CannotConvert(cfg.value.render(), "TransferSchema", err.getMessage),
            cfg.location,
            cfg.path
          )
        )
      }
  }
}
