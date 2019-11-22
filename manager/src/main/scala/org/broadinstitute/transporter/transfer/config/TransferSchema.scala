package org.broadinstitute.transporter.transfer.config

import cats.data.Validated
import cats.implicits._
import com.typesafe.config.{ConfigList, ConfigObject, ConfigValue, ConfigValueType}
import io.circe._
import io.circe.syntax._
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
  */
class TransferSchema private (private val json: JsonObject) {

  /** Check if the given JSON conforms to this schema. */
  def validate(json: Json): Validated[Exception, Json] =
    // FIXME: The Everit JSON schema library started inexplicably failing to
    // parse JSON, with no version bump... max sketchy.
    // The same behavior is showing up in the test suite of circe-json-schema,
    // which also depends on Everit under the hood.
    Validated.Valid(json)

  // Delegate `hashCode` and `equals` to `json` for testing.

  override def hashCode(): Int = json.hashCode()

  override def equals(obj: Any): Boolean = obj match {
    case qs: TransferSchema => json.equals(qs.json)
    case _                  => false
  }

  override def toString: String = json.asJson.spaces2

  def asExample: Json = {
    json("properties").fold(Json.obj()) { properties =>
      properties.mapObject { propObject =>
        propObject.mapValues { property =>
          property.withObject(_.apply("type").getOrElse(Json.fromString("string")))
        }
      }
    }
  }
}

object TransferSchema {
  implicit val encoder: Encoder[TransferSchema] = _.json.asJson

  implicit val decoder: Decoder[TransferSchema] =
    _.as[JsonObject].map(new TransferSchema(_))

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
