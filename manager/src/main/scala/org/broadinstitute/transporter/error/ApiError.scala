package org.broadinstitute.transporter.error

import java.util.UUID

import cats.data.NonEmptyList
import io.circe.{Decoder, Encoder}
import io.circe.derivation.{deriveDecoder, deriveEncoder}

sealed trait ApiError extends Exception with Product with Serializable

object ApiError {

  /**
    * Exception used to mark when a user submits transfers
    * that don't match the manager's expected schema.
    */
  case class InvalidRequest(failures: NonEmptyList[String]) extends ApiError

  implicit val irDecoder: Decoder[InvalidRequest] = deriveDecoder
  implicit val irEncoder: Encoder[InvalidRequest] = deriveEncoder

  /**
    * Exception used to mark when a user attempts to interact
    * with a nonexistent request.
    */
  case class NotFound(requestId: UUID, transferId: Option[UUID] = None) extends ApiError

  implicit val nsrDecoder: Decoder[NotFound] = deriveDecoder
  implicit val nsrEncoder: Encoder[NotFound] = deriveEncoder

  /**
    * Exception used to mark when a user attempts to modify a resource
    * but the request could not be completed due to a conflict with the current state of the resource.
    */
  case class Conflict(requestId: UUID, transferId: Option[UUID] = None) extends ApiError

  implicit val cDecoder: Decoder[Conflict] = deriveDecoder
  implicit val cEncoder: Encoder[Conflict] = deriveEncoder

  /** Catch-all error model used to wrap unhandled exceptions from business logic. */
  case class UnhandledError(message: String) extends ApiError

  implicit val ueDecoder: Decoder[UnhandledError] = deriveDecoder
  implicit val ueEncoder: Encoder[UnhandledError] = deriveEncoder
}
