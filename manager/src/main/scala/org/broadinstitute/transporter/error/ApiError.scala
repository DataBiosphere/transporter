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
  case class NoSuchRequest(id: UUID) extends ApiError

  implicit val nsrDecoder: Decoder[NoSuchRequest] = deriveDecoder
  implicit val nsrEncoder: Encoder[NoSuchRequest] = deriveEncoder

  /**
    * Exception used to mark when a user attempt to interact
    * with a nonexistent transfer.
    */
  case class NoSuchTransfer(requestId: UUID, id: UUID) extends ApiError

  implicit val nstDecoder: Decoder[NoSuchTransfer] = deriveDecoder
  implicit val nstEncoder: Encoder[NoSuchTransfer] = deriveEncoder

  /** TODO */
  case class UnhandledError(message: String) extends ApiError

  implicit val ueDecoder: Decoder[UnhandledError] = deriveDecoder
  implicit val ueEncoder: Encoder[UnhandledError] = deriveEncoder
}
