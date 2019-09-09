package org.broadinstitute.transporter.kafka

import io.circe.{Encoder, Json}
import io.circe.syntax._
import org.broadinstitute.transporter.kafka.TransferStreamBuilder.UnhandledErrorInfo
import org.broadinstitute.transporter.transfer.TransferResult

/** The possible states of a transfer request as seen by an agent. */
sealed abstract class TransferState[+I, +P, +O] extends Product with Serializable {
  def isDone: Boolean
  def message: Json
}

final case class Progress[P: Encoder](value: P)
    extends TransferState[Nothing, P, Nothing] {
  def isDone = false

  def message: Json = (0, value).asJson
}

final case class Done[O: Encoder](value: O) extends TransferState[Nothing, Nothing, O] {
  def isDone = true

  def message: Json = (TransferResult.Success: TransferResult, value).asJson
}

final case class Expanded[I: Encoder](value: List[I])
    extends TransferState[I, Nothing, Nothing] {
  def isDone = true

  def message: Json = (TransferResult.Expanded: TransferResult, value).asJson
}

final case class Failure(value: Throwable)
    extends TransferState[Nothing, Nothing, Nothing] {
  def isDone = true

  def message: Json =
    (
      TransferResult.FatalFailure: TransferResult,
      UnhandledErrorInfo(
        s"Failed to run next step",
        Option(value.getMessage)
      )
    ).asJson
}
