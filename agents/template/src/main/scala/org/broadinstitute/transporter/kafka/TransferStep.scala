package org.broadinstitute.transporter.kafka

import io.circe.{Encoder, Json}
import io.circe.syntax._
import org.broadinstitute.transporter.kafka.TransferStreamBuilder.UnhandledErrorInfo
import org.broadinstitute.transporter.transfer.TransferResult

/** The possible states of a transfer request step as seen by an agent. */
sealed abstract class TransferStep[+I, +P, +O] extends Product with Serializable {
  def isDone: Boolean
  def message: Json
}

/**
  * Progress step, where the transfer is not done yet.
  * @param value The value/message to put in TransferMessage for the Progress state NOT INCLUDING the step count
  */
final case class Progress[P: Encoder](value: P)
    extends TransferStep[Nothing, P, Nothing] {
  def isDone = false

  def message: Json = value.asJson
}

/**
  * Done step, where the transfer has completed.
  * @param value The value/message to put in TransferMessage for the Done step
  */
final case class Done[O: Encoder](value: O) extends TransferStep[Nothing, Nothing, O] {
  def isDone = true

  def message: Json = (TransferResult.Success: TransferResult, value).asJson
}

/**
  * Expanded step, where the transfer needs to be expanded into multiple transfers.
  * @param value The value/message to put in TransferMessage for the Expanded step
  */
final case class Expanded[I: Encoder](value: List[I])
    extends TransferStep[I, Nothing, Nothing] {
  def isDone = true

  def message: Json = (TransferResult.Expanded: TransferResult, value).asJson
}

/**
  * Failure step, where something has gone wrong.
  * @param value The value/message to put in TransferMessage for the Failure step
  */
final case class Failure(value: Throwable)
    extends TransferStep[Nothing, Nothing, Nothing] {
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
