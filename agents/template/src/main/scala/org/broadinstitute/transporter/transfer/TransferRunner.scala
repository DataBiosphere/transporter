package org.broadinstitute.transporter.transfer

import io.circe.Json

/**
  * Component capable of actually running data transfers.
  *
  * Agents are expected to "fill in" an instance of this interface
  * to handle specific storage source / destination pairs.
  */
trait TransferRunner {

  type In

  type Progress

  type Out

  def decodeInput(json: Json): Either[Throwable, In]

  def decodeProgress(json: Json): Either[Throwable, Progress]

  def encodeProgress(progress: Progress): Json

  def encodeOutput(output: Out): Json

  /**
    * Initialize the transfer described by the given request, and
    * emit enough information to push the first chunk of data.
    */
  def initialize(request: In): Progress

  /**
    * Push the next chunk of data into an initialized transfer, either
    * completing the transfer or emitting enough information to push the
    * following chunk.
    */
  def step(progress: Progress): Either[Progress, Out]

  def retriable(err: Throwable): Boolean
}
