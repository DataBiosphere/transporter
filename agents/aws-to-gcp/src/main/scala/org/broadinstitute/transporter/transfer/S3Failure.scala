package org.broadinstitute.transporter.transfer

import cats.effect.IO
import org.http4s.{Response, Status}

case class S3Failure(status: Status, body: String, message: String)
    extends Exception(
      s"""$message
         |Failed response returned $status:
         |$body""".stripMargin
    )

private[transfer] object S3Failure {

  /**
    * Convert a failed HTTP response into an error capturing info needed for debugging / retries.
    *
    * @param failedResponse the failed responses with a given status from an HTTP requests
    * @param additionalErrorMessage Any extra information the should be added to the raised error
    */
  def raise[A](failedResponse: Response[IO], additionalErrorMessage: String): IO[A] =
    failedResponse.body.compile.toChunk.flatMap { chunk =>
      val error = S3Failure(
        failedResponse.status,
        new String(chunk.toArray[Byte]),
        additionalErrorMessage
      )
      IO.raiseError(error)
    }
}
