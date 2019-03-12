package org.broadinstitute.transporter.web

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.queue.{Queue, QueueController, QueueRequest}
import org.broadinstitute.transporter.transfer.{
  TransferAck,
  TransferController,
  TransferRequest
}
import org.http4s.circe.{CirceEntityDecoder, CirceInstances}
import org.http4s.{EntityDecoder, EntityEncoder, Method}
import org.http4s.rho.RhoRoutes

/** Container for Transporter's API (eventually auth-protected) routes. */
class ApiRoutes(queueController: QueueController, transferController: TransferController)
    extends RhoRoutes[IO]
    with CirceEntityDecoder
    with CirceInstances {

  private val log = Slf4jLogger.getLogger[IO]

  private implicit val ackEncoder: EntityEncoder[IO, TransferAck] = jsonEncoderOf
  private implicit val errEncoder: EntityEncoder[IO, ErrorResponse] = jsonEncoderOf
  private implicit val queueEncoder: EntityEncoder[IO, Queue] = jsonEncoderOf

  /** Build an API route prefix beginning with the given HTTP method. */
  private def api(m: Method) = m / "api" / "transporter" / "v1"

  /**
    * Build a 500 error response containing the given message,
    * using our uniform error response model.
    *
    * Logs the error causing the 500 response.
    */
  private def ISE(message: String, err: Throwable) =
    for {
      _ <- log.error(err)(message)
      ise <- InternalServerError(ErrorResponse(message))
    } yield ise

  private val createQueue = (api(POST) / "queues")
    .withDescription("Create a new queue of transfer requests")

  private val lookupQueue = (api(GET) / "queues" / pathVar[String]("name"))
    .withDescription("Fetch information about an existing queue")

  private val submitTransfers =
    (api(POST) / "queues" / pathVar[String]("name") / "transfers")
      .withDescription("Submit a new batch of transfer requests to a queue")

  /*
   * ROUTE BINDINGS GO BELOW HERE.
   *
   * Rho's DSL bundles up top-level route definitions at the end of the class
   * body into a collection, which it then passes into the superclass constructor.
   */

  createQueue.decoding(EntityDecoder[IO, QueueRequest]) |>> { request: QueueRequest =>
    queueController.createQueue(request).attempt.map {
      case Right(queue) => Ok(queue)
      case Left(err)    => ISE(s"Failed to create queue ${request.name}", err)
    }
  }

  lookupQueue |>> { name: String =>
    queueController.lookupQueue(name).attempt.map {
      case Right(Some(queue)) => Ok(queue)
      case Right(None)        => NotFound(ErrorResponse(s"Queue $name does not exist"))
      case Left(err)          => ISE(s"Failed to lookup queue $name", err)
    }
  }

  submitTransfers.decoding(EntityDecoder[IO, TransferRequest]) |>> {
    (name: String, request: TransferRequest) =>
      transferController.submitTransfer(name, request).attempt.map {
        case Right(ack) => Ok(ack)
        case Left(TransferController.NoSuchQueue(_)) =>
          NotFound(ErrorResponse(s"Queue $name does not exist"))
        case Left(TransferController.InvalidRequest(_)) =>
          BadRequest(
            ErrorResponse(s"Submission does not match expected schema for queue $name")
          )
        case Left(err) => ISE(s"Failed to submit request to $name", err)
      }
  }
}
