package org.broadinstitute.transporter.web

import java.util.UUID

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.queue.api.{Queue, QueueRequest}
import org.broadinstitute.transporter.queue.QueueController
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.TransferController
import org.http4s.circe.{CirceEntityDecoder, CirceInstances}
import org.http4s.{EntityDecoder, EntityEncoder, Method}
import org.http4s.rho.RhoRoutes

/** Container for Transporter's API (eventually auth-protected) routes. */
class ApiRoutes(queueController: QueueController, transferController: TransferController)
    extends RhoRoutes[IO]
    with CirceEntityDecoder
    with CirceInstances {

  private val log = Slf4jLogger.getLogger[IO]

  private implicit val ackEncoder: EntityEncoder[IO, RequestAck] = jsonEncoderOf
  private implicit val errEncoder: EntityEncoder[IO, ErrorResponse] = jsonEncoderOf
  private implicit val queueEncoder: EntityEncoder[IO, Queue] = jsonEncoderOf
  private implicit val statusEncoder: EntityEncoder[IO, RequestStatus] = jsonEncoderOf
  private implicit val messagesEncoder: EntityEncoder[IO, RequestMessages] = jsonEncoderOf
  private implicit val detailsEncoder: EntityEncoder[IO, TransferDetails] = jsonEncoderOf

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

  private val submitBatchTransfer =
    (api(POST) / "queues" / pathVar[String]("name") / "transfers")
      .withDescription("Submit a new batch of transfer requests to a queue")

  private def transfer(m: Method) =
    api(m) / "queues" / pathVar[String]("name") / "transfers" / pathVar[UUID](
      "request-id"
    )

  private val lookupRequestStatus = (transfer(GET) / "status")
    .withDescription("Get the current summary status of a transfer request")

  private val lookupRequestOutputs = (transfer(GET) / "outputs")
    .withDescription("Get the outputs of successful transfers from a request")

  private val lookupRequestFailures = (transfer(GET) / "failures")
    .withDescription("Get the outputs of failed transfers from a request")

  private val lookupTransferDetail =
    (transfer(GET) / "detail" / pathVar[UUID]("transfer-id"))
      .withDescription("Get detailed info about a single transfer from a request")

  /*
   * ROUTE BINDINGS GO BELOW HERE.
   *
   * Rho's DSL bundles up top-level route definitions at the end of the class
   * body into a collection, which it then passes into the superclass constructor.
   */

  createQueue.decoding(EntityDecoder[IO, QueueRequest]).bindAction {
    request: QueueRequest =>
      queueController.createQueue(request).attempt.map {
        case Right(queue) => Ok(queue)
        case Left(e: QueueController.QueueAlreadyExists) =>
          Conflict(ErrorResponse(e.getMessage))
        case Left(err) => ISE(s"Failed to create queue ${request.name}", err)
      }
  }

  lookupQueue.bindAction { name: String =>
    queueController.lookupQueue(name).attempt.map {
      case Right(queue) => Ok(queue)
      case Left(e: QueueController.NoSuchQueue) =>
        NotFound(ErrorResponse(e.getMessage))
      case Left(err) => ISE(s"Failed to lookup queue $name", err)
    }
  }

  submitBatchTransfer.decoding(EntityDecoder[IO, BulkRequest]).bindAction {
    (name: String, request: BulkRequest) =>
      transferController.submitTransfer(name, request).attempt.map {
        case Right(ack) => Ok(ack)
        case Left(QueueController.NoSuchQueue(_)) =>
          NotFound(ErrorResponse(s"Queue $name does not exist"))
        case Left(TransferController.InvalidRequest(_)) =>
          BadRequest(
            ErrorResponse(s"Submission does not match expected schema for queue $name")
          )
        case Left(err) => ISE(s"Failed to submit request to $name", err)
      }
  }

  lookupRequestStatus.bindAction { (queueName: String, requestId: UUID) =>
    transferController
      .lookupRequestStatus(queueName, requestId)
      .attempt
      .map {
        case Right(status) => Ok(status)
        case Left(e: QueueController.NoSuchQueue) =>
          NotFound(ErrorResponse(e.getMessage))
        case Left(e: TransferController.NoSuchRequest) =>
          NotFound(ErrorResponse(e.getMessage))
        case Left(err) => ISE(s"Failed to look up request status for $requestId", err)
      }
  }

  lookupRequestOutputs.bindAction { (queueName: String, requestId: UUID) =>
    transferController
      .lookupRequestOutputs(queueName, requestId)
      .attempt
      .map {
        case Right(messages) => Ok(messages)
        case Left(e: QueueController.NoSuchQueue) =>
          NotFound(ErrorResponse(e.getMessage))
        case Left(e: TransferController.NoSuchRequest) =>
          NotFound(ErrorResponse(e.getMessage))
        case Left(err) => ISE(s"Failed to look up outputs for $requestId", err)
      }
  }

  lookupRequestFailures.bindAction { (queueName: String, requestId: UUID) =>
    transferController
      .lookupRequestFailures(queueName, requestId)
      .attempt
      .map {
        case Right(messages) => Ok(messages)
        case Left(e: QueueController.NoSuchQueue) =>
          NotFound(ErrorResponse(e.getMessage))
        case Left(e: TransferController.NoSuchRequest) =>
          NotFound(ErrorResponse(e.getMessage))
        case Left(err) => ISE(s"Failed to look up failures for $requestId", err)
      }
  }

  lookupTransferDetail.bindAction {
    (queueName: String, requestId: UUID, transferId: UUID) =>
      transferController
        .lookupTransferDetails(queueName, requestId, transferId)
        .attempt
        .map {
          case Right(details) => Ok(details)
          case Left(e: QueueController.NoSuchQueue) =>
            NotFound(ErrorResponse(e.getMessage))
          case Left(e: TransferController.NoSuchRequest) =>
            NotFound(ErrorResponse(e.getMessage))
          case Left(e: TransferController.NoSuchTransfer) =>
            NotFound(ErrorResponse(e.getMessage))
          case Left(err) => ISE(s"Failed to look up details for $transferId", err)
        }
  }
}
