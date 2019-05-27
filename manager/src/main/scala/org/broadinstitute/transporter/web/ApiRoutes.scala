package org.broadinstitute.transporter.web

import java.util.UUID

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.error.{
  InvalidQueueParameter,
  InvalidRequest,
  NoSuchQueue,
  NoSuchRequest,
  NoSuchTransfer,
  QueueAlreadyExists
}
import org.broadinstitute.transporter.queue.api.{Queue, QueueParameters, QueueRequest}
import org.broadinstitute.transporter.queue.QueueController
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.TransferController
import org.broadinstitute.transporter.web.config.OAuthConfig
import org.http4s.circe.{CirceEntityDecoder, CirceInstances}
import org.http4s.{EntityDecoder, EntityEncoder, Method}
import org.http4s.rho.RhoRoutes
import org.http4s.rho.swagger._

/** Container for Transporter's API (eventually auth-protected) routes. */
class ApiRoutes(
  queueController: QueueController,
  transferController: TransferController,
  withAuth: Boolean
) extends RhoRoutes[IO]
    with CirceEntityDecoder
    with CirceInstances {

  private val log = Slf4jLogger.getLogger[IO]

  private implicit val ackEncoder: EntityEncoder[IO, RequestAck] = jsonEncoderOf
  private implicit val detailsEncoder: EntityEncoder[IO, TransferDetails] = jsonEncoderOf
  private implicit val errEncoder: EntityEncoder[IO, ErrorResponse] = jsonEncoderOf
  private implicit val messagesEncoder: EntityEncoder[IO, RequestInfo] = jsonEncoderOf
  private implicit val queueEncoder: EntityEncoder[IO, Queue] = jsonEncoderOf
  private implicit val statusEncoder: EntityEncoder[IO, RequestStatus] = jsonEncoderOf

  /** Build an API route prefix beginning with the given HTTP method. */
  private def api(m: Method) = {
    val base = m / "api" / "transporter" / "v1"
    if (withAuth) {
      Map(OAuthConfig.AuthName -> OAuthConfig.AuthScopes) ^^ base
    } else {
      base
    }
  }

  /** Build an appropriate response for an error from the main app code. */
  private def buildResponse[Out](exec: IO[Out], iseMessage: String)(
    implicit e: EntityEncoder[IO, Out]
  ) =
    exec.attempt.flatMap {
      case Right(out) => Ok(out)
      case Left(err) =>
        err match {
          case QueueAlreadyExists(name) =>
            Conflict(ErrorResponse(s"Queue $name already exists"))
          case InvalidQueueParameter(name, message) =>
            BadRequest(ErrorResponse(s"Invalid parameter for queue $name: $message"))
          case InvalidRequest(failures) =>
            BadRequest(
              ErrorResponse(
                s"${failures.length} invalid transfers in request",
                failures.map(_.getMessage).toList
              )
            )
          case NoSuchQueue(name) =>
            NotFound(ErrorResponse(s"Queue $name does not exist"))
          case NoSuchRequest(name, id) =>
            NotFound(ErrorResponse(s"Request $name/$id does not exist"))
          case NoSuchTransfer(name, reqId, id) =>
            NotFound(ErrorResponse(s"Transfer $name/$reqId/$id does not exist"))
          case _ =>
            log
              .error(err)(iseMessage)
              .flatMap(_ => InternalServerError(ErrorResponse(iseMessage)))

        }
    }

  private val createQueue = (api(POST) / "queues")
    .withDescription("Create a new queue of transfer requests")

  private val patchQueue = (api(PATCH) / "queues" / pathVar[String]("name"))
    .withDescription("Update paramaters of an existing queue")

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

  private val reconsiderRequest = (transfer(PUT) / "reconsider")
    .withDescription("Reset the state of all failed transfers in a request to 'pending'")

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
      buildResponse(
        queueController.createQueue(request),
        s"Failed to create queue ${request.name}"
      )
  }

  patchQueue.decoding(EntityDecoder[IO, QueueParameters]).bindAction {
    (name: String, params: QueueParameters) =>
      buildResponse(
        queueController.patchQueue(name, params),
        s"Failed to update parameters for queue $name"
      )
  }

  lookupQueue.bindAction { name: String =>
    buildResponse(queueController.lookupQueue(name), s"Failed to lookup queue $name")
  }

  submitBatchTransfer.decoding(EntityDecoder[IO, BulkRequest]).bindAction {
    (name: String, request: BulkRequest) =>
      buildResponse(
        transferController.recordTransfer(name, request),
        s"Failed to submit request to $name"
      )
  }

  lookupRequestStatus.bindAction { (queueName: String, requestId: UUID) =>
    buildResponse(
      transferController.lookupRequestStatus(queueName, requestId),
      s"Failed to look up request status for $requestId"
    )
  }

  lookupRequestOutputs.bindAction { (queueName: String, requestId: UUID) =>
    buildResponse(
      transferController.lookupRequestOutputs(queueName, requestId),
      s"Failed to look up outputs for $requestId"
    )
  }

  lookupRequestFailures.bindAction { (queueName: String, requestId: UUID) =>
    buildResponse(
      transferController.lookupRequestFailures(queueName, requestId),
      s"Failed to look up failures for $requestId"
    )
  }

  reconsiderRequest.bindAction { (queueName: String, requestId: UUID) =>
    buildResponse(
      transferController.reconsiderRequest(queueName, requestId),
      s"Failed to reconsider transfers for $requestId"
    )
  }

  lookupTransferDetail.bindAction {
    (queueName: String, requestId: UUID, transferId: UUID) =>
      buildResponse(
        transferController.lookupTransferDetails(queueName, requestId, transferId),
        s"Failed to look up details for $transferId"
      )
  }
}
