package org.broadinstitute.transporter.web

import java.util.UUID

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.error.{
  InvalidRequest,
  NoSuchRequest,
  NoSuchTransfer
}
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.TransferController
import org.broadinstitute.transporter.web.config.OAuthConfig
import org.http4s.circe.{CirceEntityDecoder, CirceInstances}
import org.http4s.{EntityDecoder, EntityEncoder, Method}
import org.http4s.rho.RhoRoutes

/** Container for Transporter's API (eventually auth-protected) routes. */
class ApiRoutes(
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
  private implicit val statusEncoder: EntityEncoder[IO, RequestStatus] = jsonEncoderOf

  /** Build an API route prefix beginning with the given HTTP method. */
  private def api(m: Method) = {
    val base = m / "api" / "transporter" / "v1"
    if (withAuth) {
      base.withSecurityScopes(Map(OAuthConfig.AuthName -> OAuthConfig.AuthScopes))
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
          case InvalidRequest(failures) =>
            BadRequest(
              ErrorResponse(
                s"${failures.length} invalid transfers in request",
                failures.map(_.getMessage).toList
              )
            )
          case NoSuchRequest(id) =>
            NotFound(ErrorResponse(s"Request $id does not exist"))
          case NoSuchTransfer(reqId, id) =>
            NotFound(ErrorResponse(s"Transfer $reqId/$id does not exist"))
          case _ =>
            log
              .error(err)(iseMessage)
              .flatMap(_ => InternalServerError(ErrorResponse(iseMessage)))

        }
    }

  private val submitBatchTransfer =
    (api(POST) / "transfers").withDescription("Submit a new batch of transfer requests")

  private def transfer(m: Method) =
    api(m) / "transfers" / pathVar[UUID]("request-id")

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

  submitBatchTransfer.decoding(EntityDecoder[IO, BulkRequest]).bindAction {
    request: BulkRequest =>
      buildResponse(
        transferController.recordTransfer(request),
        "Failed to submit request"
      )
  }

  lookupRequestStatus.bindAction { requestId: UUID =>
    buildResponse(
      transferController.lookupRequestStatus(requestId),
      s"Failed to look up request status for $requestId"
    )
  }

  lookupRequestOutputs.bindAction { requestId: UUID =>
    buildResponse(
      transferController.lookupRequestOutputs(requestId),
      s"Failed to look up outputs for $requestId"
    )
  }

  lookupRequestFailures.bindAction { requestId: UUID =>
    buildResponse(
      transferController.lookupRequestFailures(requestId),
      s"Failed to look up failures for $requestId"
    )
  }

  reconsiderRequest.bindAction { requestId: UUID =>
    buildResponse(
      transferController.reconsiderRequest(requestId),
      s"Failed to reconsider transfers for $requestId"
    )
  }

  lookupTransferDetail.bindAction { (requestId: UUID, transferId: UUID) =>
    buildResponse(
      transferController.lookupTransferDetails(requestId, transferId),
      s"Failed to look up details for $transferId"
    )
  }
}
