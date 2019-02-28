package org.broadinstitute.transporter.queue

import cats.effect.IO
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.web.ErrorResponse
import org.http4s.{EntityEncoder, Method}
import org.http4s.rho.RhoRoutes
import org.http4s.rho.swagger.{syntax => swaggerSyntax}

class QueueRoutes(queueController: QueueController) extends RhoRoutes[IO] {
  import swaggerSyntax.io._

  private val log = Slf4jLogger.getLogger[IO]

  private def api(m: Method) = m / "api" / "transporter" / "v1"

  private def ISE(
    message: String,
    err: Throwable
  ): IO[InternalServerError.T[ErrorResponse]] =
    for {
      _ <- log.error(err)(message)
      ise <- InternalServerError(ErrorResponse(message))
    } yield ise

  private val createRoute =
    "Create a new queue of transfer requests" ** api(POST) / "queues"

  private val lookupRoute =
    "Fetch information about an existing queue" **
      api(GET) / "queues" / pathVar[String]("name")

  implicit val queueEncoder: EntityEncoder[IO, Queue] =
    org.http4s.circe.jsonEncoderOf

  implicit val errEncoder: EntityEncoder[IO, ErrorResponse] =
    org.http4s.circe.jsonEncoderOf

  createRoute.decoding(org.http4s.circe.jsonOf[IO, QueueRequest]) |>> {
    request: QueueRequest =>
      queueController.createQueue(request).attempt.map {
        case Right(queue) => Ok(queue)
        case Left(err)    => ISE(s"Failed to create queue ${request.name}", err)
      }
  }

  lookupRoute |>> { name: String =>
    queueController.lookupQueue(name).attempt.map {
      case Right(queue)                         => Ok(queue)
      case Left(QueueController.NoSuchQueue(q)) => NotFound(s"Queue $q does not exist")
      case Left(err)                            => ISE(s"Failed to lookup queue $name", err)
    }
  }
}
