package org.broadinstitute.transporter.queue

import cats.effect.IO
import org.http4s.{EntityEncoder, Method}
import org.http4s.rho.RhoRoutes
import org.http4s.rho.swagger.{syntax => swaggerSyntax}

class QueueRoutes(queueController: QueueController) extends RhoRoutes[IO] {
  import swaggerSyntax.io._

  private def api(m: Method) = m / "api" / "transporter" / "v1"

  private val createRoute =
    "Create a new queue of transfer requests" ** api(POST) / "queues"

  private val lookupRoute =
    "Fetch information about an existing queue" **
      api(GET) / "queues" / pathVar[String]("name")

  implicit val encoder: EntityEncoder[IO, Queue] =
    org.http4s.circe.jsonEncoderOf

  createRoute.decoding(org.http4s.circe.jsonOf[IO, QueueRequest]) |>> {
    request: QueueRequest =>
      queueController.createQueue(request).attempt.map {
        case Right(queue) => Ok(queue)
        // TODO: Consistent error JSON?
        // TODO: Decide where to log errors.
        case Left(_) => InternalServerError(s"Failed to create queue ${request.name}")
      }
  }

  lookupRoute |>> { name: String =>
    queueController.lookupQueue(name).attempt.map {
      case Right(queue)                         => Ok(queue)
      case Left(QueueController.NoSuchQueue(q)) => NotFound(s"Queue $q does not exist")
      case Left(_)                              => InternalServerError(s"Failed to lookup queue $name")
    }
  }
}
