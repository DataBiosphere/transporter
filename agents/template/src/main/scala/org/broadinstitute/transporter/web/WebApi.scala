package org.broadinstitute.transporter.web

import cats.effect.IO
import org.broadinstitute.transporter.info.InfoController
import org.http4s.{HttpApp, HttpRoutes}
import org.http4s.implicits._

/**
  * Thin REST API exposed by Transporter agents for status checks.
  *
  * @param infoController component which can query info about the
  *                       running agent
  */
class WebApi(infoController: InfoController) {

  import org.http4s.dsl.io._
  import org.http4s.circe.CirceEntityEncoder._

  private val routes = HttpRoutes.of[IO] {
    case GET -> Root / "status" =>
      infoController.status.flatMap { status =>
        if (status.ok) {
          Ok(status)
        } else {
          InternalServerError(status)
        }
      }
  }

  def app: HttpApp[IO] = routes.orNotFound
}
