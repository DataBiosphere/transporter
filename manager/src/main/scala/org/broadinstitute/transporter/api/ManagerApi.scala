package org.broadinstitute.transporter.api

import cats.effect.IO
import org.broadinstitute.transporter.ManagerApp
import org.http4s._
import org.http4s.circe.CirceEntityEncoder
import org.http4s.rho.RhoRoutes
import org.http4s.rho.bits.PathAST.{PathMatch, TypedPath}
import org.http4s.rho.swagger.models.Info
import org.http4s.rho.swagger.syntax.{io => swaggerIo}

class ManagerApi(appVersion: String, apiDocsPath: String, app: ManagerApp) {

  import swaggerIo._

  private val infoRoutes = new RhoRoutes[IO] with CirceEntityEncoder {

    // NOTE: Has to be a thunk here to prevent eager evaluation / caching.
    "Query operational status of the system" ** GET / "status" |>> { () =>
      app.statusController.status.map { status =>
        if (status.ok) {
          Ok.pure(status)
        } else {
          InternalServerError.pure(status)
        }
      }
    }

    // NOTE: The response here is evaluated at startup and cached,
    // since it's not a thunk.
    "Query version of the system" ** GET / "version" |>> Ok(appVersion)
  }

  def routes: HttpRoutes[IO] = {
    val swaggerMiddleware = swaggerIo.createRhoMiddleware(
      apiInfo = Info(
        title = "Transporter API",
        version = appVersion,
        description = Some("Bulk file-transfer system for data ingest / delivery.")
      ),
      apiPath = TypedPath(PathMatch(apiDocsPath))
    )

    infoRoutes.toRoutes(swaggerMiddleware)
  }
}
