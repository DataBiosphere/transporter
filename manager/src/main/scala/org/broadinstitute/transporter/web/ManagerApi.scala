package org.broadinstitute.transporter.web

import cats.effect.IO
import org.broadinstitute.transporter.ManagerApp
import org.broadinstitute.transporter.info.{ManagerStatus, ManagerVersion}
import org.http4s._
import org.http4s.rho.RhoRoutes
import org.http4s.rho.swagger.{syntax => swaggerSyntax}

class ManagerApi(app: ManagerApp) extends RhoRoutes[IO] {

  import swaggerSyntax.io._

  private val statusRoute =
    "Query operational status of the system" ** GET / "status"

  private val versionRoute =
    "Query version of the system" ** GET / "version"

  /*
   * NOTES on weirdness discovered in Rho that you'll see below:
   *
   *   1. Thunks are required on no-arg routes to prevent eager
   *      evaluation & result caching on app startup. This is
   *      desired behavior according to the Rho maintainers.
   *
   *   2. http4s provides a generic method for deriving `EntityEncoder`
   *      instances for any type with a circe `Encoder`, but for some
   *      reason combining it w/ the thunks causes divergence in
   *      implicit search. There are a couple maybe-related bugs filed
   *      in Rho; I'll also try to minimize & file a bug. Until then,
   *      we have to live with the boilerplate.
   */

  statusRoute |>> { () =>
    implicit val encoder: EntityEncoder[IO, ManagerStatus] =
      org.http4s.circe.jsonEncoderOf

    app.infoController.status.flatMap { status =>
      if (status.ok) {
        Ok(status)
      } else {
        InternalServerError(status)
      }
    }
  }

  versionRoute |>> { () =>
    implicit val encoder: EntityEncoder[IO, ManagerVersion] =
      org.http4s.circe.jsonEncoderOf

    app.infoController.version.flatMap(Ok(_))
  }
}
