package org.broadinstitute.transporter

import cats.effect.IO
import org.http4s.rho.{CompileRoutes, PathBuilder, RouteExecutable}
import org.http4s.rho.bits.{HListToFunc, PathAST}
import org.http4s.rho.swagger.RouteDesc
import shapeless.HList

import scala.language.higherKinds

package object web {

  /** Extension methods for Rho's route-builder type, to make writing Swagger less painful. */
  private[web] implicit class BuilderOps[HL <: HList](val builder: PathBuilder[IO, HL])
      extends AnyVal {

    /** Add a description to the Swagger generated for the wrapped route. */
    def withDescription(description: String): PathBuilder[IO, HL] =
      new PathBuilder(
        builder.method,
        PathAST.MetaCons(builder.path, RouteDesc(description))
      )
  }

  /** Extension methods for Rho's "executable route" type, to make tinkering less painful / magical. */
  private[web] implicit class RouteOps[F[_], T <: HList](val route: RouteExecutable[F, T])
      extends AnyVal {

    /**
      * Bind a function to run every time the wrapped route is called.
      *
      * The arg names and types are one-to-one copies from the `|>>` method
      * in Rho, which doesn't provide a non-symbolic alias.
      */
    def bindAction[U, R](action: U)(
      implicit hltf: HListToFunc[F, T, U],
      srvc: CompileRoutes[F, R]
    ): R = route |>> action
  }

}
