package org.broadinstitute.transporter

import cats.effect.IO
import org.http4s.rho.PathBuilder
import org.http4s.rho.bits.PathAST
import org.http4s.rho.swagger.RouteDesc
import shapeless.HList

package object web {

  private[web] implicit class BuilderOps[HL <: HList](val builder: PathBuilder[IO, HL])
      extends AnyVal {

    def withDescription(description: String): PathBuilder[IO, HL] =
      new PathBuilder(
        builder.method,
        PathAST.MetaCons(builder.path, RouteDesc(description))
      )
  }
}
