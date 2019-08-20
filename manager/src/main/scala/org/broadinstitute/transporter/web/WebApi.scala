package org.broadinstitute.transporter.web

import java.util.UUID

import cats.data.{Kleisli, NonEmptyList}
import cats.effect.{ContextShift, IO}
import cats.implicits._
import enumeratum.{Enum, EnumEntry}
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.broadinstitute.transporter.BuildInfo
import org.broadinstitute.transporter.error.ApiError
import org.broadinstitute.transporter.info.{InfoController, ManagerStatus, ManagerVersion}
import org.broadinstitute.transporter.transfer.api._
import org.broadinstitute.transporter.transfer.TransferController
import org.broadinstitute.transporter.web.config.OAuthConfig
import org.http4s.circe.CirceEntityEncoder
import org.http4s.dsl.io._
import org.http4s.headers.Location
import org.http4s.{HttpApp, HttpRoutes, Response, Status, Uri}
import org.http4s.server.middleware.Logger
import org.http4s.server.staticcontent.WebjarService
import org.http4s.server.staticcontent.WebjarService.Config
import tapir.{MediaType => _, _}
import tapir.json.circe._
import tapir.model.StatusCodes
import tapir.docs.openapi._
import tapir.openapi.{OAuthFlow, OAuthFlows, SecurityScheme}
import tapir.server.ServerEndpoint
import tapir.server.http4s._
import tapir.openapi.circe.yaml._

import scala.collection.immutable.ListMap
import scala.concurrent.ExecutionContext
import scala.language.higherKinds

/**
  * HTTP front-end for Transporter components exposed over a REST API.
  *
  * @param infoController controller handling unauthed info requests
  * @param transferController controller handling potentially-authed transfer requests
  * @param googleAuthConfig optional configuration for OAuth. If set, will
  *                         be used to protect transfer-level APIs
  * @param blockingEc thread pool to block on when reading static resources
  *                   from the classpath
  */
class WebApi(
  infoController: InfoController,
  transferController: TransferController,
  googleAuthConfig: Option[OAuthConfig],
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {
  import WebApi._

  private val log = Slf4jLogger.getLogger[IO]

  /**
    * Transform an effectful action to extract error information
    * into our API model, so it can be rendered as JSON.
    */
  private def buildResponse[Out](
    action: IO[Out],
    failureMessage: String
  ): IO[Either[ApiError, Out]] =
    action.attempt.flatMap[Either[ApiError, Out]](
      _.fold(
        {
          case api: ApiError => IO.pure(Left(api))
          case unhandled =>
            log
              .error(unhandled)(failureMessage)
              .as(Left(ApiError.UnhandledError(unhandled.getMessage)))
        },
        out => IO.pure(Right(out))
      )
    )

  private val statusRoute: Route[Unit, ManagerStatus, ManagerStatus] =
    endpoint
      .in("status")
      .out(jsonBody[ManagerStatus])
      .errorOut(jsonBody[ManagerStatus])
      .description("Query operational status of the system")
      .tag("Info")
      .serverLogic { _ =>
        infoController.status.map { status =>
          Either.cond(status.ok, status, status)
        }
      }

  private val versionRoute: Route[Unit, Unit, ManagerVersion] =
    endpoint
      .in("version")
      .out(jsonBody[ManagerVersion])
      .description("Query version of the system")
      .tag("Info")
      .serverLogic { _ =>
        IO.pure(Either.right[Unit, ManagerVersion](infoController.version))
      }

  private val baseRoute: Endpoint[Unit, Unit, Unit, Nothing] =
    endpoint
      .in("api" / "transporter" / "v1")
      .tag("Transfers")

  private val requestsBase = baseRoute.in("transfers")

  private val lookupRequestsRoute: Route[
    (Long, Long, SortOrder),
    ApiError,
    Page[RequestSummary]
  ] = requestsBase
    .in(query[Long]("offset"))
    .in(query[Long]("limit"))
    .in(query[SortOrder]("sort"))
    .out(jsonBody[Page[RequestSummary]])
    .errorOut(
      oneOf[ApiError](
        statusMapping(
          StatusCodes.InternalServerError,
          jsonBody[ApiError.UnhandledError]
        )
      )
    )
    .description("List all transfer batches known to Transporter")
    .serverLogic {
      case (offset, limit, sort) =>
        val getPage = transferController.listRequests(
          offset,
          limit,
          newestFirst = sort == SortOrder.Desc
        )
        val getTotal = transferController.countRequests

        buildResponse(
          (getPage, getTotal).parMapN { case (items, total) => Page(items, total) },
          "Failed to list transfer batches"
        )
    }

  private val singleRequestBase = requestsBase
    .in(path[UUID]("request-id"))
    .errorOut(
      oneOf(
        statusMapping(StatusCodes.NotFound, jsonBody[ApiError.NotFound]),
        statusMapping(
          StatusCodes.InternalServerError,
          jsonBody[ApiError.UnhandledError]
        )
      )
    )

  private val batchSubmitRoute: Route[BulkRequest, ApiError, RequestAck] =
    requestsBase.post
      .in(jsonBody[BulkRequest])
      .out(jsonBody[RequestAck])
      .errorOut(
        oneOf(
          statusMapping(StatusCodes.BadRequest, jsonBody[ApiError.InvalidRequest]),
          statusMapping(
            StatusCodes.InternalServerError,
            jsonBody[ApiError.UnhandledError]
          )
        )
      )
      .description("Submit a new batch of transfer requests")
      .serverLogic { request =>
        buildResponse(
          transferController.recordRequest(request),
          "Failed to submit request"
        )
      }

  private val requestStatusRoute: Route[UUID, ApiError, RequestSummary] =
    singleRequestBase
      .in("status")
      .out(jsonBody[RequestSummary])
      .description("Get the current summary status of a transfer request")
      .serverLogic { requestId =>
        buildResponse(
          transferController.lookupRequestStatus(requestId),
          s"Failed to look up request status for $requestId"
        )
      }

  private val requestOutputsRoute: Route[UUID, ApiError, RequestInfo] =
    singleRequestBase
      .in("outputs")
      .out(jsonBody[RequestInfo])
      .description("Get the outputs of successful transfers from a request")
      .serverLogic { requestId =>
        buildResponse(
          transferController.lookupRequestOutputs(requestId),
          s"Failed to look up outputs for $requestId"
        )
      }

  private val requestFailuresRoute: Route[UUID, ApiError, RequestInfo] =
    singleRequestBase
      .in("failures")
      .out(jsonBody[RequestInfo])
      .description("Get the outputs of failed transfers from a request")
      .serverLogic { requestId =>
        buildResponse(
          transferController.lookupRequestFailures(requestId),
          s"Failed to look up failures for $requestId"
        )
      }

  private val lookupTransfersRoute: Route[
    (UUID, Long, Long, SortOrder),
    ApiError,
    Page[TransferDetails]
  ] = singleRequestBase
    .in("list-transfers")
    .in(query[Long]("offset"))
    .in(query[Long]("limit"))
    .in(query[SortOrder]("sort"))
    .out(jsonBody[Page[TransferDetails]])
    .description(
      "Get transfer details for a given request"
    )
    .serverLogic {
      case (requestId, offset, limit, sort) =>
        val getPage = transferController.listTransfers(
          requestId,
          offset,
          limit,
          sortDesc = sort == SortOrder.Desc
        )
        val getTotal = transferController.CountTransfers(requestId)
        buildResponse(
          (getPage, getTotal).parMapN { case (items, total) => Page(items, total) },
          s"Failed to list transfers for request ID $requestId"
        )
    }

  private val reconsiderRoute: Route[UUID, ApiError, RequestAck] =
    singleRequestBase
      .in("reconsider")
      .put
      .out(jsonBody[RequestAck])
      .description("Reset the state of all failed transfers in a request to 'pending'")
      .serverLogic { requestId =>
        buildResponse(
          transferController.reconsiderRequest(requestId),
          s"Failed to reconsider transfers for $requestId"
        )
      }

  private val reconsiderSingleTransferRoute: Route[(UUID, UUID), ApiError, RequestAck] =
    singleRequestBase
      .in("detail" / path[UUID]("transfer-id") / "reconsider")
      .put
      .out(jsonBody[RequestAck])
      .description(
        "Reset the state of a single failed transfer in a request to 'pending'"
      )
      .serverLogic {
        case (requestId, transferId) =>
          buildResponse(
            transferController.reconsiderSingleTransfer(requestId, transferId),
            s"Failed to reconsider transfer for $transferId in request $requestId"
          )
      }

  private val detailsRoute: Route[(UUID, UUID), ApiError, TransferDetails] =
    singleRequestBase
      .in("detail" / path[UUID]("transfer-id"))
      .out(jsonBody[TransferDetails])
      .description("Get detailed info about a single transfer from a request")
      .serverLogic {
        case (requestId, transferId) =>
          buildResponse(
            transferController.lookupTransferDetails(requestId, transferId),
            s"Failed to look up details for $transferId"
          )
      }

  /** All tapir routes which should be included in generated documentation. */
  private val documentedRoutes = List(
    statusRoute,
    versionRoute,
    lookupRequestsRoute,
    batchSubmitRoute,
    requestStatusRoute,
    requestOutputsRoute,
    requestFailuresRoute,
    lookupTransfersRoute,
    reconsiderRoute,
    reconsiderSingleTransferRoute,
    detailsRoute
  )

  private val openapi = {
    val base = documentedRoutes
      .toOpenAPI("Transporter Manager", infoController.version.version)

    googleAuthConfig.fold(base) { _ =>
      val implicitFlow = OAuthFlow(
        authorizationUrl = "https://accounts.google.com/o/oauth2/auth",
        // Yuck, but it's typed as `String` even though it isn't required so not much we can do.
        tokenUrl = "",
        refreshUrl = None,
        scopes = OAuthConfig.AuthScopes.foldLeft(ListMap.empty[String, String]) {
          (acc, scope) =>
            acc + (scope -> s"$scope authorization")
        }
      )

      val scheme = SecurityScheme(
        `type` = "oauth2",
        description = None,
        name = Some(OAuthConfig.AuthName),
        in = None,
        scheme = None,
        bearerFormat = None,
        flows = Some(OAuthFlows(`implicit` = Some(implicitFlow))),
        openIdConnectUrl = None
      )
      val labeledScheme = ListMap(OAuthConfig.AuthName -> Right(scheme))

      base.copy(components = base.components.map { components =>
        components.copy(securitySchemes = labeledScheme)
      })
    }
  }

  /** Convenience routes that don't need to be included in generated documentation. */
  private val shimRoutes = HttpRoutes.of[IO] {

    // Redirect from "stable" routes to version-specific routes for static pieces of Swagger.
    case GET -> Root =>
      TemporaryRedirect(Location(Uri.unsafeFromString(s"/$StableUiPath")))
    case GET -> Root / segment if segment == StableUiPath =>
      TemporaryRedirect(Location(Uri.unsafeFromString(ApiUiPath)))
    case GET -> Root / segment if segment == StableOAuthRedirectPath =>
      TemporaryRedirect(Location(Uri.unsafeFromString(OauthRedirectPath)))

    // Expose generated docs in YAML format.
    case GET -> Root / segment if segment == ApiDocsPath =>
      Ok(openapi.toYaml)
  }

  /**
    * Static resource routes matching anything within the resources of the the Swagger webjar
    * _except for_ the index page, which we have to modify in-flight to inject app-specific
    * info.
    */
  private val swaggerUiAssetRoutes = WebjarService[IO](
    config = Config(
      blockingEc,
      asset =>
        asset.library == BuildInfo.swaggerLibrary &&
          asset.version == BuildInfo.swaggerVersion &&
          asset.asset != "index.html"
    )
  )

  /**
    * Static resource route matching only the Swagger index page.
    *
    * We inject two types of information by rewriting the HTML on its way to the client:
    *   1. We rewrite URLs to point at Transporter routes
    *   2. If OAuth config is given, we add a call to the 'initOAuth' method
    *
    * This seems to be standard practice for apps that serve Swagger from resources,
    * at least in DSP. For example, see CromIAM at:
    *
    * https://github.com/broadinstitute/cromwell/blob/master/CromIAM/src/main/scala/cromiam/webservice/SwaggerUiHttpService.scala
    */
  private val swaggerUiIndexRoutes = WebjarService[IO](
    config = Config(
      blockingEc,
      asset =>
        asset.library == BuildInfo.swaggerLibrary &&
          asset.version == BuildInfo.swaggerVersion &&
          asset.asset == "index.html"
    )
  ).map { response =>
    if (response.status.isSuccess) {
      val newBody = fs2.text.lines(response.bodyAsText).map { line =>
        val withUrls = line.replace(
          /*
           * Replace the dummy "petstore" URL with the route to this app's API docs.
           * Also replace the OAuth redirect URL here since it goes in the same method
           * call on the front-end, even if OAuth isn't going to be enabled.
           *
           * NOTE: We redirect to a stable alias instead of the versioned HTML page in the
           * resources jar because Google requires that allowable redirect URLs be white-listed
           * in the cloud console, and this is easier to maintain than it would be to add a new
           * white-listed URL every time we bump the Swagger UI's version.
           */
          """url: "https://petstore.swagger.io/v2/swagger.json",""",
          s"""url: "/$ApiDocsPath",
             |validatorUrl: null,
             |oauth2RedirectUrl: window.location.origin + "/$StableOAuthRedirectPath",""".stripMargin
        )

        /*
         * If auth config is given, inject a call to initialize the OAuth dialog on
         * the front-end. The OAuth client ID will be visible in the page's source code,
         * but apparently that's Just How Swagger Works.
         */
        googleAuthConfig.fold(withUrls) { config =>
          withUrls.replace(
            "window.ui = ui",
            s"""ui.initOAuth({
               |  appName: "Swagger Auth",
               |  clientId: "${config.clientId}",
               |  scopeSeparator: " "
               |})
               |window.ui = ui""".stripMargin
          )
        }
      }

      response
        .withEntity(newBody.intersperse("\n"))
        .withContentTypeOption(response.contentType)
    } else {
      response
    }
  }

  /**
    * HTTP "application" which can convert requests into responses
    * via the manager's controllers.
    *
    * Adds audit logging to all requests / responses.
    */
  def app: HttpApp[IO] = {
    implicit val serverOptions: Http4sServerOptions[IO] = Http4sServerOptions
      .default[IO]
      .copy(blockingExecutionContext = blockingEc)

    val definedRoutes = documentedRoutes.toRoutes
      .combineK(swaggerUiIndexRoutes)
      .combineK(swaggerUiAssetRoutes)
      .combineK(shimRoutes)

    val sealedRoutes: HttpApp[IO] = Kleisli { request =>
      definedRoutes.run(request).getOrElse {
        import CirceEntityEncoder._
        Response[IO](status = Status.NotFound)
          .withEntity(ApiError.UnhandledError("Not Found"))
      }
    }

    Logger.httpApp(logHeaders = true, logBody = true)(sealedRoutes)
  }
}

object WebApi {

  type Route[I, E, O] = ServerEndpoint[I, E, O, Nothing, IO]

  /*
   * Tapir can auto-derive schemas for most types, but it needs
   * some help for classes that we'd rather have map to standard
   * JSON types instead of their internal Scala representation.
   */

  implicit def nonEmptyListSchema[A](
    implicit s: SchemaFor[A]
  ): SchemaFor[NonEmptyList[A]] =
    SchemaFor(Schema.SArray(s.schema))

  implicit def enumSchema[E <: EnumEntry: Enum]: SchemaFor[E] =
    SchemaFor(Schema.SString)

  implicit def enumCodec[E <: EnumEntry](implicit E: Enum[E]): Codec.PlainCodec[E] =
    Codec.stringPlainCodecUtf8.mapDecode { s =>
      E.namesToValuesMap.get(s) match {
        case Some(e) => DecodeResult.Value(e)
        case None =>
          DecodeResult.Mismatch(s"One of: ${E.values.map(_.entryName).mkString(",")}", s)
      }
    }(_.entryName).schema(enumSchema[E].schema)

  implicit def enumMapSchema[E <: EnumEntry, V](
    implicit e: Enum[E],
    s: SchemaFor[V]
  ): SchemaFor[Map[E, V]] =
    SchemaFor(
      Schema.SProduct(
        Schema.SObjectInfo(s"${e.toString}Map"),
        e.values.map(_.entryName -> s.schema),
        Nil
      )
    )

  /** Top-level route which serves generated API docs. */
  val ApiDocsPath = "api-docs.yaml"

  /**
    * Top-level route which serves the Swagger UI.
    *
    * Our swagger routes change every time we bump the UI's version, so we
    * define stable aliases to expose in external systems / documentation.
    */
  val StableUiPath = "swagger-ui"

  /**
    * Callback page for Google to redirect to on successful OAuth logins.
    *
    * Our swagger routes change every time we bump the UI's version, so we
    * define stable aliases to expose in external systems / documentation.
    */
  val StableOAuthRedirectPath = "oauth2-redirect"

  /** Route which serves the Swagger UI. */
  val ApiUiPath =
    /*
     * Usually hard-coding a reference to `BuildInfo` makes things
     * more difficult to test, but in this case there's only ever
     * one safe choice for the swagger parameters so there's no point
     * in taking them as arguments.
     */
    s"/${BuildInfo.swaggerLibrary}/${BuildInfo.swaggerVersion}/index.html"

  /** Route which serves the OAuth redirect logic for the Swagger UI. */
  val OauthRedirectPath =
    s"/${BuildInfo.swaggerLibrary}/${BuildInfo.swaggerVersion}/oauth2-redirect.html"
}
