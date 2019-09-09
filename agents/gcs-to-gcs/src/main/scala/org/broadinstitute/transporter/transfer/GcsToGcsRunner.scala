package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import org.broadinstitute.monster.storage.gcs.GcsApi
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.kafka.{Done, Progress, TransferStep}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}

import scala.concurrent.ExecutionContext

/**
  * Transfer runner which can copy files from one location in GCS to another
  * using GCS's efficient rewrite API.
  *
  * @param api client which actually knows how to communicate with GCS over HTTP
  */
class GcsToGcsRunner private[transfer] (api: GcsApi)
    extends TransferRunner[GcsToGcsRequest, GcsToGcsProgress, GcsToGcsOutput] {

  /*
   * NOTE: There's a lot more we could do here.
   *
   * For example, the S3->GCS agent supports:
   *   1. Fail-fast checks on expected size
   *   2. Early-exit checks on content md5
   *   3. Overwrite-prevention, with a "force" flag as an escape hatch
   *
   * We could also enable the agent to receive directories/prefixes, and
   * recursively unroll the paths.
   *
   * Choosing to punt on all of these ideas for now because:
   *   1. We don't actually need this agent for our own ingest projects,
   *      so we can add features as Ops asks for them
   *   2. The best way to model file-vs-directory transfers is to make
   *      the request model a sealed trait. circe-derivation 0.12.x has
   *      some awesome new functionality to make handling trait hierarchies
   *      seamless, but we aren't quite ready to upgrade yet.
   */

  override def initialize(
    request: GcsToGcsRequest
  ): TransferStep[GcsToGcsRequest, GcsToGcsProgress, GcsToGcsOutput] =
    api
      .initializeCopy(
        request.sourceBucket,
        request.sourcePath,
        request.targetBucket,
        request.targetPath
      )
      .unsafeRunSync()
      .fold(
        id =>
          Progress(
            GcsToGcsProgress(
              request.sourceBucket,
              request.sourcePath,
              request.targetBucket,
              request.targetPath,
              id
            )
          ),
        _ => Done(GcsToGcsOutput(request.targetBucket, request.targetPath))
      )

  override def step(
    progress: GcsToGcsProgress
  ): Either[GcsToGcsProgress, GcsToGcsOutput] =
    api
      .incrementCopy(
        progress.sourceBucket,
        progress.sourcePath,
        progress.targetBucket,
        progress.targetPath,
        progress.uploadId
      )
      .unsafeRunSync()
      .bimap(
        GcsToGcsProgress(
          progress.sourceBucket,
          progress.sourcePath,
          progress.targetBucket,
          progress.targetPath,
          _
        ),
        _ => GcsToGcsOutput(progress.targetBucket, progress.targetPath)
      )
}

object GcsToGcsRunner {

  /** Build a new runner, wrapped in setup / teardown logic. */
  def resource(config: RunnerConfig, ec: ExecutionContext)(
    implicit cs: ContextShift[IO],
    t: Timer[IO]
  ): Resource[IO, GcsToGcsRunner] =
    BlazeClientBuilder[IO](ec)
      .withResponseHeaderTimeout(config.timeouts.responseHeaderTimeout)
      .withRequestTimeout(config.timeouts.requestTimeout)
      .resource
      .evalMap { httpClient =>
        val retryPolicy = RetryPolicy[IO](
          RetryPolicy
            .exponentialBackoff(config.retries.maxDelay, config.retries.maxRetries)
        )
        val retryingClient = Retry(retryPolicy)(httpClient)

        GcsApi.build(
          // Log bodies so we can see byte counts in responses.
          Logger(logHeaders = true, logBody = true)(retryingClient),
          config.serviceAccountJson
        )
      }
      .map(new GcsToGcsRunner(_))
}
