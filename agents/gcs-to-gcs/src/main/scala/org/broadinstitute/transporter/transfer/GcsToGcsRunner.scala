package org.broadinstitute.transporter.transfer

import cats.effect.{ContextShift, IO, Resource, Timer}
import cats.implicits._
import org.broadinstitute.monster.storage.gcs.GcsApi
import org.broadinstitute.transporter.config.RunnerConfig
import org.broadinstitute.transporter.kafka.{Done, Progress, TransferStep}
import org.http4s.client.blaze.BlazeClientBuilder
import org.http4s.client.middleware.{Logger, Retry, RetryPolicy}

import scala.concurrent.ExecutionContext

class GcsToGcsRunner private[transfer] (api: GcsApi)
    extends TransferRunner[GcsToGcsRequest, GcsToGcsProgress, GcsToGcsOutput] {

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
