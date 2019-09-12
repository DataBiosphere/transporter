package org.broadinstitute.transporter.transfer

import cats.effect.IO
import org.broadinstitute.monster.storage.gcs.GcsApi
import org.broadinstitute.transporter.api.{
  GcsToGcsOutput,
  GcsToGcsProgress,
  GcsToGcsRequest
}
import org.broadinstitute.transporter.kafka.{Done, Progress}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class GcsToGcsRunnerSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {

  private val sourceBucket = "bucket"
  private val sourcePath = "the-path/foo.bar"
  private val targetBucket = "bucket2"
  private val targetPath = "the-path/bar.foo"
  private val token = "the-token"
  private val token2 = "token-the"

  behavior of "GcsToGcsRunner"

  it should "initialize transfers and return immediately" in {
    val fakeApi = mock[GcsApi]
    (fakeApi.initializeCopy _)
      .expects(sourceBucket, sourcePath, targetBucket, targetPath)
      .returning(IO.pure(Right(())))

    val out = new GcsToGcsRunner(fakeApi)
      .initialize(GcsToGcsRequest(sourceBucket, sourcePath, targetBucket, targetPath))

    out shouldBe Done(GcsToGcsOutput(targetBucket, targetPath))
  }

  it should "initialize transfers and return progress" in {
    val fakeApi = mock[GcsApi]
    (fakeApi.initializeCopy _)
      .expects(sourceBucket, sourcePath, targetBucket, targetPath)
      .returning(IO.pure(Left(token)))

    val out = new GcsToGcsRunner(fakeApi)
      .initialize(GcsToGcsRequest(sourceBucket, sourcePath, targetBucket, targetPath))

    out shouldBe Progress(
      GcsToGcsProgress(sourceBucket, sourcePath, targetBucket, targetPath, token)
    )
  }

  it should "increment a transfer and finish" in {
    val fakeApi = mock[GcsApi]
    (fakeApi.incrementCopy _)
      .expects(sourceBucket, sourcePath, targetBucket, targetPath, token)
      .returning(IO.pure(Right(())))

    val out = new GcsToGcsRunner(fakeApi)
      .step(GcsToGcsProgress(sourceBucket, sourcePath, targetBucket, targetPath, token))

    out shouldBe Done(GcsToGcsOutput(targetBucket, targetPath))
  }

  it should "increment a transfer and return the new progress" in {
    val fakeApi = mock[GcsApi]
    (fakeApi.incrementCopy _)
      .expects(sourceBucket, sourcePath, targetBucket, targetPath, token)
      .returning(IO.pure(Left(token2)))

    val out = new GcsToGcsRunner(fakeApi)
      .step(GcsToGcsProgress(sourceBucket, sourcePath, targetBucket, targetPath, token))

    out shouldBe Progress(
      GcsToGcsProgress(sourceBucket, sourcePath, targetBucket, targetPath, token2)
    )
  }
}
