package org.broadinstitute.transporter.transfer

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import org.broadinstitute.monster.storage.common.FileAttributes
import org.broadinstitute.monster.storage.gcs.{GcsApi, JsonHttpGcsApi}
import org.broadinstitute.monster.storage.sftp.SftpApi
import org.broadinstitute.transporter.api.{
  SftpToGcsOutput,
  SftpToGcsProgress,
  SftpToGcsRequest
}
import org.broadinstitute.transporter.kafka.{Done, Progress}
import org.http4s.MediaType
import org.http4s.headers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.{EitherValues, FlatSpec, Matchers}

class SftpToGcsRunnerSpec
    extends FlatSpec
    with Matchers
    with MockFactory
    with EitherValues {

  private val sourcePath = "sftp/path/to/thing"
  private val targetBucket = "the-bucket"
  private val targetPath = "gcs/path/to/thing"
  private val token = "the-token"

  behavior of "SftpToGcsRunner"

  it should "transfer small files in one shot" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val expectedSize = JsonHttpGcsApi.DefaultWriteChunkSize / 2L
    val data = Stream.emits("test data".getBytes)

    (fakeSftp.statFile _)
      .expects(sourcePath)
      .returning(
        IO.pure(Some(FileAttributes(expectedSize, None)))
      )
    (fakeSftp.readFile _).expects(sourcePath, 0L, None).returning(data)
    (fakeGcs.createObjectOneShot _)
      .expects(
        targetBucket,
        targetPath,
        `Content-Type`(MediaType.application.`octet-stream`),
        None,
        data
      )
      .returning(IO.unit)

    val out = new SftpToGcsRunner(fakeSftp, fakeGcs)
      .initialize(SftpToGcsRequest(sourcePath, targetBucket, targetPath))

    out shouldBe Done(SftpToGcsOutput(targetBucket, targetPath))
  }

  it should "initialize resumable uploads for large files" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val expectedSize = JsonHttpGcsApi.DefaultWriteChunkSize * 2L

    (fakeSftp.statFile _)
      .expects(sourcePath)
      .returning(
        IO.pure(Some(FileAttributes(expectedSize, None)))
      )
    (fakeGcs.initResumableUpload _)
      .expects(
        targetBucket,
        targetPath,
        `Content-Type`(MediaType.application.`octet-stream`),
        expectedSize,
        None
      )
      .returning(IO.pure(token))

    val out = new SftpToGcsRunner(fakeSftp, fakeGcs)
      .initialize(SftpToGcsRequest(sourcePath, targetBucket, targetPath))

    out shouldBe Progress(
      SftpToGcsProgress(sourcePath, targetBucket, targetPath, token, 0L, expectedSize)
    )
  }

  it should "raise an error on requests that point to a nonexistent file" in {
    val fakeSftp = mock[SftpApi]

    (fakeSftp.statFile _).expects(sourcePath).returning(IO.pure(None))

    val out = Either.catchNonFatal {
      new SftpToGcsRunner(fakeSftp, mock[GcsApi])
        .initialize(SftpToGcsRequest(sourcePath, targetBucket, targetPath))
    }

    out.left.value.getMessage should include(sourcePath)
  }

  it should "continue an in-flight resumable upload" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val start = 12345L
    val nextStart = 2 * start
    val expectedData = Stream.emits("hello world".getBytes)

    (fakeSftp.readFile _)
      .expects(sourcePath, start, Some(start + JsonHttpGcsApi.DefaultWriteChunkSize))
      .returning(expectedData)
    (fakeGcs.uploadBytes _)
      .expects(targetBucket, token, start, expectedData)
      .returning(IO.pure(Left(nextStart)))

    val in = SftpToGcsProgress(
      sourcePath,
      targetBucket,
      targetPath,
      token,
      start,
      start + 2 * JsonHttpGcsApi.DefaultWriteChunkSize
    )
    val out = new SftpToGcsRunner(fakeSftp, fakeGcs).step(in)

    out shouldBe Progress(in.copy(bytesUploaded = nextStart))
  }

  it should "finish an in-flight resumable upload" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val start = 12345L
    val nextStart = 2 * start
    val expectedData = Stream.emits("hello world".getBytes)

    (fakeSftp.readFile _)
      .expects(sourcePath, start, Some(nextStart))
      .returning(expectedData)
    (fakeGcs.uploadBytes _)
      .expects(targetBucket, token, start, expectedData)
      .returning(IO.pure(Right(())))

    val out = new SftpToGcsRunner(fakeSftp, fakeGcs).step(
      SftpToGcsProgress(
        sourcePath,
        targetBucket,
        targetPath,
        token,
        start,
        nextStart
      )
    )

    out shouldBe Done(SftpToGcsOutput(targetBucket, targetPath))
  }
}
