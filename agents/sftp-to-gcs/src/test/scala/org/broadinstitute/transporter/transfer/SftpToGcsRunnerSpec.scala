package org.broadinstitute.transporter.transfer

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import org.broadinstitute.monster.storage.common.{FileAttributes, FileType}
import org.broadinstitute.monster.storage.gcs.GcsApi
import org.broadinstitute.monster.storage.sftp.SftpApi
import org.broadinstitute.transporter.api.{
  SftpToGcsOutput,
  SftpToGcsProgress,
  SftpToGcsRequest
}
import org.broadinstitute.transporter.kafka.{Done, Expanded, Progress}
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
  private val stepSize = 32

  behavior of "SftpToGcsRunner"

  it should "transfer small files in one shot" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val expectedSize = stepSize / 2L
    val data = Stream.emits("test data".getBytes)

    (fakeSftp.statFile _)
      .expects(sourcePath)
      .returning(
        IO.pure(Some(FileAttributes(expectedSize, None)))
      )
    (fakeSftp.readFile _).expects(sourcePath, 0L, None).returning(data)
    (fakeGcs.createObject _)
      .expects(
        targetBucket,
        targetPath,
        `Content-Type`(MediaType.application.`octet-stream`),
        expectedSize,
        None,
        data
      )
      .returning(IO.unit)

    val out = new SftpToGcsRunner(fakeSftp, fakeGcs, stepSize).initialize(
      SftpToGcsRequest(sourcePath, targetBucket, targetPath, isDirectory = false)
    )

    out shouldBe Done(SftpToGcsOutput(targetBucket, targetPath))
  }

  it should "initialize resumable uploads for large files" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val expectedSize = stepSize * 2L

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

    val out = new SftpToGcsRunner(fakeSftp, fakeGcs, stepSize).initialize(
      SftpToGcsRequest(sourcePath, targetBucket, targetPath, isDirectory = false)
    )

    out shouldBe Progress(
      SftpToGcsProgress(sourcePath, targetBucket, targetPath, token, 0L, expectedSize)
    )
  }

  it should "raise an error on requests that point to a nonexistent file" in {
    val fakeSftp = mock[SftpApi]

    (fakeSftp.statFile _).expects(sourcePath).returning(IO.pure(None))

    val out = Either.catchNonFatal {
      new SftpToGcsRunner(fakeSftp, mock[GcsApi], stepSize).initialize(
        SftpToGcsRequest(sourcePath, targetBucket, targetPath, isDirectory = false)
      )
    }

    out.left.value.getMessage should include(sourcePath)
  }

  it should "expand directory requests into multiple transfers" in {
    val fakeSftp = mock[SftpApi]

    val fakeContents = List(
      "file" -> FileType.File,
      "dir1" -> FileType.Directory,
      "dir2" -> FileType.Directory,
      "fiiile" -> FileType.File
    )

    (fakeSftp.listDirectory _)
      .expects(sourcePath)
      .returning(Stream.emits(fakeContents.map {
        case (name, typ) => s"$sourcePath/$name" -> typ
      }))

    val out = new SftpToGcsRunner(fakeSftp, mock[GcsApi], stepSize).initialize(
      SftpToGcsRequest(sourcePath, targetBucket, targetPath, isDirectory = true)
    )

    out shouldBe Expanded(
      fakeContents.map {
        case (name, typ) =>
          SftpToGcsRequest(
            s"$sourcePath/$name",
            targetBucket,
            s"$targetPath/$name",
            typ == FileType.Directory
          )
      }
    )
  }

  it should "continue an in-flight resumable upload" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val start = 12345L
    val nextStart = 2 * start
    val expectedData = Stream.emits("hello world".getBytes)

    (fakeSftp.readFile _)
      .expects(sourcePath, start, Some(start + stepSize))
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
      start + 2 * stepSize
    )
    val out = new SftpToGcsRunner(fakeSftp, fakeGcs, stepSize).step(in)

    out shouldBe Progress(in.copy(bytesUploaded = nextStart))
  }

  it should "finish an in-flight resumable upload" in {
    val fakeSftp = mock[SftpApi]
    val fakeGcs = mock[GcsApi]

    val start = 12345L
    val totalSize = start + stepSize - 1
    val expectedData = Stream.emits("hello world".getBytes)

    (fakeSftp.readFile _)
      .expects(sourcePath, start, Some(totalSize))
      .returning(expectedData)
    (fakeGcs.uploadBytes _)
      .expects(targetBucket, token, start, expectedData)
      .returning(IO.pure(Right(())))

    val out = new SftpToGcsRunner(fakeSftp, fakeGcs, stepSize).step(
      SftpToGcsProgress(
        sourcePath,
        targetBucket,
        targetPath,
        token,
        start,
        totalSize
      )
    )

    out shouldBe Done(SftpToGcsOutput(targetBucket, targetPath))
  }
}
