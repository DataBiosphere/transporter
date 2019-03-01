package org.broadinstitute.transporter.kafka

import java.util.concurrent.{CancellationException, CompletionException}

import cats.effect._
import cats.implicits._
import fs2.kafka.AdminClientSettings
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.common.KafkaFuture

import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

/**
  * Client responsible for sending requests to / parsing responses from
  * Transporter's backing Kafka cluster.
  *
  * @param adminClient Kafka client which can query and modify
  *                    cluster-level information (i.e. existing topics)
  * @param blockingEc execution context which should run all blocking I/O
  *                   required by the underlying Kafka clients
  * @param cs proof of the ability to shift IO-wrapped computations
  *           onto other threads
  */
class KafkaClient private[kafka] (
  adminClient: AdminClient,
  topicConfig: TopicConfig,
  blockingEc: ExecutionContext
)(implicit cs: ContextShift[IO]) {
  import KafkaClient._

  private val logger = Slf4jLogger.getLogger[IO]

  /** Check if the client can interact with the backing cluster. */
  def checkReady: IO[Boolean] = {

    val okCheck = for {
      _ <- logger.info("Running status check against Kafka cluster...")
      id <- cs.evalOn(blockingEc) {
        IO.suspend(adminClient.describeCluster().clusterId().cancelable)
      }
      _ <- logger.debug(s"Got cluster ID $id")
    } yield {
      true
    }

    okCheck.handleErrorWith { err =>
      logger.error(err)("Kafka status check hit error").as(false)
    }
  }

  def createTopics(topicNames: String*): IO[Unit] = {
    val newTopics = topicNames.map { name =>
      new NewTopic(name, topicConfig.partitions, topicConfig.replicationFactor)
    }

    val tryCreateAndSummarize = {
      val attempts = IO.suspend {
        adminClient
          .createTopics(newTopics.asJava)
          .values()
          .asScala
          .toList
          .parTraverse {
            case (topic, res) => res.cancelable.attempt.map(topic -> _.void)
          }
      }

      cs.evalOn(blockingEc)(attempts).flatMap { results =>
        val (successes, failures) =
          results.foldLeft((Set.empty[String], Map.empty[String, Throwable])) {
            case ((createdTopics, errs), (topic, Left(err))) =>
              (createdTopics, errs + (topic -> err))
            case ((createdTopics, errs), (topic, _)) =>
              (createdTopics + topic, errs)
          }

        if (failures.isEmpty) {
          IO.pure(successes)
        } else {
          IO.raiseError(TopicCreationFailed(failures, successes))
        }
      }
    }

    val topics = topicNames.mkString(", ")
    for {
      _ <- logger.info(s"Creating Kafka topics: $topics")
      _ <- tryCreateAndSummarize.guaranteeCase {
        case ExitCase.Completed =>
          logger.debug(s"Successfully created topics: $topics")
        case ExitCase.Canceled =>
          logger.warn(s"Topic creation canceled: $topics")
        case ExitCase.Error(TopicCreationFailed(failures, successes)) =>
          val logFailures = failures.toList.traverse_ {
            case (topic, err) => logger.error(err)(s"Failed to create topic $topic")
          }
          for {
            _ <- logger.error(s"Errors hit while creating topics:")
            _ <- logFailures
            _ <- logger.warn(
              s"Rolling back creation of topics: ${successes.mkString(", ")}"
            )
            _ <- cs.evalOn(blockingEc) {
              IO.suspend(adminClient.deleteTopics(successes.asJava).all().cancelable)
            }
          } yield ()
        case ExitCase.Error(err) =>
          logger.error(err)(s"Unrecognized error hit while creating topics: $topics")
      }
    } yield ()
  }
}

object KafkaClient {

  /**
    * Extension methods for Kafka's bespoke Future implementation.
    *
    * Copied from https://github.com/ovotech/fs2-kafka until they
    * add functionality we need to their admin wrapper.
    */
  implicit final class KafkaFutureSyntax[A](
    private val future: KafkaFuture[A]
  ) extends AnyVal {

    /**
      * Cancel the Future and return an IO-wrapped token.
      *
      * Can't be named 'cancel' because there's already a method
      * on the Future type with that name.
      */
    def cancelToken: CancelToken[IO] =
      IO.delay { future.cancel(true); () }

    /** Convert the wrapped Future into a cancellable IO. */
    def cancelable: IO[A] =
      IO.cancelable { cb =>
        future.whenComplete { (a: A, t: Throwable) =>
          t match {
            case null                                         => cb(Right(a))
            case _: CancellationException                     => ()
            case e: CompletionException if e.getCause != null => cb(Left(e.getCause))
            case e                                            => cb(Left(e))
          }
        }.cancelToken
      }
  }

  case class TopicCreationFailed(failures: Map[String, Throwable], successes: Set[String])
      extends RuntimeException(
        s"Failed to create topics: ${failures.keys.mkString(", ")}"
      )

  /**
    * Construct a Kafka client, wrapped in logic which will:
    *   1. Automatically set up underlying Kafka client instances
    *      in separate thread pools on startup, and
    *   2. Automatically clean up the underlying clients and their
    *      pools on shutdown
    *
    * @param config settings for the underlying Kafka clients powering
    *               our client
    * @param blockingEc execution context which should run all blocking I/O
    *                   required by the underlying Kafka clients
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  def resource(config: KafkaConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaClient] =
    clientResource(config, blockingEc)
      .map(new KafkaClient(_, config.topicDefaults, blockingEc))

  private[kafka] def clientResource(
    config: KafkaConfig,
    blockingEc: ExecutionContext
  )(implicit cs: ContextShift[IO]): Resource[IO, AdminClient] = {
    val settings = AdminClientSettings.Default
      .withBootstrapServers(config.bootstrapServers.mkString(","))
      .withClientId(config.clientId)
      .withRequestTimeout(config.timeouts.requestTimeout)
      .withCloseTimeout(config.timeouts.closeTimeout)

    val initClient = cs.evalOn(blockingEc) {
      settings.adminClientFactory.create[IO](settings)
    }

    val closeClient = (client: AdminClient) =>
      cs.evalOn(blockingEc)(IO.delay {
        client.close(settings.closeTimeout.length, settings.closeTimeout.unit)
      })

    Resource.make(initClient)(closeClient)
  }
}
