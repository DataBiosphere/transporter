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
  * @param topicConfig configuration to apply to all topics created
  *                    by the client
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

  /**
    * Create new topics with the given names in Kafka, using global config
    * for partition counts and replication factors.
    *
    * Kafka supports requesting multiple new topics within a single request,
    * but the result isn't transactional. In the case that some topics get
    * created but others fail, we delete the successful topics before returning
    * the error.
    */
  def createTopics(topicNames: List[String]): IO[Unit] = {
    val newTopics = topicNames.map { name =>
      new NewTopic(name, topicConfig.partitions, topicConfig.replicationFactor)
    }

    val topics = topicNames.mkString(", ")
    for {
      _ <- logger.info(s"Creating Kafka topics: $topics")
      _ <- attemptCreateTopics(newTopics).guaranteeCase {
        case ExitCase.Completed                           => logger.debug(s"Successfully created topics: $topics")
        case ExitCase.Canceled                            => logger.warn(s"Topic creation canceled: $topics")
        case ExitCase.Error(summary: TopicCreationFailed) => reportAndRollback(summary)
        case ExitCase.Error(err)                          => logger.error(err)(s"Failed to create topics: $topics")
      }
    } yield ()
  }

  /**
    * Attempt to create the given new topics within Kafka, and summarize the results.
    *
    * If some topics are created but others fail, the failed [[IO]] will contain
    * a [[TopicCreationFailed]] exception summarizing the state for reporting / rollback.
    */
  private def attemptCreateTopics(topics: List[NewTopic]): IO[Unit] = {
    val attempts = IO.suspend {
      adminClient
        .createTopics(topics.asJava)
        .values()
        .asScala
        .toList
        .parTraverse {
          case (topic, res) => res.cancelable.attempt.map(topic -> _.void)
        }
    }

    cs.evalOn(blockingEc)(attempts).flatMap { results =>
      val (failures, successes) = results.foldMap {
        case (topic, res) =>
          res.fold(
            err => (List(topic -> err), Set.empty[String]),
            _ => (Nil, Set(topic))
          )
      }

      if (failures.isEmpty) {
        IO.unit
      } else {
        IO.raiseError(TopicCreationFailed(failures, successes))
      }
    }
  }

  /**
    * Log topics that failed to be created within a request, and roll back
    * creation of those that were successfully created.
    */
  private def reportAndRollback(summary: TopicCreationFailed): IO[Unit] =
    for {
      _ <- logger.error(s"Errors hit while creating topics:")
      _ <- summary.failures.traverse_ {
        case (topic, err) => logger.error(err)(s"Failed to create topic $topic")
      }
      _ <- logger.warn(
        s"Rolling back creation of topics: ${summary.successes.mkString(", ")}"
      )
      _ <- cs.evalOn(blockingEc) {
        IO.suspend(adminClient.deleteTopics(summary.successes.asJava).all().cancelable)
      }
    } yield ()
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

  /**
    * Summary of the results of an unsuccessful topic-creation request.
    *
    * @param failures failures encountered during the request, keyed by
    *                 the corresponding topic name
    * @param successes topics which were successfully created as part
    *                  of the request
    */
  case class TopicCreationFailed(
    failures: List[(String, Throwable)],
    successes: Set[String]
  ) extends RuntimeException(
        s"Failed to create topics: ${failures.map(_._1).mkString(", ")}"
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

  /**
    * Construct a "raw" Kafka admin client, wrapped in
    * setup / teardown logic.
    *
    * @param config settings for the admin client
    * @param blockingEc execution context which should run all blocking I/O
    *                   required by the underlying Kafka clients
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
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
