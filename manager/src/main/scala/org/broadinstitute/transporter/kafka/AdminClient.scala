package org.broadinstitute.transporter.kafka

import java.time.Duration
import java.util.concurrent.{CancellationException, CompletionException}

import cats.effect._
import cats.implicits._
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.{NewTopic, AdminClient => JAdminClient}
import org.apache.kafka.common.KafkaFuture
import org.broadinstitute.transporter.kafka.config.KafkaConfig

import scala.concurrent.ExecutionContext
import scala.collection.JavaConverters._

/**
  * Client responsible for managing "admin" operations in Transporter's Kafka cluster.
  *
  * Admin ops include querying cluster-level state, creating/modifying/deleting topics,
  * and settings ACLs (though we don't handle that yet).
  */
trait AdminClient {

  /** Check if the client can interact with the backing cluster. */
  def checkReady: IO[Boolean]

  /**
    * Create new Kafka topics with the given name, using global config
    * for partition counts and replication factors.
    *
    * Kafka supports requesting multiple new topics within a single request,
    * but the result isn't transactional. In the case that some topics get
    * created but others fail, we delete the successful topics before returning
    * the error.
    */
  def createTopics(topicNames: String*): IO[Unit]

  /** Check that the given topics exist in Kafka. */
  def topicsExist(topicNames: String*): IO[Boolean]
}

object AdminClient {

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
      * Build an IO that will cancel the wrapped Future.
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
    * Summary of the unsuccessful results of a topic-creation request.
    *
    * @param failures failures encountered during the request, keyed by
    *                 the corresponding topic name
    */
  case class TopicCreationFailed(failures: List[(String, Throwable)])
      extends RuntimeException(
        s"Failed to create topics: ${failures.map(_._1).mkString(", ")}"
      )

  /**
    * Construct a Kafka client, wrapped in logic which will:
    *   1. Automatically set up underlying Kafka client instances
    *      in separate thread pools on startup, and
    *   2. Automatically clean up the underlying clients and their
    *      pools on shutdown
    *
    * @param config settings for the underlying Kafka client
    * @param blockingEc execution context which should run the blocking I/O
    *                   required to set up / tear down the client
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  def resource(config: KafkaConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, AdminClient] =
    adminResource(config, blockingEc).map(new Impl(_, config.replicationFactor))

  /** Construct a Kafka admin client, wrapped in setup and teardown logic. */
  private[kafka] def adminResource(config: KafkaConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, JAdminClient] = {
    val settings = config.adminSettings
    val initClient = cs.evalOn(blockingEc) {
      settings.adminClientFactory.create[IO](settings)
    }
    val closeClient = (client: JAdminClient) =>
      cs.evalOn(blockingEc)(IO.delay {
        client.close(Duration.ofMillis(settings.closeTimeout.toMillis))
      })

    Resource.make(initClient)(closeClient)
  }

  /**
    * Concrete implementation of our admin client used by mainline code.
    *
    * Partly a copy-paste of fs2-kafka's AdminClient, which doesn't support all
    * the functionality we need.
    *
    * @param adminClient client which can execute "raw" Kafka requests
    * @param newTopicReplicationFactor replication factor to use for all topics
    *                                  created by the client
    * @param cs proof of the ability to shift IO-wrapped computations
    *           onto other threads
    */
  private[kafka] class Impl(
    adminClient: JAdminClient,
    newTopicReplicationFactor: Short
  )(implicit cs: ContextShift[IO])
      extends AdminClient {

    private val logger = Slf4jLogger.getLogger[IO]

    override def checkReady: IO[Boolean] = {
      val okCheck = for {
        _ <- logger.info("Running status check against Kafka cluster...")
        id <- IO.suspend(adminClient.describeCluster().clusterId().cancelable)
        _ <- logger.debug(s"Got cluster ID $id")
      } yield {
        true
      }

      okCheck.handleErrorWith { err =>
        logger.error(err)("Kafka status check hit error").as(false)
      }
    }

    override def createTopics(topicNames: String*): IO[Unit] = {
      val topics = topicNames.mkString(", ")
      for {
        _ <- logger.info(s"Creating Kafka topics: $topics")
        _ <- attemptCreateTopics(topicNames.toList).guaranteeCase {
          case ExitCase.Completed => logger.debug(s"Successfully created topics: $topics")
          case ExitCase.Canceled  => logger.warn(s"Topic creation canceled: $topics")
          case ExitCase.Error(TopicCreationFailed(failures)) =>
            reportAndRollback(topicNames.toList, failures)
          case ExitCase.Error(err) =>
            logger.error(err)(s"Failed to create topics: $topics")
        }
      } yield ()
    }

    override def topicsExist(topicNames: String*): IO[Boolean] =
      for {
        _ <- logger.info(s"Checking for existence of topics: ${topicNames.mkString(",")}")
        existingTopics <- listTopics
      } yield {
        topicNames.toSet.subsetOf(existingTopics)
      }

    /**
      * Get the list of all non-internal topics in Transporter's backing Kafka cluster.
      *
      * Exposed for testing.
      */
    private[kafka] def listTopics: IO[scala.collection.mutable.Set[String]] =
      IO.suspend(adminClient.listTopics().names().cancelable).map(_.asScala)

    /**
      * Attempt to create new topics with the given names in Kafka, and summarize the results.
      *
      * If some topics are created but others fail, the failed [[IO]] will contain
      * a [[TopicCreationFailed]] exception summarizing the state for reporting / rollback.
      */
    private def attemptCreateTopics(topicNames: List[String]): IO[Unit] = {
      val newTopics = topicNames.map { name =>
        new NewTopic(name, KafkaConfig.TopicPartitions, newTopicReplicationFactor)
      }

      val attempts = IO.suspend {
        adminClient
          .createTopics(newTopics.asJava)
          .values
          .asScala
          .toList
          .parTraverse {
            case (topic, res) => res.cancelable.attempt.map(topic -> _.void)
          }
      }

      attempts.flatMap { results =>
        val failures = results.foldMap {
          case (topic, res) => res.fold(err => List(topic -> err), _ => Nil)
        }

        if (failures.isEmpty) {
          IO.unit
        } else {
          IO.raiseError(TopicCreationFailed(failures))
        }
      }
    }

    /**
      * Log topics that failed to be created within a request, and roll back
      * creation of those that were successfully created.
      */
    private def reportAndRollback(
      attempted: List[String],
      failures: List[(String, Throwable)]
    ): IO[Unit] = {
      val successes = attempted.toSet.diff(failures.map(_._1).toSet)

      for {
        _ <- logger.error(s"Errors hit while creating topics:")
        _ <- failures.traverse_ {
          case (topic, err) => logger.error(err)(s"Failed to create topic $topic")
        }
        _ <- logger.warn(
          s"Rolling back creation of topics: ${successes.mkString(", ")}"
        )
        _ <- IO.suspend(adminClient.deleteTopics(successes.asJava).all.cancelable)
      } yield ()
    }
  }
}
