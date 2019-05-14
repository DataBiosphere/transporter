package org.broadinstitute.transporter.kafka

import java.time.Duration
import java.util.concurrent.{CancellationException, CompletionException}

import cats.effect.{CancelToken, ContextShift, ExitCase, IO, Resource}
import cats.implicits._
import fs2.kafka.AdminClientSettings
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.apache.kafka.clients.admin.{AdminClient, NewPartitions, NewTopic}
import org.apache.kafka.common.KafkaFuture
import org.broadinstitute.transporter.kafka.config.{AdminConfig, ConnectionConfig}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext

/**
  * Client responsible for managing "admin" operations in Transporter's Kafka cluster.
  *
  * Admin ops include querying cluster-level state, creating/modifying/deleting topics,
  * and settings ACLs (though we don't handle that yet).
  */
trait KafkaAdminClient {

  /**
    * Check if the Kafka cluster contains enough brokers to meet the replication-factor
    * requirements of new topics created by this client.
    */
  def checkEnoughBrokers: IO[Boolean]

  /**
    * Create new Kafka topics with the given name, using global config
    * for partition counts and replication factors.
    *
    * Kafka supports requesting multiple new topics within a single request,
    * but the result isn't transactional. In the case that some topics get
    * created but others fail, we delete the successful topics before returning
    * the error.
    */
  def createTopics(topicPartitions: List[(String, Int)]): IO[Unit]

  /**
    * Increase the partition counts for a set of topics to a new value.
    *
    * Kafka only supports increasing partitions, not decreasing them. This
    * method will fail if one of the given numbers is lower than the existing
    * partition count for the corresponding topic.
    */
  def increasePartitionCounts(newTopicPartitions: List[(String, Int)]): IO[Unit]
}

object KafkaAdminClient {

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

  def resource(conn: ConnectionConfig, admin: AdminConfig, blockingEc: ExecutionContext)(
    implicit cs: ContextShift[IO]
  ): Resource[IO, KafkaAdminClient] =
    adminResource(conn, blockingEc).map(
      new Impl(_, admin.replicationFactor)
    )

  /** Construct a Kafka admin client, wrapped in setup and teardown logic. */
  private[kafka] def adminResource(
    config: ConnectionConfig,
    blockingEc: ExecutionContext
  )(
    implicit cs: ContextShift[IO]
  ): Resource[IO, AdminClient] = {
    val settings = AdminClientSettings.Default
    // Required to connect to Kafka at all.
      .withBootstrapServers(config.bootstrapServers.mkString_(","))
      // For debugging on the Kafka server; adds an ID to the logs.
      .withClientId(config.clientId)
      // No "official" recommendation on these values, we can tweak as we see fit.
      .withRequestTimeout(config.requestTimeout)
      .withCloseTimeout(config.closeTimeout)

    val initClient = cs.evalOn(blockingEc) {
      settings.adminClientFactory.create[IO](settings)
    }
    val closeClient = (client: AdminClient) =>
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
    adminClient: AdminClient,
    newTopicReplicationFactor: Short
  )(implicit cs: ContextShift[IO])
      extends KafkaAdminClient {

    private val logger = Slf4jLogger.getLogger[IO]

    override def checkEnoughBrokers: IO[Boolean] =
      for {
        _ <- logger.info("Querying Kafka cluster state...")
        clusterInfo <- IO.suspend(adminClient.describeCluster().nodes().cancelable)
        _ <- logger.debug(s"Got cluster info: $clusterInfo")
        _ <- logger.info("Checking cluster state...")
      } yield {
        clusterInfo.size() >= newTopicReplicationFactor
      }

    override def createTopics(topicPartitions: List[(String, Int)]): IO[Unit] = {
      val topics = topicPartitions.mkString(", ")
      for {
        _ <- logger.info(s"Creating Kafka topics: $topics")
        _ <- attemptCreateTopics(topicPartitions).guaranteeCase {
          case ExitCase.Completed => logger.debug(s"Successfully created topics: $topics")
          case ExitCase.Canceled  => logger.warn(s"Topic creation canceled: $topics")
          case ExitCase.Error(TopicCreationFailed(failures)) =>
            reportAndRollback(topicPartitions, failures)
          case ExitCase.Error(err) =>
            logger.error(err)(s"Failed to create topics: $topics")
        }
      } yield ()
    }

    override def increasePartitionCounts(
      newTopicPartitions: List[(String, Int)]
    ): IO[Unit] = {
      val topicPartString = newTopicPartitions.map { case (t, p) => s"$t -> $p" }
        .mkString("[", ", ", "]")
      val newPartitionCounts = newTopicPartitions.map {
        case (topic, num) => topic -> NewPartitions.increaseTo(num)
      }.toMap.asJava

      for {
        _ <- logger.info(s"Increasing Kafka topic partition counts: $topicPartString")
        _ <- IO.suspend(adminClient.createPartitions(newPartitionCounts).all().cancelable)
      } yield ()
    }

    /**
      * Attempt to create new topics with the given names in Kafka, and summarize the results.
      *
      * If some topics are created but others fail, the failed [[IO]] will contain
      * a [[TopicCreationFailed]] exception summarizing the state for reporting / rollback.
      */
    private def attemptCreateTopics(topicNames: List[(String, Int)]): IO[Unit] = {
      val newTopics = topicNames.map {
        case (name, partitions) =>
          new NewTopic(name, partitions, newTopicReplicationFactor)
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
      attempted: List[(String, Int)],
      failures: List[(String, Throwable)]
    ): IO[Unit] = {
      val successes = attempted.map(_._1).toSet.diff(failures.map(_._1).toSet)

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
