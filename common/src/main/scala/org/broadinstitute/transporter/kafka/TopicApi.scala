package org.broadinstitute.transporter.kafka

import java.util.UUID

import scala.util.matching.Regex

object TopicApi {

  /**
    * Prefix used for all Kafka topics created by Transporter for sending
    * transfer requests to agents.
    */
  private val requestTopicPrefix = "transporter.requests."

  /**
    * Prefix used for all Kafka topics created by Transporter for tracking
    * incremental transfer progress.
    */
  private val progressTopicPrefix = "transporter.progress."

  /**
    * Prefix used for all Kafka topics created by Transporter for receiving
    * transfer results from agents.
    */
  private val responseTopicPrefix = "transporter.responses."

  /**
    * Compute the topic names expected to be used by the queue with the given ID.
    *
    * TODO: Use topic name to derive these instead of ID, so agents can compute
    * them without needing to HTTP GET the manager.
    */
  def queueTopics(queueId: UUID): (String, String, String) = (
    s"$requestTopicPrefix$queueId",
    s"$progressTopicPrefix$queueId",
    s"$responseTopicPrefix$queueId"
  )

  /**
    * Pattern matching all Transporter request topics, for use in consumer
    * subscriptions.
    */
  val RequestSubscriptionPattern: Regex = s"${Regex.quote(requestTopicPrefix)}.+".r

  /**
    * Pattern matching all Transporter progress topics, for use in consumer
    * subscriptions.
    */
  val ProgressSubscriptionPattern: Regex = s"${Regex.quote(progressTopicPrefix)}.+".r

  /**
    * Pattern matching all Transporter response topics, for use in consumer
    * subscriptions.
    */
  val ResponseSubscriptionPattern: Regex = s"${Regex.quote(responseTopicPrefix)}.+".r
}
