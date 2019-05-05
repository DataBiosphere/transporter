package org.broadinstitute.transporter.error

/** Exception used to mark when a user attempts to create a queue that already exists. */
case class QueueAlreadyExists(name: String) extends IllegalArgumentException
