package org.broadinstitute.transporter.error

/** Exception used to mark when a user attempts to interact with a nonexistent queue. */
case class NoSuchQueue(name: String) extends IllegalArgumentException
