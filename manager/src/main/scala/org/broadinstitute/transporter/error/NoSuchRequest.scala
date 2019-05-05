package org.broadinstitute.transporter.error

import java.util.UUID

/** Exception used to mark when a user attempts to interact with a nonexistent request. */
case class NoSuchRequest(queue: String, id: UUID) extends IllegalArgumentException
