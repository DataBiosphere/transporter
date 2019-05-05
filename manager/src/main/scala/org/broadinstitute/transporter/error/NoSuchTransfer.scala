package org.broadinstitute.transporter.error

import java.util.UUID

/** Exception used to mark when a user attempt to interact with a nonexistent transfer. */
case class NoSuchTransfer(queue: String, requestId: UUID, id: UUID)
    extends IllegalArgumentException
