package org.broadinstitute.transporter.error

import cats.data.NonEmptyList

/** Exception used to mark when a user submits transfers that don't match a queue's expected schema. */
case class InvalidRequest(failures: NonEmptyList[Throwable])
    extends IllegalArgumentException
