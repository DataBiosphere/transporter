package org.broadinstitute.transporter.error

/** Exception used to mark when a user attempts to overwrite a queue parameter with an invalid value. */
case class InvalidQueueParameter(name: String, message: String)
    extends IllegalArgumentException
