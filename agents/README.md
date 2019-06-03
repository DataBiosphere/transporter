# Transporter Agents
Specialized services for executing file transfers.

## Responsibilities
Agents are the workhorses within [Transporter](../README.md). A single agent connects
to a trio of Kafka topics ("request", "progress", and "result"), and:
* Consumes messages from the "request" topic, using the data to:
   1. Perform storage-specific validation of the request
   2. Initialize the upload in the target storage system
   3. Push an initial progress marker onto the "progress" topic
* Consumes messages from the "progress" topic, using the data to:
   1. Transfer a chunk of data from source to target
   2. Determine if the pushed chunk was the last required chunk
   3. Push either a new progress marker onto the "progress" topic,
      or a success message onto the "result" topic, depending on
      the result of transferring the chunk
* Converts errors raised during the initialization and step streams into
  failure messages, and pushes those messages onto the "result" topic

## Implementations
Common agent code is captured in a [template](template/README.md). So far,
we've produced the following concrete implementation of the template:
* [Echo](echo/README.md) (no-op)
* [AWS->GCP](aws-to-gcp/README.md)

## Running Locally
To run an agent locally, first set up the local environment using [this script](../setup-local-env).

The agent can then be run through `sbt`:
```bash
$ sbt
# Launch as a background process:
sbt:transporter> transporter-aws-to-gcp-agent/reStart
... logging from app usage ...
# Later, stop the background process:
sbt:transporter> transporter-aws-to-gcp-agent/reStop
```

While it's possible to run an agent on its own, the only way to submit messages to it is to
[run the Manager](../manager/README.md#running-locally). You can then submit transfers to the
agent via the Manager's Swagger UI.
