# Transporter Agents
Specialized services for executing file transfers.

## Responsibilities
Agents are the workhorses within [Transporter](../README.md). A single agent connects to a
queue resource initialized by the [Manager](../manager/README.md), and:
* Consumes messages from the queue's "request" topic, using the data to:
   1. Perform storage-specific validation of the request
   2. Initialize the upload in the target storage system
   3. Push an initial progress marker onto the queue's "progress" topic
* Consumes messages from the queue's "progress" topic, using the data to:
   1. Transfer a chunk of data from source to target
   2. Determine if the pushed chunk was the last required chunk
   3. Push either a new progress marker onto the queue's "progress" topic,
      or a success message onto the queue's "result" topic, depending on
      the result of transferring the chunk
* Converts errors raised during the initialization and step streams into
  failure messages, and pushes those messages onto the queue's "result" topic

## Implementations
Common agent code is captured in a [template](template/README.md). So far,
we've produced the following concrete implementation of the template:
* [Echo](echo/README.md) (no-op)
* [AWS->GCP](aws-to-gcp/README.md)

## Running Locally
To run an agent locally:
1. Run the [Manager](../manager/README.md#running-locally) locally as a background process.
2. Initialize a queue resource in the Manager for the agent to connect to.
   Use the JSON schema listed in the agent's documentation when sending the request.
3. Add the name of the local queue to the agent's `application.conf`. For example:
   ```bash
   $ cat <<-EOF > agents/aws-to-gcp/src/main/resources/application.conf
   org.broadinstitute.transporter.queue.queue-name: "<the-queue>"
   EOF
   ```

Once all this is done, the agent can be run through `sbt`:
```bash
$ sbt
# Launch as a background process:
sbt:transporter> transporter-aws-to-gcp-agent/reStart
... logging from app usage ...
# Later, stop the background process:
sbt:transporter> transporter-aws-to-gcp-agent/reStop
```

You can then submit transfers to the queue via the Manager's Swagger UI, and see progress
messages begin to be logged by the agent.
