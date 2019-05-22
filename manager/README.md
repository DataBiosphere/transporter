# Transporter Manager
Generic service for validating, orchestrating, and reporting bulk file transfers.

## Responsibilities
The Manager is the single point of entry for transfer requests to enter [Transporter](../README.md)'s
internal messaging/tracking system. It:
* Creates Kafka topics to route messages for user-requested "queue" resources
* Accepts transfer submissions to a queue over a REST API
* Validates transfer submissions against queue-specific schemas
* Pushes transfer requests into Kafka based on user-defined parallelism settings
* Pulls and records transfer results from Kafka when reported by Transporter agents
* Exposes transfer summary information to users via a REST API

It does not:
* Enforce any validation on transfer requests that can't be expressed in JSON schema
* Perform any custom aggregation of transfer results
* Actually transfer files

## Dependencies
Apart from Kafka, the Manager requires connection to a Postgresql database. The DB is
used to track the current status of all created queues / all successfully submitted transfers.

## APIs
The Manager auto-generates Swagger documentation at runtime, and navigating to its root URI
will redirect to a Swagger UI displaying the most up-to-date API documentation. APIs available
at the last time somebody remembered to update this README are:

### Informational
| Verb | Path | Description |
| ---- | ---- | ----------- |
| GET | `/status` | Check if the Manager can handle API requests. |
| GET | `/version` | Get the Manager's version (Git hash for now). |

### Queue-level
Paths below prefixed with `/api/transporter/v1`.

| Verb | Path | Description |
| ---- | ---- | ----------- |
| POST | `/queues` | Create a new queue resource. Initializes Kafka topics and DB records. |
| GET | `/queues/{name}` | Get information (topic names, schema, max parallelism) about a queue. |
| PATCH | `/queues/{name}` | Update user-defined parameters (schema, max parallelism) for a queue. |

### Request-level
Paths below prefixed with `/api/transporter/v1/queues/{name}`.

| Verb | Path | Description |
| ---- | ---- | ----------- |
| POST | `/transfers` | Submit a new batch of transfer requests to a queue. Validates requests and records them in the DB, but _doesn't_ submit them to Kafka. |
| GET | `/transfers/{request-id}/status` | Get summary-level status of a transfer request. |
| GET | `/transfers/{request-id}/outputs` | Get agent-reported outputs of transfers that succeeded within a request. |
| GET | `/transfers/{request-id}/failures` | Get agent-reported error messages of transfers that failed within a request. |
| PUT | `/transfers/{request-id}/reconsider` | Reset the state of all failed transfers in a request to 'Pending'. |

### Transfer-level
Paths below prefixed with `/api/transporter/v1/queues/{name}/transfers/{request-id}`.

| Verb | Path | Description |
| ---- | ---- | ----------- |
| GET | `/detail/{transfer-id}` | Get all information stored by the Manager about a specific transfer under a bulk request. |

## Running Locally
To run the Manager locally:

1. Install its dependencies. On OS X, they can be installed via Homebrew:
   ```bash
   # Will also install Zookeeper, if not present.
   $ brew install postgresql@9.6 kafka
   # Replace 'run' with 'start' below to make the services auto-start on login:
   $ brew services run postgresql@9.6 &&
     brew services run zookeeper &&
     brew services run kafka
   ```
2. Run migrations on the DB. Instructions are [here](db-migrations/README.md).
3. Add local DB credentials to the Manager's `application.conf`:
   ```bash
   $ cat <<-EOF > manager/src/main/resources/application.conf
   org.broadinstitute.transporter.db.username = "$(whoami)"
   org.broadinstitute.transporter.db.password = ""
   EOF
   ```

Once all this is done, you can run the Manager through `sbt`:
```bash
$ sbt
# Launch as a background process:
sbt:transporter> transporter-manager/reStart
... logging from app usage ...
# Later, stop the background process:
sbt:transporter> transporter-manager/reStop
```

You can then view the Swagger UI [on port 8080](http://localhost:8080).
