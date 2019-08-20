# Transporter Manager
Generic service for validating, orchestrating, and reporting bulk file transfers.

## Responsibilities
The Manager is the single point of entry for transfer requests to enter [Transporter](../README.md)'s
internal messaging/tracking system. It:
* Accepts transfer submissions over a REST API
* Validates transfer submissions against a configured JSON schema
* Pushes transfer requests into Kafka based on configured parallelism settings
* Pulls and records transfer results from Kafka when reported by Transporter agents
* Exposes transfer summary information to users via a REST API

It does not:
* Create or otherwise manage topics in Kafka
* Enforce any validation on transfer requests that can't be expressed in JSON schema
* Perform any custom aggregation of transfer results
* Actually transfer files

## Dependencies
Apart from Kafka, the Manager requires connection to a PostgreSQL database. The DB is
used to track the current status of all successfully submitted transfers.

## APIs
The Manager auto-generates Swagger documentation at runtime, and navigating to its root URI
will redirect to a Swagger UI displaying the most up-to-date API documentation. APIs available
at the last time somebody remembered to update this README are:

### Informational
| Verb | Path | Description |
| ---- | ---- | ----------- |
| GET | `/status` | Check if the Manager can handle API requests. |
| GET | `/version` | Get the Manager's version (Git hash for now). |


### Request-level
Paths below prefixed with `/api/transporter/v1`.

| Verb | Path | Description |
| ---- | ---- | ----------- |
| POST | `/transfers` | Submit a new batch of transfer requests. Validates requests and records them in the DB, but _doesn't_ submit them to Kafka. |
| GET | `/transfers` | Get summaries of all batch requests stored by Transporter which fall within a specified page range. |
| GET | `/transfers/{request-id}/status` | Get summary-level status of a transfer request. |
| GET | `/transfers/{request-id}/outputs` | Get agent-reported outputs of transfers that succeeded within a request. |
| GET | `/transfers/{request-id}/failures` | Get agent-reported error messages of transfers that failed within a request. |
| PUT | `/transfers/{request-id}/reconsider` | Reset the state of all failed transfers in a request to 'Pending'. |

### Transfer-level
Paths below prefixed with `/api/transporter/v1/transfers/{request-id}`.

| Verb | Path | Description |
| ---- | ---- | ----------- |
| PUT | `/detail/{transfer-id}/reconsider` | Reset the state of a specific failed transfer in a request to 'Pending'. |
| GET | `/detail/{transfer-id}` | Get all information stored by the Manager about a specific transfer under a bulk request. |
| GET | `/list-transfers` | Get the transfer IDs for a given request ID which fall within a specified page range. |

## Running Locally
To run the Manager locally:

1. Set up the local environment using [this script](../setup-local-env).
2. Run migrations on the DB. Instructions are [here](db-migrations/README.md).

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
