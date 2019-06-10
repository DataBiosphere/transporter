# Transporter Agents - AWS->GCP
Transporter agent which copies files from AWS to GCP.

## Motivating Projects
Since S3 is so popular, this agent covers many of our ingest use-cases:
* UKBB
* ENCODE
* V2F

## Maintenance Status
This agent provides crucial functionality to the team behind Transporter.
It should be reliable (and hopefully efficient!) to use as-is. If not, please
file a bug report so we can improve transfers across the board!

## Expected schema
Use this JSON schema to configure a Manager paired with this agent:
```json
{
  "$schema": "http://json-schema.org/draft-04/schema",
  "type": "object",
  "properties": {
    "s3Bucket": { "type": "string" },
    "s3Region": { "type":  "string" },
    "s3Path": { "type": "string" },
    "gcsBucket": { "type": "string" },
    "gcsPath": { "type": "string" },
    "expectedSize": { "type": "integer" },
    "expectedMd5": { "type": "string", "pattern": "[0-9a-f]+" },
    "force": { "type":  "boolean" }
  },
  "required": ["s3Bucket", "s3Region", "s3Path", "gcsBucket", "gcsPath"],
  "additionalProperties": false
}
```

## Expected Configuration
Deployments of this agent should include an `application.conf` setting the properties:
```hocon
org.broadinstitute.transporter.runner-config {
  aws {
    # AWS credentials to use when pulling data from S3.
    access-key-id: ???
    secret-access-key: ???
  }

  gcp {
    # Path to service-account credentials which the agent should
    # report when writing data to GCS.
    service-account-json: ???
  }

  timeouts {
    # Time to wait to receive the first response bytes from AWS/GCP requests.
    response-header-timeout: ???
    # Time to wait for AWS/GCP requests to complete.
    request-timeout: ???
  }

  retries {
    # Max number of times to retry a request to AWS/GCP.
    max-retries: ???
    # Max delay between retries of a request to AWS/GCP.
    # The agent uses exponential backoff on retries.
    max-delay: ???
  }
}
```
