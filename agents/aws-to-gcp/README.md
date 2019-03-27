# Transporter AWS->GCP Agent
Transporter agent which copies files from AWS to GCP.

## Expected schema
Use this JSON schema to initialize Transporter queues for this agent:
```json
{
  "$schema": "http://json-schema.org/draft-04/schema",
  "type": "object",
  "properties": {
    "s3Bucket": { "type": "string" },
    "s3Path": { "type": "string" },
    "gcsBucket": { "type": "string" },
    "gcsPath": { "type": "string" },
    "expectedSize": { "type": "integer" },
    "expectedMd5": { "type": "string", "pattern": "[0-9a-fA-F]+" }
  },
  "required": ["s3Bucket", "s3Path", "gcsBucket", "gcsPath"],
  "additionalProperties": false
}
```
