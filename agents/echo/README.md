# Transporter Echo Agent
Example "agent" program for Transporter which returns its input back to the manager,
or randomly fails with a "transient" error.

## Expected schema
Use this JSON schema to initialize Transporter queues for this agent:
```json
{
  "$schema": "http://json-schema.org/draft-04/schema",
  "type": "object",
  "properties": {
    "message": { "type":  "string" },
    "fail": { "type":  "boolean" }
  },
  "required": ["message", "fail"],
  "additionalProperties": false
}
```
