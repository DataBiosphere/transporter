# Transporter Agents - Echo
Transporter agent which does not transfer files.

## Motivating Projects
This agent is intended to be used for internal projects such as:
* Manual plumbing validation by developers
* Stress-testing by Dev/Sec Ops
* (Future) Automated integration testing

## Maintenance Status
Since this agent isn't used in "real" projects, its code health is likely to lag.
Hopefully this will improve once we add integration tests which use this agent.

## Expected Schema
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

## Expected Configuration
Deployments of this agent should include an `application.conf` setting the properties:
```hocon
org.broadinstitute.transporter.runner-config {
  # Percentage of requests which the agent should report as failed.
  transient-failure-rate: ???
}
```
