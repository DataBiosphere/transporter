# Transporter Agents - Template
Utilities and abstract classes providing a common harness for specialized agents.

## Included Behavior
To reduce the boilerplate required when extending Transporter to handle a
new set of source/destination storage APIs, this template attempts to bundle behavior
required by all agents into a single extend-able package. Included logic is:
* Loading config
* Discovering topic names by querying the Manager
* Initializing the consume-transform-produce flows for transfer initialization and
  incremental upload using [Kafka Streams](https://kafka.apache.org/documentation/streams/).
* Cleaning up resources on graceful shutdown

## Implementing a New Agent
New agent projects within the Transporter project can depend on this sub-project in `sbt`
to pull in template functionality. To "fill in" an agent, they then need to:
1. Declare types modeling their agent's:
   1. Custom configuration (if any)
   2. Request payloads
   3. Progress payloads
   4. Output payloads
2. Define a class implementing `TransferRunner[In, Progress, Out]`, plugging in the custom
   types as parameters. The implementation must define two methods:
   1. An `initialize` method to transform a transfer request into an initial progress message
   2. A `step` method to transform a progress message into either the next progress message
      or a result message
3. Define a main class implementing `TransporterAgent[Config, In, Progress, Out]`,
   plugging in the custom types. The implementation must define one method to use the custom
   configuration type to build an instance of the custom runner type.
