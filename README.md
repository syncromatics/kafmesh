# Kafmesh

Kafmesh is a [Kafka] streaming and observability framework built for go. It's
built using [Goka] and generates strongly-typed wrappers to Goka's streaming
concepts that are easy to write and unit-test.

TODO: note on requirement for protocol buffers

## Features

  * **gRPC-like code generation for streaming services**

    Kafmesh components are defined in yaml files and generated via
    `kafmesh-gen`. The generated code makes it easy to unit test streaming
    services.

  * **Discovery api to aggregate multiple running services into a single view**

    Kafmesh defines discovery via a grpc service. All running services
    implement this api and will respond with the parts of the service that
    are currently running in process.

  * **Stream visualizer and reading with kafql**

## Getting Started

### Installation

Install the kafmesh generator by using:

`go get -u github.com/syncromatics/kafmesh/cmd/kafmesh-gen`

### Concepts

A kafmesh service is built from components that have specific functions. A
component is made up of different streaming objects

  * **Source**

    A source is how data gets into kafka. This could be from a grpc service
    getting information from different sources, such as clicks from a
    webpage, gps positions from vehicles, anything you would need. This is a
    strongly typed wrapper around the [Goka
    Emitter](https://github.com/lovoo/goka#concepts) concept

* **Processor**

  A processor is where the bulk of the stream processing takes place. This is
  a strongly typed wrapper around the [Goka
  Processor](https://github.com/lovoo/goka#concepts) concept.

* **View**

  A view is a look into what kafka is storing for a particular topic. It takes a feed from a kafka topic and provides a key/value datastore. This is a strongly typed wrapper aroudn the [Goka View](https://github.com/lovoo/goka#concepts) concept.

* **ViewSource**

  View source will keep a kafka compacted topic in sync with an external source.

* **ViewSink**

  A view sink will sink a kafka compacted topic into a sink.

* **Sink**

  A sink will ship kafka messages to an external datastore such as a database.

## Usage

You can use kafmesh-gen by running

`kafmesh-gen docs/definition.yaml`

### Example service

See [kafmesh-example] for a complete usage demo.

docs/definition.yaml
```yaml
name: exampleService
description: A example kafmesh service.

output:
  package: definitions
  path: internal/definitions
  module: example-service

messages:
  protobuf:
    - ../protos

components:
  - ./components/*.yaml

defaults:
  partition: 10
  replication: 3
  retention: 24h
  segment: 12h

```
docs/components/math.yaml
```yaml
name: math
description: Does some simple math.

sources:
  - message: userId.click

processors:
  - name: total clicks
    inputs:
      - message: userId.click
    outputs:
      - message: userId.totalClicks
    persistence:
      - message: userId.totalClicksState

views:
  - message: userId.totalClicks

```

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* [goka](https://github.com/lovoo/goka) A big shout out for goka doing the hard part of building out a kafka streaming service for go.

* [Visual Studio Code](https://code.visualstudio.com/) for just being an all around great editor

[Kafka]: https://kafka.apache.org/
[Goka]: https://github.com/lovoo/goka
[kafmesh-example]: https://github.com/syncromtaics/kafmesh-example
