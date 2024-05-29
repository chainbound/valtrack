# Valtrack

> Valtrack is a suite of tools aimed at geo-locating and tracking Ethereum validators.

Sample output in CSV: [validator_metadata_events_sample.csv](validator_metadata_events_sample.csv)

## Getting Started

To get started with Valtrack, you need to clone the repository and install the dependencies.

### 1. Clone the Repository

```shell
git clone git@github.com:chainbound/valtrack.git
```

### 2. Install Dependencies and Build

Valtrack:

```shell
go mod download
go build
```

NATS:

-   https://docs.nats.io/running-a-nats-service/introduction/installation

### 3. Run the Application

Valtrack consists of 3 main components: the **sentry**, the **consumer** and a NATS JetStream server. The sentry is responsible for discovering and tracking Ethereum validators, while the consumer consumes the data published by the sentry, processed it and stores it in a database (Parquet files for now). Read more under [Architecture](#architecture).

#### Sentry

To run the sentry:

```shell
./valtrack --nats-url nats://localhost:4222 sentry
```

#### Consumer

```shell
./valtrack --nats-url nats://localhost:4222 consumer
```

#### NATS JetStream

We provide an example configuration file for the NATS server in [server/nats-server.conf](server/nats-server.conf). To run the NATS server with JetStream enabled, you can run the following command:

```shell
nats-server --jetstream --config server/nats-server.conf
```

This will create a `data` directory in the current working directory with all the JetStream data.

<details>
<summary>This should print this help text</summary>

```text
NAME:
   valtrack - Ethereum consensus validator tracking tool

USAGE:
   valtrack [global options] command [command options] [arguments...]

COMMANDS:
   sentry    run the sentry node
   consumer  run the consumer
   help, h   Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --log-level value, -l value  log level (default: "info")
   --nats-url value, -n value   NATS server URL (needs JetStream) (default: "nats://localhost:4222")
   --help, -h                   show help
```

</details>

## Architecture

### Beacon Sentry

The Beacon Sentry is a service that crawls the Ethereum discv5 DHT and discovers nodes and listens for incoming connections.
It tries to connect to any discovered peers and performs a handshake for their [`Status`](https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#status) and [`MetaData`](https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#metadata).

In parallel, it will try to gauge the peer's GossipSub subscriptions, specifically to the attestation subnet topics. Once it has all of that data,
it will publish it to the NATS Jetstream server.

Peers are stored in a peerstore that will periodically (every epoch) run through this process again. This allows us to get multiple data points over time which will provide more accuracy in determining the number of validators attached to a beacon node.

TODO: The beacon sentry should later store estimations on the number of validators attached to a beacon node. It should then expose it over an API.

### Consumer

Consumer is a service which consumes the sentry data from the NATS Jetstream server and stores it in parquet file (database soon). Maintains 3 tables:

-   `discovery_events`: contains the discovery events of the sentry
-   `metadata_events`: contains the metadata events of the sentry
-   `validator_metadata_events`: a derived table from the metadata events, which contains data points of validators

### NATS Server

[NATS](https://docs.nats.io/nats-concepts/what-is-nats) is a message oriented middleware. Valtrack uses NATS Jetstream which enables message persistence funcionalities.

#### Installation

Check the official NATS documentation for NATS installation instructions [DOCS](https://docs.nats.io/using-nats/nats-tools/nats_cli)

To run the NATS Jetstream server:

```shell
nats-server --jetstream
```

#### Stream Configuration

It is important to create stream with the right configuration according to the requirements. [DOCS](https://docs.nats.io/nats-concepts/jetstream/streams#configuration)

Valtrack uses the following configuration:

```golang
jetstreamCfg := jetstream.StreamConfig{
		Name:      "EVENTS",
		Retention: jetstream.InterestPolicy,
		Subjects:  []string{"events.metadata_received", "events.peer_discovered"},
	}
```

-   RetentionPolicy is set to InterestPolicy, which means that the messages are retained based on the consumer interest in the messages. The messages are retained until is not acknowledged by all the consumers. If there is zero consumer, the messages are deleted i.e. not retained. [DOCS](https://docs.nats.io/nats-concepts/jetstream/streams#retentionpolicy)
-   The subjects are the NATS subjects where the sentry data is published.

#### Consumer Configuration

Consumer configuration [DOCS](https://docs.nats.io/nats-concepts/jetstream/consumers#configuration)
Valtrack uses the following configuration:

```golang
consumerCfg := jetstream.ConsumerConfig{
		Name:        fmt.Sprintf("consumer-%s", uniqueID),
		Durable:     fmt.Sprintf("consumer-%s", uniqueID),
		Description: "Consumes valtrack events",
		AckPolicy:   jetstream.AckExplicitPolicy,
	}
```

-   Durable is set, which means that the consumer will be binded until explicitly deleted.
-   AckPolicy is set to AckExplicitPolicy, which means that the consumer has to explicitly acknowledge the message. [DOCS](https://docs.nats.io/nats-concepts/jetstream/consumers#ackpolicy)

#### Useful Commands

-   Check stream info

```shell
nats stream info <STREAM_NAME> --server <NATS_URL>
```

-   Delete stream

```shell
nats stream rm <STREAM_NAME> --server <NATS_URL>
```

-   Check consumer info

```shell
nats consumer ls <STREAM_NAME> --server <NATS_URL>
```

-   Delete consumer

```shell
nats consumer rm <STREAM_NAME> <CONSUMER_NAME> --server <NATS_URL>
```

## Credits

Shoutout to the following projects for inspiration and reference :

-   [Hermes](https://github.com/probe-lab/hermes)

-   [Armiarma](https://github.com/migalabs/armiarma/)
