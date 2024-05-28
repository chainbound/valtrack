# Valtrack

## Getting Started

To get started with Valtrack, you need to clone the repository and install the dependencies.

### 1\. Clone the Repository

```shell
git clone git@github.com:chainbound/valtrack.git
```

### 2\. Install Dependencies and Build

```shell
go mod download
go build
```

### 3\. Run the Application

```shell
./valtrack
```

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
   --nats value, -n value       natsJS server url
   --help, -h                   show help
```

</details>

## Details

### Beacon Sentry

The Beacon Sentry is a service that crawls the Ethereum DHT network and discovers nodes. The discovered nodes are then tried to connect and perform successful handshake with them. Successful handshakes allows us to get the peer's status and metadata.

The sentry data is published to NATS server by Jetstreams, provided NATS url is provided as argument. If not provided, it saves the entries in log files.

To run the Beacon Sentry service (without NATS):

```shell
./valtrack sentry
```

To run the Beacon Sentry service (with NATS):

```shell
./valtrack --nats <NATS_URL> sentry
```

### Consumer

Consumer is a service which consumes the sentry data from the NATS Jetstream server and stores it in parquet file (database soon).

The `metadata_received` sentry data is used to calculate the subnets information of the peers. The metadata will contain both long-lived and short-lived attestation subnet subscriptions. We can filter out the long-lived subnet subscriptions ([`compute_subscribed_subnets`](https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/p2p-interface.md#attestation-subnet-subscription)) to get the short-lived subscriptions. Prysm [`computeSubscribedSubnets`](https://github.com/prysmaticlabs/prysm/blob/2f2152e039c6e9273873cfec73bf5edefcd41d39/beacon-chain/p2p/subnets.go#L210) is used for the same.

-   Metadata with subnet information is stored in `validator_metadata_events.parquet`
-   `metadata_received` event data is stored in `metadata_events.parquet`
-   `peer_discovered` event data is stored in `discovery_events.parquet`

To run the consumer:

```shell
./valtrack consumer
```

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
