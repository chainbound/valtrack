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

To run the Beacon Sentry:

```shell
./valtrack sentry
```

### Consumer

Consumer is a service which consumes the sentry data from the NATS Jetstream server and stores it in parquet file (database soon).

To run the consumer:

```shell
./valtrack consumer
```
