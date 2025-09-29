# replset

A simple Go CLI to start and stop a local MongoDB replica set.

## Requirements

* Go 1.21+
* MongoDB binaries (`mongod`, `mongosh`) available in `$PATH`

## Usage

Start ephemeral replica set (data wiped each run):

```sh
go run ./cmd/replset
```

Start persistent replica set (data kept under `./data`):

```sh
go run ./cmd/replset --persistent
```

Stop replica set:

```sh
go run ./cmd/replset stop
```

Data is stored under `./data/rs0-*`. Logs are in each folder.
