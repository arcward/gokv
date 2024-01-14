# keyquarry

An in-memory key-value store with gRPC API.

## Quickstart

### Build

```shell
$ make build
built ./dist/bin/keyquarry
```

### Run the server

By default, the server listens on `localhost:33969`, without TLS:

```shell
$ ./dist/bin/keyquarry server
time=2023-11-26T15:13:50.083-05:00 level=INFO msg="starting server" address=:33969 config.ssl-certfile="" config.ssl-keyfile="" config.backup="" config.MaxNumberOfKeys=0 config.DefaultMaxValueSize=1000000 config.History.RevisionLimit=5
```

Alternatively, you can specify a unix socket like:

```shell
$ ./dist/bin/keyquarry server --listen unix:///tmp/keyquarry.sock
```

Or bind to all interfaces, accepting connections from anywhere:

```shell
$ ./dist/bin/keyquarry server --listen :39969
```

### Client usage

Set a key `foo` to value `bar`:

```shell
$ ./dist/bin/keyquarry client set foo bar
{"success":true,"isNew":true}
```

Get the value of `foo`:

```shell
$ ./dist/bin/keyquarry client get foo
bar
```

Update the value of `foo`:

```shell
$ ./dist/bin/keyquarry client set foo baz
$ ./dist/bin/keyquarry client get foo
baz
```

Get metadata for `foo`, indenting the output with two spaces:

```shell
$ ./dist/bin/keyquarry client --indent=2 info foo
{
  "key":"foo",
  "hash":16101355973854746,
  "created":{"seconds":1704058237,"nanos":591124661},
  "version":1,
  "size":3,
  "content_type":"text/plain; charset=utf-8"
  }
```

Note: `version` is incremented whenever the value of a key is updated, based
on the hash of the value.

Lock `foo`, fail to update its value, then unlock and delete `foo`:

```shell
$ ./dist/bin/keyquarry client lock foo 30s
{"success":true}
$ ./dist/bin/keyquarry client --client-id=bar set foo baz
time=2023-11-29T14:57:53.813-05:00 level=ERROR source=/home/edward/sdk/keyquarry/cmd/root.go:498 msg="rpc error: code = PermissionDenied desc = key is locked"
exit status 1
$ ./dist/bin/keyquarry client unlock foo
{"success":true}
$ ./dist/bin/keyquarry client delete foo
{"deleted":true}
```

By default, `--client-id` is set as `{user}@{hostname}`. You can override this.
Only the client ID that locked a key can unlock it, extend the lock timeout,
or get/set the value.

### Backup/restore

You can persist the current state of the server to a SQLite or Postgres
database. First, set `KEYQUARRY_SNAPSHOT_ENABLED=true`.

Set  `KEYQUARRY_SNAPSHOT_DATABASE` environment  variable to your connection 
string (ex: `sqlite:///path/to/file.db` or 
`postgres://user:password@host:port/dbname`).

By default, the server will load this state on start (if any snapshots exist),
and save its state on shutdown. To disable loading on start, set
`KEYQUARRY_START_FRESH=true`. To create new snapshots periodically,
set `KEYQUARRY_SNAPSHOT_INTERVAL` with a duration (ex: `5m`). If no interval
is specified, a snapshot will only be created on shutdown.

On startup, it will look for the most recent snapshot record based on `KEYQUARRY_NAME` (by 
default, set as `{user}@{host}`), attempting to restore from that data.

## Docker

### Build the image

```shell
$ docker build -t keyquarry:dev .
```

(Or `docker compose build server`)

### Run the server, snapshot DB and jaeger

```shell
$ docker compose up -d
```

The compose file enables telemetry by default (`KEYQUARRY_TRACE=true`), and traces
can be viewed in jaeger at `http://localhost:16686`

You can monitor events with the client with `docker compose logs -f client`.
