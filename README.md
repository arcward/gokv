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
$ ./dist/bin/keyquarry serve
time=2023-11-26T15:13:50.083-05:00 level=INFO msg="starting server" address=:33969 config.ssl-certfile="" config.ssl-keyfile="" config.backup="" config.MaxNumberOfKeys=0 config.DefaultMaxValueSize=1000000 config.History.RevisionLimit=5
```

Alternatively, you can specify a unix socket like:

```shell
$ ./dist/bin/keyquarry serve --listen unix:///tmp/keyquarry.sock
```

Or bind to all interfaces, accepting connections from anywhere:

```shell
$ ./dist/bin/keyquarry serve --listen :39969
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

## Methods

### Register

Adds a new client ID to the server. This is used to identify the client
in other methods for the purpose of locking/unlocking keys. Subsequent
calls of other methods will fail if the client ID is not registered,
or included in the request metadata as `client_id`.

### Set

Creates a new `key` with the provided `value`. If the key already exists,
it will be updated. 

If `lock_duration` is provided, the key will be locked for that duration, 
and automatically unlocked after it has passed.

If `lifespan` is provided, the key will be deleted after that duration. The
lifespan will be extended if the key is updated before it expires. Subsequent
calls to `Set` with a lifespan will reset the lifespan to the given duration.
This takes precedence over `lock_duration`, in that the key may be deleted
prior to being automatically unlocked, if the lifespan is shorter than the
lock duration.

If a key is locked, only the `client_id` that set the lock can update the key.

### Lock

Locks a key for the given duration. If the key is already locked, the lock
will be extended by the given duration. If the key is not already locked, a new
lock is created. After the lock duration has passed, the key will be
automatically unlocked.

Locked keys can only be updated by the same `client_id` that created the lock.

If the lock duration is shorter than the lifespan of the key, the key may be
deleted prior to the lock expiring.

Creating/updating a lock will not reset the lifespan of the key.

### Unlock

Unlocks a key. If the key is not locked, nothing will happen. Only the
same `client_id` that created the lock can unlock the key.

Unlocking a key will not reset the lifespan of the key.

### Get

Gets the value of a key. If the key does not exist, or the key is
currently locked by a different `client_id`, this will return an error.

### Inspect

Inspect returns metadata about a key. If the key does not exist, this will
return an error. 

If `include_value` is true, the value of the key will be included in the response,
unless the key is currently locked by another `client_id`, in which case an error
will be returned.

If `include_metrics` is true, additional access/set/lock data will be included in 
the response.

### Delete

Deletes a key. If the key does not exist, or is currently locked
by a different `client_id`, this will return an error.

### Exists

Indicates whether a key exists.

### Pop

Gets the value of a key, and deletes the key. If the key does not exist, or is
currently locked by a different `client_id`, this will return an error.

### ListKeys

Returns a list of keys. Results can be filtered with `pattern`, which,
if provided, must be a valid regex for `regexp.MatchString`. Limit the
number of results with `limit` (0 for no limit).

### WatchStream

Returns a stream of events. Watch specific keys with `keys` (if empty, all
keys will be watched). Filter for specific events with `events` (if empty,
all events will be watched).

### WatchKeyValue

Returns a stream which will emit the value of a key whenever it is updated.
The current value of the key will be emitted immediately. Additional metadata
about the key is also provided. Values will only be sent when a key is
created, updated, deleted, expired or expunged. Use `KeyEvent` to determine
whether the value was actually updated, and when the key no longer exists.

If the key is locked by another `client_id`, values will not be sent until
the key is unlocked. For an `Unlock` event, the current value will be
sent immediately.

For `Deleted`, `Expired` or `Expunged` events, the value of the key at
the time of the event will be sent, whether or not it has changed.

### GetRevision

Gets the value of a key for a specific revision. If the key does not exist,
or the revision does not exist, this will return an error.

### GetKeyMetric

Returns metrics related to a key. If the key does not exist, this will return
an error.

## Server configuration

Example YAML configuration:

```yaml
listen_address: localhost:33969
name: foo@bar
graceful_shutdown_timeout: 30s

log_level: NOTICE
log_json: false
log_events: false

event_stream_buffer_size: 1000
event_stream_send_timeout: 1s
event_stream_subscriber_limit: 0

snapshot:
  database: postgres://keyquarry:keyquarry@localhost:5432/keyquarry
  enabled: true
  interval: 5m

monitor_address: localhost:33970
metrics: false
expvar: false
pprof: false
trace: false
service_name: keyquarry

revision_limit: 5
max_key_length: 1024
max_value_size: 1000000
min_lock_duration: 5s
max_lock_duration: 5m
min_lifespan: 5s

max_keys: 10000
prune_interval: 1h
prune_at: 10000
prune_to: 9000
eager_prune_at: 20000
eager_prune_to: 15000
```

### List of options

Each of these options can be set as an environment variable, or in a YAML
configuration file. The environment variable name is the same as the option,
prefixed with `KEYQUARRY_`, uppercased, with any period replaced by an underscore. 
For example, `listen_address` can be set as `KEYQUARRY_LISTEN_ADDRESS` in the
environment, while `snapshot.database` would be `KEYQUARRY_SNAPSHOT_DATABASE`
in the environment.

- `name`
- `service_name`
- `listen_address`
- `graceful_shutdown_timeout`
- `log_level`
- `log_json`
- `log_events`
- `event_stream_buffer_size`
- `event_stream_send_timeout`
- `event_stream_subscriber_limit`
- `monitor_address`
- `prometheus`
- `expvar`
- `pprof`
- `trace`
- `max_key_length`
- `max_keys`
- `max_value_size`
- `min_lock_duration`

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
