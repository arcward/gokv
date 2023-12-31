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
time=2023-11-26T15:13:50.083-05:00 level=INFO msg="starting server" address=:33969 config.HashValues=false config.ssl-certfile="" config.ssl-keyfile="" config.backup="" config.MaxNumberOfKeys=0 config.DefaultMaxValueSize=1000000 config.History.RevisionLimit=5
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
  "hash":"626172d41d8cd98f00b204e9800998ecf8427e",
  "created":{"seconds":1704058237,"nanos":591124661},
  "version":1,
  "size":3,
  "content_type":"text/plain; charset=utf-8"
  }
```

Note: `version` is incremented whenever the value of a key is updated. If
`--hash-algorithm` is specified (ex: `--hash-algorithm=MD5`), then the hash
of the value will be used to determine whether or not to increment the version.
If values aren't hashed, then the version will be incremented whether or not
the new value is different from the old value.

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

When the server is run with the `--snapshot-dir` flag, on shutdown, it will
attempt to write a gzipped JSON file to the directory specified by the flag,
with all current key-value pairs. On startup, it will look for the most
recent snapshot file based on filename (named 
like `{unix-timestamp}.json.gz`), and attempt to restore from
that file. Example:

```shell
$ ./dist/bin/keyquarry server --snapshot-dir ./snapshots --snapshot-interval=5m
```

Snapshots can be enabled with `--snapshot`.

If you specify `--snapshot-interval`, the server will write a snapshot file
at each interval, in addition to the snapshot file written on shutdown. When
using this flag, `--snapshot-dir` defaults to the current working directory.
If you don't specify `--snapshot-interval`, the server will only write a
snapshot file on shutdown.

Snapshots will only be written if a change is detected (meaning a key has 
been set, updated or deleted, either by a client or by the server itself via
expiry). If hashing isn't enabled, any update will be considered a change.

Snapshots can be automatically encrypted with `--encrypt-snapshots`, which
requires setting `SNAPSHOT.SECRET_KEY` or envvar 
`KEYQUARRY_SNAPSHOT_SECRET_KEY`.

```shell

## Docker

### Build the image

```shell
$ docker build -t keyquarry:dev .
```

### Run the server

This will run the server in the foreground, with the backup file
`/data/backup.json` on the host machine, so that it can be restored
on startup:

```shell
$ mkdir data
$ docker run --interactive --tty \  
    --mount type=bind,source=${PWD}/data,target=/data \
    --publish 33969:33969 \
     keyquarry:dev server --load-snapshot=/data/backup.json.gz
```

To use TLS/SSL with local files `cert.crt` and `cert.key` under `data/ssl`:

```shell
$ docker run --interactive --tty \
    --env keyquarry_SERVER_SSL_CERTFILE=/data/ssl/cert.crt \
    --env keyquarry_SERVER_SSL_KEYFILE=/data/ssl/cert.key \
    --mount type=bind,source=${PWD}/data/ssl,target=/data/ssl \
    --publish 33969:33969 \
     keyquarry:dev server --log-level=DEBUG
```

```shell
$ docker run --interactive --tty \
    --publish 33969:33969 \
     keyquarry:dev server --log-level=INFO

```
