# gokv

An in-memory key-value store with gRPC API.

## Quickstart

### Build

```shell
$ make build
built ./dist/gokv-linux-amd64
```

### Run the server

By default, it listens on port `33969`, without TLS:

```shell
$ ./dist/gokv-linux-amd64 server
time=2023-11-26T15:13:50.083-05:00 level=INFO msg="starting server" address=:33969 config.HashValues=false config.ssl-certfile="" config.ssl-keyfile="" config.backup="" config.MaxNumberOfKeys=0 config.DefaultMaxValueSize=1000000 config.History.RevisionLimit=5
```

### Client usage

Set a key `foo` to value `bar`:

```shell
$ ./dist/gokv-linux-amd64 client --no-tls set foo bar
success:true  is_new:true
```

Get the value of `foo`:

```shell
$ ./dist/gokv-linux-amd64 client --no-tls get foo
bar
```

Update the value of `foo`:

```shell
$ ./dist/gokv-linux-amd64 client --no-tls set foo baz
$ ./dist/gokv-linux-amd64 client --no-tls get foo
baz
```

Get metadata for `foo` (versions start at 0 and increment only when the 
value is set):

```shell
$ ./dist/gokv-linux-amd64 client --no-tls info foo
key:"foo"  created:1701029048652039449  updated:1701029311808824146  version:1  accessed:1701029339919359375  size:3  content_type:"text/plain; charset=utf-8"
```

Lock `foo`, fail to update its value, then unlock and delete `foo`:

```shell
$ ./dist/gokv-linux-amd64 client --no-tls lock foo --lock-timeout 30s
success:true
$ ./dist/gokv-linux-amd64 client --no-tls set foo bar
rpc error: code = PermissionDenied desc = key is Locked
$ ./dist/gokv-linux-amd64 client --no-tls unlock foo
success:true
$ ./dist/gokv-linux-amd64 client --no-tls delete foo
deleted:true
```

### Backup/restore

When the server is run with the `--backup` flag, on shutdown, it will
attempt to write a backup JSON file to the path specified by the flag,
with all current key-value pairs. On startup, if the file exists, it
will attempt to restore all key-value pairs from the file. Example:

```shell
$ ./dist/gokv-linux-amd64 server --backup=gokv-backup.json
```
