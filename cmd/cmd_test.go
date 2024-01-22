package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	pb "github.com/arcward/keyquarry/api"
	kc "github.com/arcward/keyquarry/client"
	"github.com/arcward/keyquarry/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

var (
	signals            = make(chan os.Signal, 1)
	ctx                context.Context
	cancel             context.CancelFunc
	defaultClientID    = "foo"
	privilegedClientID = "someprivilegedidhere"
	testTimeout        = 60 * time.Second
	dialTimeout        = 10 * time.Second
)

type AddrTest struct {
	ListenAddress string
	SocketFile    string
	URL           *url.URL
}

func newClient(t *testing.T, addr AddrTest) *kc.Client {
	t.Helper()

	client := kc.New(
		addr.ListenAddress,
		defaultClientID,
		nil,
		nil,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:    kc.DefaultDialKeepAliveTime,
				Timeout: kc.DefaultDialKeepAliveTimeout,
			},
		),
	)
	connCtx, connCancel := context.WithTimeout(ctx, dialTimeout)
	err := client.Dial(connCtx, true)

	if err != nil {
		panic(err)
	}
	connCancel()
	t.Cleanup(
		func() {
			discErr := client.CloseConnection()
			if discErr != nil {
				panic(discErr)
			}
		},
	)
	return client
}

func socketAddr(t *testing.T) AddrTest {
	t.Helper()
	tdir := t.TempDir()
	unixSocket := filepath.Join(tdir, fmt.Sprintf("%s.sock", t.Name()))
	listenAddress := fmt.Sprintf("unix://%s", unixSocket)
	addr := AddrTest{
		ListenAddress: listenAddress,
		SocketFile:    unixSocket,
	}
	u, err := parseURL(listenAddress)
	if err != nil {
		t.Fatalf("error parsing listen address: %s", err.Error())
	}
	addr.URL = u
	return addr
}

//goland:noinspection GoVetFailNowInNotTestGoroutine
func newServer(
	t *testing.T,
	cfg *server.Config,
	addr AddrTest,
) *server.Server {
	t.Helper()
	opts := &cliOpts
	currentClientOpts := &opts.clientOpts
	serverCfg := server.NewConfig()
	serverCfg.ListenAddress = addr.ListenAddress
	newClientOpts := &clientOptions{
		ClientID:             t.Name(),
		Address:              addr.ListenAddress,
		DialKeepAliveTimeout: kc.DefaultDialKeepAliveTimeout,
		DialKeepAliveTime:    kc.DefaultDialKeepAliveTime,
		DialTimeout:          kc.DefaultDialTimeout,
		NoTLS:                true,
		Verbose:              true,
	}
	newOpts := &cliConfig{
		ServerOpts: *serverCfg,
		clientOpts: *newClientOpts,
	}

	*opts = *newOpts
	*currentClientOpts = *newClientOpts

	var srv *server.Server

	td := os.Getenv("TEST_TIMEOUT")
	if td != "" {
		var ee error
		testTimeout, ee = time.ParseDuration(td)
		if ee != nil {
			panic(
				fmt.Sprintf(
					"failed to parse TEST_TIMEOUT: %s",
					ee.Error(),
				),
			)
		}
	}

	tctx, tcancel := context.WithTimeout(ctx, testTimeout)
	go func() {
		select {
		case <-signals:
			panic(fmt.Sprintf("%s: interrupted", t.Name()))
		case <-tctx.Done():
			if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
				//goland:noinspection GoVetFailNowInNotTestGoroutine
				t.Fatalf("%s: timeout exceeded", t.Name())
			}
		}
	}()

	t.Cleanup(
		func() {
			tcancel()
			rootCmd.SetContext(ctx)
		},
	)
	rootCmd.SetContext(tctx)

	var err error
	switch {
	case cfg == nil:
		cfg = server.NewConfig()
		cfg.RevisionLimit = 2
		cfg.MinLifespan = time.Duration(1) * time.Second
		cfg.MinLockDuration = time.Duration(1) * time.Second
		cfg.PruneInterval = 0
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
		cfg.ListenAddress = addr.ListenAddress
		cfg.MaxLockDuration = time.Duration(1) * time.Hour
		cfg.PrivilegedClientID = t.Name()
		slog.SetDefault(cfg.Logger)
		srv, err = server.New(cfg)
		if err != nil {
			panic(err)
		}
	default:
		cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, nil))
		cfg.ListenAddress = addr.ListenAddress
		slog.SetDefault(cfg.Logger)
		srv, err = server.New(cfg)
		if err != nil {
			panic(err)
		}
	}

	fatalOnErr(
		t,
		clientCmd.PersistentFlags().Set("address", addr.ListenAddress),
	)
	fatalOnErr(
		t,
		clientCmd.PersistentFlags().Set("client-id", t.Name()),
	)
	fatalOnErr(
		t,
		clientCmd.PersistentFlags().Set("no-tls", "true"),
	)

	go func() {
		if e := srv.Serve(tctx); e != nil {
			panic(e)
		}
	}()

	t.Cleanup(
		func() {
			tcancel()
		},
	)

	socketCtx, socketCancel := context.WithTimeout(tctx, 15*time.Second)
	for {
		if socketCtx.Err() != nil && errors.Is(
			socketCtx.Err(),
			context.DeadlineExceeded,
		) {
			t.Fatalf(
				"error waiting for socket '%s': %s",
				srv.Config().ListenAddress,
				socketCtx.Err().Error(),
			)
		}
		sf, e := os.Stat(addr.SocketFile)

		if e == nil {
			t.Logf(
				"no error statting %s (%s / %d / %s)",
				addr.SocketFile,
				sf.Name(),
				sf.Size(),
				sf.Mode().String(),
			)
			socketCancel()
			break
		}

		if !errors.Is(e, os.ErrNotExist) {
			t.Fatalf(
				"err: %s (addr: %s)",
				e.Error(),
				srv.Config().ListenAddress,
			)
		}

		// if e != nil && !errors.Is(e, os.ErrNotExist) {
		// 	t.Fatalf(
		// 		"err: %s (addr: %s)",
		// 		e.Error(),
		// 		srv.Config().ListenAddress,
		// 	)
		// } else if e == nil {
		// 	t.Logf(
		// 		"no error statting %s (%s / %d / %s)",
		// 		addr.SocketFile,
		// 		sf.Name(),
		// 		sf.Size(),
		// 		sf.Mode().String(),
		// 	)
		// 	socketCancel()
		// 	break
		// }
		time.Sleep(1 * time.Second)
	}
	return srv
}

func clientCtx(t *testing.T) context.Context {
	t.Helper()
	md := metadata.New(map[string]string{"client_id": defaultClientID})
	ictx := metadata.NewIncomingContext(ctx, md)

	return ictx
}

func captureOutput(t *testing.T, f func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("error with pipe: %s", err.Error())
	}
	t.Cleanup(
		func() {
			out = os.Stdout
		},
	)
	out = w

	outC := make(chan string)
	go func() {
		var buf bytes.Buffer
		_, e := io.Copy(&buf, r)
		if e != nil {
			panic(err)
		}
		outC <- buf.String()
	}()
	f()
	err = w.Close()
	if err != nil {
		panic(err)
	}
	o := <-outC
	d := strings.TrimSpace(o)
	t.Logf("result: %s", d)
	return d
}

func init() {
	td := os.Getenv("TEST_TIMEOUT")
	if td != "" {
		var err error
		testTimeout, err = time.ParseDuration(td)
		if err != nil {
			panic(fmt.Sprintf("failed to parse TEST_TIMEOUT: %s", err.Error()))
		}
	}

	ctx, cancel = context.WithCancel(context.Background())

	signal.Notify(signals, os.Interrupt, syscall.SIGHUP, syscall.SIGTERM)
	go func() {
		select {
		case <-signals:
			cancel()
			panic("interrupted")
		case <-ctx.Done():
			panic("canceled")
		}
	}()

	fmt.Println("init done")
}

func TestSetCmd(t *testing.T) {
	_ = newServer(t, nil, socketAddr(t))
	var data string
	rootCmd.SetArgs(
		[]string{
			"client",
			"--verbose",
			"set",
			"foo",
			"bar",
		},
	)

	f := func() {
		failOnErr(t, setCmd.Execute())
	}
	data = captureOutput(t, f)
	rv := pb.SetResponse{Success: true, IsNew: true}
	//goland:noinspection GoVetCopyLock
	expected, err := json.Marshal(rv)
	failOnErr(t, err)
	if err != nil {
		assertEqual(t, data, string(expected))
	}
	t.Logf("expected:\n%s\ngot:\n%s", string(expected), data)

}

func TestGetCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	cctx := clientCtx(t)

	client := newClient(t, addr)
	// set the first value
	value := []byte("baz")

	kx, err := client.Set(cctx, &pb.KeyValue{Key: "foo", Value: value})
	fatalOnErr(t, err)
	t.Logf("created key: %+v", kx)

	// make sure it was set correctly
	rv, err := client.Get(cctx, &pb.Key{Key: "foo"})
	failOnErr(t, err)
	assertEqual(t, string(rv.Value), string(value))
	t.Logf("got value: %+v", rv)

	// clientAddr := client.ServerAddress()
	// t.Logf("client addr: %s", clientAddr)
	//
	// srvAddr := srv.Config().ListenAddress
	// t.Logf("srv addr: %s", srvAddr)
	// // get it via CLI
	rootCmd.SetArgs([]string{"client", "--verbose", "get", "foo"})
	// fatalOnErr(
	// 	t,
	// 	clientCmd.PersistentFlags().Set("address", addr.ListenAddress),
	// )
	// _, err = os.Stat(addr.SocketFile)
	// if err == nil {
	// 	t.Logf("file %s exists", addr.SocketFile)
	// } else {
	// 	t.Fatalf("error with socket file %s: %s", addr.SocketFile, err.Error())
	// }
	data := captureOutput(
		t, func() {
			fatalOnErr(t, getCmd.Execute())
		},
	)
	assertEqual(t, data, string(value))

	// set two new values, so the original should be saved as
	// revision 1, this should be revision 2, and the final value
	// should not be in the history, just be the current value
	targetValue := []byte("newbaz")
	_, err = client.Set(
		cctx,
		&pb.KeyValue{Key: "foo", Value: targetValue},
	)
	failOnErr(t, err)

	insp, e := client.Inspect(cctx, &pb.InspectRequest{Key: "foo"})
	fatalOnErr(t, e)
	assertEqual(t, insp.Version, 2)

	finalValue := []byte("asdf")
	_, err = client.Set(
		cctx,
		&pb.KeyValue{Key: "foo", Value: finalValue},
	)
	failOnErr(t, err)

	// version 2 should be the value set prior to the current value
	rootCmd.SetArgs([]string{"client", "get", "foo", "--revision", "2"})
	getCmd.SetContext(cctx)
	data = captureOutput(
		t, func() {
			failOnErr(t, getCmd.Execute())
		},
	)
	assertEqual(t, data, string(targetValue))

	// `--revision=0` should return the current version
	rootCmd.SetArgs([]string{"client", "get", "foo", "--revision", "0"})
	data = captureOutput(
		t, func() {
			fatalOnErr(t, getCmd.Execute())
		},
	)
	assertEqual(t, data, string(finalValue))
}

func TestPruneCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	cctx := clientCtx(t)
	client := newClient(t, addr)
	// set the first value
	value := []byte("baz")

	kx, err := client.Set(cctx, &pb.KeyValue{Key: "foo", Value: value})
	fatalOnErr(t, err)
	t.Logf("created key: %+v", kx)

	pruneCmd.SetContext(ctx)
	rootCmd.SetArgs([]string{"client", "prune", "1"})
	data := captureOutput(
		t, func() {
			fatalOnErr(t, pruneCmd.Execute())
		},
	)

	var pr pb.PruneResponse
	err = json.Unmarshal([]byte(data), &pr)
	fatalOnErr(t, err)
	assertEqual(t, pr.Pruned, 1)
}

func TestServeCmd(t *testing.T) {
	// When we call `server --config=...`, it will set `cliConfig.configFile`
	// and read the config from there. If we don't reset it after the test,
	// the next test will fail as the file will no longer exist
	opts := &cliOpts
	newOpts := &cliConfig{ServerOpts: *server.NewConfig()}
	*opts = *newOpts

	addr := socketAddr(t)

	monitorAddr := socketAddr(t)

	// Set our own context to control when the server stops
	tctx, tcancel := context.WithTimeout(ctx, testTimeout*5)
	go func() {
		select {
		case <-signals:
			panic(fmt.Sprintf("%s: interrupted", t.Name()))
		case <-tctx.Done():
			if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
				//goland:noinspection GoVetFailNowInNotTestGoroutine
				t.Fatalf("%s: timeout exceeded", t.Name())
			}
		}
	}()

	t.Cleanup(
		func() {
			rootCmd.SetContext(ctx)
		},
	)

	srvContext, srvCancel := context.WithTimeout(tctx, testTimeout)
	go func() {
		select {
		case <-srvContext.Done():
			if e := srvContext.Err(); errors.Is(e, context.DeadlineExceeded) {
				//goland:noinspection GoVetFailNowInNotTestGoroutine
				t.Fatalf("%s: timeout exceeded", t.Name())
			}
		}
	}()
	rootCmd.SetContext(srvContext)
	serverCmd.SetContext(srvContext)

	t.Cleanup(
		func() {
			tcancel()
			serverCmd.SetContext(ctx)
			rootCmd.SetContext(ctx)
		},
	)

	tdir := t.TempDir()

	snapshotInterval := 2 * time.Second

	var newMaxKeySize uint64 = 500
	var newMaxValueSize uint64 = 1234
	var newMaxKeys uint64 = 9876
	var newRevisionLimit int64 = 1234

	dbfile := filepath.Join(tdir, "test.db")
	dbConnStr := fmt.Sprintf("sqlite://%s", dbfile)
	serverName := t.Name()
	tempConfig := fmt.Sprintf(
		`
listen_address: %s
monitor_address: %s
snapshot:
    enabled: true
    database: %s
    interval: %s
max_key_length: %d
max_keys: %d
max_value_size: %d
revision_limit: %d
prune_interval: 1s
log_level: DEBUG
prometheus: true
expvar: true
name: %s
`,
		addr.ListenAddress,
		monitorAddr.ListenAddress,
		dbConnStr,
		snapshotInterval,
		newMaxKeySize,
		newMaxKeys,
		newMaxValueSize,
		newRevisionLimit,
		serverName,
	)

	tmpClient := kc.New(
		addr.ListenAddress,
		defaultClientID,
		nil,
		nil,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:    kc.DefaultDialKeepAliveTime,
				Timeout: kc.DefaultDialKeepAliveTimeout,
			},
		),
	)
	configFile := filepath.Join(tdir, "temp.yaml")
	f, err := os.OpenFile(configFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("error writing %s: %s", configFile, err.Error())
	}

	writer := bufio.NewWriter(f)
	_, err = writer.WriteString(tempConfig)

	if err != nil {
		t.Fatalf("error writing %s: %s", configFile, err.Error())
	}
	err = writer.Flush()
	if err != nil {
		t.Fatalf("error writing %s: %s", configFile, err.Error())
	}
	fatalOnErr(t, f.Close())

	rootCmd.SetArgs(
		[]string{
			"serve",
			"--log-level",
			"INFO",
			"--config",
			configFile,
		},
	)

	// Execute the command to start the server, track when it's done,
	// so we know it's safe to check for the existence of the socket file
	execDone := make(chan struct{}, 1)
	go func() {
		e := serverCmd.Execute()
		fatalOnErr(t, e)
		execDone <- struct{}{}
	}()

	// Wait for the server and monitor socket files to exist, max 5 seconds
	socketCtx, socketCancel := context.WithTimeout(tctx, 15*time.Second)
	var rpcSocketFound bool
	var monitorSocketFound bool
	for {
		if socketCtx.Err() != nil {
			t.Fatalf(
				"error waiting for socket '%s': %s",
				addr.SocketFile,
				socketCtx.Err().Error(),
			)
		}
		if !rpcSocketFound {
			_, err = os.Stat(addr.SocketFile)
			switch {
			case err == nil:
				t.Logf("no error statting %s", addr.SocketFile)
				rpcSocketFound = true
			default:
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("err: %s", err.Error())
				}
			}
		}

		if !monitorSocketFound {
			_, err = os.Stat(monitorAddr.SocketFile)
			switch {
			case err == nil:
				t.Logf("no error statting %s", monitorAddr.SocketFile)
				monitorSocketFound = true
			default:
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("err: %s", err.Error())
				}
			}
		}

		if rpcSocketFound && monitorSocketFound {
			socketCancel()
			break
		}
		time.Sleep(1 * time.Second)
	}

	connCtx, connCancel := context.WithTimeout(tctx, dialTimeout)
	err = tmpClient.Dial(connCtx, false)
	connCancel()
	if err != nil {
		t.Fatalf("error dialing: %s", err.Error())
	}
	t.Cleanup(
		func() {
			_ = tmpClient.CloseConnection()
		},
	)

	connStr := fmt.Sprintf("sqlite://%s?mode=ro", dbfile)
	dialect := server.GetDialect(connStr)
	if dialect == nil {
		t.Fatalf("error getting dialect for %s", connStr)
	}
	db, err := dialect.DB(connStr)
	fatalOnErr(t, err)

	if err != nil {
		t.Fatalf("error connecting to db: %s", err.Error())
	}

	// Loop up to the snapshot limit + 1, so we can verify that the oldest
	// snapshot is deleted when the limit is reached
	var snapshotCt int

	for i := 0; i < 5; i++ {
		// Create a new key on each iteration, with a name reflecting
		// each iteration number
		kv := &pb.KeyValue{Key: fmt.Sprintf("foo-%d", i), Value: []byte("bar")}
		rv, e := tmpClient.Set(srvContext, kv)
		fatalOnErr(t, e)
		assertEqual(t, rv.IsNew, true)
		assertEqual(t, rv.Success, true)
		ssctx, sscancel := context.WithTimeout(srvContext, 30*time.Second)

		// Loop until we see the snapshot file for the current key, or until
		// we time out
		for {
			if ssctx.Err() != nil {
				sscancel()
				if errors.Is(ssctx.Err(), context.DeadlineExceeded) {
					t.Fatalf("timeout waiting for snapshot %d", i)
				}
				t.Logf(
					"breaking out of snapshot on loop %d: %s",
					i,
					ssctx.Err().Error(),
				)
				break
			}

			err = db.QueryRowContext(
				tctx,
				"SELECT COUNT(*) FROM snapshots WHERE server_name = ?",
				serverName,
			).Scan(&snapshotCt)
			if err != nil {
				t.Logf("failed to query: %s", err.Error())
			}
			if snapshotCt > i {
				break
			}

			time.Sleep(1 * time.Second)
		}
		sscancel()
	}
	assertEqual(t, snapshotCt, 5)

	// verify the prometheus endpoint responds
	monitorURL := monitorAddr.URL
	dialFunc := func(ctx context.Context, network, addr string) (
		net.Conn,
		error,
	) {
		return net.Dial(monitorURL.Scheme, monitorURL.Host)
	}
	httpClient := http.Client{
		Transport: &http.Transport{
			DialContext: dialFunc,
		},
	}
	resp, err := httpClient.Get("http://unix/metrics")

	fatalOnErr(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()
	assertEqual(t, resp.StatusCode, http.StatusOK)

	body, err := io.ReadAll(resp.Body)
	fatalOnErr(t, err)

	t.Logf("data: %s", string(body))

	// verify the /debug/vars expvar endpoint responds, and reflects
	// sane values
	resp, err = httpClient.Get("http://unix/debug/vars")

	fatalOnErr(t, err)
	defer func() {
		_ = resp.Body.Close()
	}()
	assertEqual(t, resp.StatusCode, http.StatusOK)

	body, err = io.ReadAll(resp.Body)
	fatalOnErr(t, err)

	type expvarMetrics struct {
		Keyquarry pb.ServerMetrics `json:"keyquarry"`
	}
	var metrics expvarMetrics

	err = json.Unmarshal(body, &metrics)
	fatalOnErr(t, err)
	createdCt := metrics.Keyquarry.SnapshotsCreated
	switch {
	case createdCt == nil:
		t.Errorf("error getting snapshot created count")
	default:
		if *createdCt == 0 {
			t.Errorf("expected snapshot created count > 0, got %d", *createdCt)
		}
	}

	time.Sleep(1 * time.Second)
	srvCancel()
	// Wait for the command to finish, then verify it deleted the socket
	// file before returning
	<-execDone

	// Validate the end result of the config
	cfg := cliOpts.ServerOpts

	assertEqual(t, cfg.Snapshot.Interval, snapshotInterval)
	assertEqual(
		t,
		fmt.Sprintf("%d", cfg.MaxKeyLength),
		fmt.Sprintf("%d", newMaxKeySize),
	)
	assertEqual(t, cfg.MaxValueSize, newMaxValueSize)
	assertEqual(t, cfg.RevisionLimit, newRevisionLimit)
	assertEqual(t, cfg.MaxNumberOfKeys, newMaxKeys)
	assertEqual(t, cfg.SSLCertfile, "")
	assertEqual(t, cfg.SSLKeyfile, "")
	assertEqual(t, cfg.LogLevel, "INFO")
	assertEqual(t, cfg.LogJSON, false)
	assertEqual(t, cfg.MonitorAddress, monitorAddr.URL.String())

	u := &url.URL{Scheme: "unix", Host: addr.SocketFile}
	assertEqual(t, cfg.ListenAddress, u.String())

	fileInfo, err := os.Stat(addr.SocketFile)
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf(
			"socket file '%s' still exists (%d): %#v",
			addr.SocketFile,
			fileInfo.Size(),
			err,
		)
	}

	u = &url.URL{Scheme: "unix", Host: monitorAddr.SocketFile}
	assertEqual(t, cfg.MonitorAddress, u.String())

	fileInfo, err = os.Stat(monitorAddr.SocketFile)
	if err == nil || !os.IsNotExist(err) {
		t.Fatalf(
			"socket file '%s' still exists (%d): %#v",
			monitorAddr.SocketFile,
			fileInfo.Size(),
			err,
		)
	}
}

func TestListKeysCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	client := newClient(t, addr)

	cctx := clientCtx(t)
	_, err := client.Set(cctx, &pb.KeyValue{Key: "foo", Value: nil})
	failOnErr(t, err)

	_, err = client.Set(cctx, &pb.KeyValue{Key: "bar", Value: nil})
	failOnErr(t, err)

	_, err = client.Set(cctx, &pb.KeyValue{Key: "baz", Value: nil})
	failOnErr(t, err)

	rootCmd.SetArgs([]string{"client", "--verbose", "list"})

	data := captureOutput(
		t, func() {
			failOnErr(t, listCmd.Execute())
		},
	)

	keys := strings.Split(data, "\n")
	assertSliceContains(t, keys, "foo", "bar", "baz")
	assertEqual(t, len(keys), 3)

	failOnErr(t, listCmd.Flags().Set("limit", "2"))

	data = captureOutput(
		t, func() {
			failOnErr(t, listCmd.Execute())
		},
	)
	keys = strings.Split(data, "\n")
	assertEqual(t, len(keys), 2)

	failOnErr(t, listCmd.Flags().Set("pattern", "ba*"))
	data = captureOutput(
		t, func() {
			failOnErr(t, listCmd.Execute())
		},
	)
	keys = strings.Split(data, "\n")
	assertEqual(t, len(keys), 2)
	assertSliceContains(t, keys, "bar", "baz")
}

func TestSetReadonlyCmd(t *testing.T) {
	addr := socketAddr(t)
	cfg := server.NewConfig()
	cfg.PrivilegedClientID = privilegedClientID

	srv := newServer(t, cfg, addr)

	assertEqual(t, srv.Config().Readonly, false)
	rootCmd.SetArgs(
		[]string{
			"client",
			"--client-id",
			privilegedClientID,
			"readonly",
			"on",
		},
	)
	setReadonlyCmd.SetContext(ctx)

	data := captureOutput(
		t, func() {
			failOnErr(t, setReadonlyCmd.Execute())
		},
	)
	rv := pb.ReadOnlyResponse{Success: true}
	//goland:noinspection GoVetCopyLock
	expected, err := json.Marshal(rv)
	failOnErr(t, err)
	assertEqual(t, data, string(expected))
	assertEqual(t, srv.Config().Readonly, true)

	rootCmd.SetArgs([]string{"client", "readonly", "off"})
	setReadonlyCmd.SetContext(ctx)
	data = captureOutput(
		t, func() {
			fatalOnErr(t, setReadonlyCmd.Execute())
		},
	)
	rv = pb.ReadOnlyResponse{Success: true}
	//goland:noinspection GoVetCopyLock
	expected, err = json.Marshal(rv)
	fatalOnErr(t, err)
	assertEqual(t, data, string(expected))
	assertEqual(t, srv.Config().Readonly, false)
}

func TestLockCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	client := newClient(t, addr)
	rootCmd.SetArgs([]string{"client", "lock", "foo", "10s"})
	cctx := clientCtx(t)
	value := []byte("baz")
	_, err := client.Set(cctx, &pb.KeyValue{Key: "foo", Value: value})
	failOnErr(t, err)

	kvInfo, err := client.Inspect(cctx, &pb.InspectRequest{Key: "foo"})
	fatalOnErr(t, err)
	assertEqual(t, *kvInfo.Locked, false)

	f := func() {
		failOnErr(t, lockCmd.Execute())
	}
	data := captureOutput(t, f)

	var lockResponse pb.LockResponse
	err = json.Unmarshal([]byte(data), &lockResponse)
	failOnErr(t, err)
	assertEqual(t, lockResponse.Success, true)

}

func TestLockCmdCreateIfMissing(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	rootCmd.SetArgs(
		[]string{
			"client",
			"lock",
			"somerandomkey",
			"10s",
			"--create-if-missing",
		},
	)

	f := func() {
		failOnErr(t, lockCmd.Execute())
	}
	data := captureOutput(t, f)

	var lockResponse pb.LockResponse
	err := json.Unmarshal([]byte(data), &lockResponse)
	failOnErr(t, err)
	assertEqual(t, lockResponse.Success, true)

}

func TestUnlockCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)

	client := newClient(t, addr)
	cctx := clientCtx(t)
	value := []byte("baz")
	kvSet, err := client.Set(
		cctx,
		&pb.KeyValue{
			Key:          "foo",
			Value:        value,
			LockDuration: durationpb.New(1 * time.Hour),
		},
	)
	fatalOnErr(t, err)
	assertEqual(t, kvSet.Success, true)

	rootCmd.SetArgs(
		[]string{
			"client",
			"unlock",
			"foo",
			"--client-id",
			defaultClientID,
		},
	)

	kvInfo, err := client.Inspect(cctx, &pb.InspectRequest{Key: "foo"})
	fatalOnErr(t, err)
	assertEqual(t, *kvInfo.Locked, true)

	f := func() {
		fatalOnErr(t, unlockCmd.Execute())
	}
	data := captureOutput(t, f)

	lockResponse := pb.UnlockResponse{Success: true}
	//goland:noinspection GoVetCopyLock
	expected, err := json.Marshal(lockResponse)
	assertEqual(t, data, string(expected))
}

func TestDeleteCmd(t *testing.T) {

	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	client := newClient(t, addr)
	rootCmd.SetArgs([]string{"client", "delete", "foo"})
	value := []byte("baz")
	_, err := client.Set(
		clientCtx(t),
		&pb.KeyValue{
			Key:   "foo",
			Value: value,
		},
	)
	failOnErr(t, err)

	f := func() {
		failOnErr(t, deleteCmd.Execute())
	}
	data := captureOutput(t, f)

	deleteResponse := pb.DeleteResponse{Deleted: true}
	//goland:noinspection GoVetCopyLock
	expected, err := json.Marshal(deleteResponse)

	assertEqual(t, data, string(expected))
}

func TestGetKeyMetricsCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	client := newClient(t, addr)
	keyMetricsCmd.SetContext(ctx)
	key := "foo"
	rootCmd.SetArgs([]string{"client", "key-metric", key})
	value := []byte("baz")
	cctx := clientCtx(t)
	_, err := client.Set(
		cctx,
		&pb.KeyValue{
			Key:          key,
			Value:        value,
			LockDuration: durationpb.New(1 * time.Hour),
		},
	)
	fatalOnErr(t, err)

	_, err = client.Get(
		cctx,
		&pb.Key{
			Key: key,
		},
	)
	fatalOnErr(t, err)

	f := func() {
		failOnErr(t, keyMetricsCmd.Execute())
	}
	data := captureOutput(t, f)

	var km pb.KeyMetric
	err = json.Unmarshal([]byte(data), &km)
	fatalOnErr(t, err)

	assertEqual(t, km.AccessCount, 1)
	if km.FirstAccessed == nil {
		t.Errorf("expected FirstAccessed to be set")
	}
	assertEqual(t, km.FirstAccessed.AsTime(), km.LastAccessed.AsTime())

	assertEqual(t, km.SetCount, 1)
	if km.FirstSet == nil {
		t.Errorf("expected FirstSet to be set")
	}
	assertEqual(t, km.FirstSet.AsTime(), km.LastSet.AsTime())

	assertEqual(t, km.LockCount, 1)
	assertEqual(t, km.FirstLocked.AsTime().IsZero(), false)
	assertEqual(t, km.LastLocked.AsTime(), km.FirstLocked.AsTime())
}

func TestPopKeyCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	client := newClient(t, addr)

	value := []byte("baz")
	cctx := clientCtx(t)
	_, err := client.Set(cctx, &pb.KeyValue{Key: "foo", Value: value})
	fatalOnErr(t, err)

	popCmd.SetContext(ctx)
	rootCmd.SetArgs([]string{"client", "pop", "foo"})

	f := func() {
		failOnErr(t, popCmd.Execute())
	}
	data := captureOutput(t, f)

	assertEqual(t, data, string(value))

	_, err = client.Inspect(cctx, &pb.InspectRequest{Key: "foo"})

	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	if status.Code(err) != codes.NotFound {
		t.Fatalf("expected NotFound, got: %+v", err)
	}
}

func TestInspectKeyCmd(t *testing.T) {
	addr := socketAddr(t)
	_ = newServer(t, nil, addr)
	client := newClient(t, addr)
	infoCmd.SetContext(ctx)
	rootCmd.SetArgs([]string{"client", "inspect", "foo"})
	value := []byte("baz")
	cctx := clientCtx(t)
	_, err := client.Set(
		cctx,
		&pb.KeyValue{
			Key:          "foo",
			Value:        value,
			LockDuration: durationpb.New(1 * time.Hour),
		},
	)
	failOnErr(t, err)

	f := func() {
		failOnErr(t, infoCmd.Execute())
	}
	data := captureOutput(t, f)

	kvInfo, err := client.Inspect(cctx, &pb.InspectRequest{Key: "foo"})
	failOnErr(t, err)
	expected, err := json.Marshal(kvInfo)
	failOnErr(t, err)

	assertEqual(t, data, string(expected))
}

// failOnErr is a helper function that takes the result of a function that
// only has 1 return value (error), and fails the test if the error is not nil.
// It's intended to reduce boilerplate code in tests.
func failOnErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("[%s] error: %s", t.Name(), err.Error())
	}
}

func assertSliceContains[V comparable](t *testing.T, value []V, expected ...V) {
	t.Helper()
	found := map[V]bool{}
	if len(expected) == 0 {
		t.Fatalf("expected slice must not be empty")
	}
	for _, v := range value {
		for _, exp := range expected {
			if v == exp {
				found[exp] = true
				break
			}
		}
	}
	if len(found) != len(expected) {
		t.Errorf(
			"expected:\n%#v\n\nnot found in:\n%#v", expected, value,
		)
	}
}

func assertEqual[V comparable](t *testing.T, val V, expected V) {
	t.Helper()
	if val != expected {
		t.Errorf(
			"expected:\n%#v\n\ngot:\n%#v",
			expected,
			val,
		)
	}
}

func fatalOnErr(t *testing.T, err error, msg ...string) {
	t.Helper()
	if err == nil {
		return
	}

	if len(msg) > 0 {
		t.Fatalf(
			"expected no error, got: %s (%s)",
			err.Error(),
			strings.Join(msg, " / "),
		)
	}

	t.Fatalf("expected no error, got: %s", err.Error())
}
