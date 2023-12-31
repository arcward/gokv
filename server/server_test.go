package server

import (
	"compress/gzip"
	"context"
	"crypto"
	"encoding/json"
	"errors"
	"fmt"
	kclient "github.com/arcward/keyquarry/client"
	"golang.org/x/exp/rand"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/arcward/keyquarry/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
)

const bufSize = 1024 * 1024

// var lis *bufconn.Listener
// var srv *KeyValueStore
// var s *grpc.Server
var signals = make(chan os.Signal, 1)
var ctx context.Context
var cancel context.CancelFunc
var defaultClientID = "foo"
var testTimeout time.Duration = 60 * time.Second
var dialTimeout = 10 * time.Second

func newServer(t *testing.T, lis *bufconn.Listener, cfg *Config) (
	*KeyValueStore,
	*bufconn.Listener,
) {
	t.Helper()

	var srv *KeyValueStore
	var s *grpc.Server

	log.SetOutput(io.Discard)
	fmt.Println("starting stuff")
	if lis == nil {
		lis = bufconn.Listen(bufSize)
	}

	td := os.Getenv("TEST_TIMEOUT")
	if td != "" {
		var err error
		testTimeout, err = time.ParseDuration(td)
		if err != nil {
			panic(fmt.Sprintf("failed to parse TEST_TIMEOUT: %s", err.Error()))
		}
	}

	var err error
	if cfg == nil {
		cfg = DefaultConfig()
		cfg.HashAlgorithm = crypto.MD5
		cfg.RevisionLimit = 2
		cfg.MinLifespan = time.Duration(1) * time.Second
		cfg.MinLockDuration = time.Duration(1) * time.Second
		cfg.EagerPrune = false
		cfg.PruneInterval = 0
		cfg.LogLevel = "ERROR"
	}
	handler := slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{AddSource: true, Level: slog.LevelError},
	)
	logger := slog.New(handler).WithGroup(t.Name())
	slog.SetDefault(logger)
	cfg.Logger = logger

	srv, err = NewServer(cfg)
	if err != nil {
		panic(err)
	}

	if s == nil {
		s = grpc.NewServer(
			grpc.UnaryInterceptor(ClientIDInterceptor(srv)),
			grpc.KeepaliveEnforcementPolicy(
				keepalive.EnforcementPolicy{
					MinTime:             5 * time.Second,
					PermitWithoutStream: true,
				},
			),
		)
	}

	pb.RegisterKeyValueStoreServer(s, srv)

	//go func() {
	//	select {
	//	case <-signals:
	//		//t.Fatalf("%s: interrupted", t.Name())
	//		panic(fmt.Sprintf("%s: interrupted", t.Name()))
	//	case <-ctx.Done():
	//		if e := ctx.Err(); errors.Is(e, context.DeadlineExceeded) {
	//			//t.Fatalf("%s: timeout exceeded", t.Name())
	//			panic(fmt.Sprintf("%s: timeout exceeded", t.Name()))
	//		}
	//		//case <-ctx.Done():
	//		//	panic("done called")
	//	}
	//}()
	tctx, tcancel := context.WithTimeout(ctx, testTimeout)

	go func() {
		select {
		case <-tctx.Done():
			if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
				t.Fatalf("%s: timeout exceeded", t.Name())
			}
		}
	}()
	srvDone := make(chan struct{})
	go func() {
		defer func() {
			srvDone <- struct{}{}
		}()
		if e := srv.Start(tctx); e != nil {
			panic(e)
		}
	}()
	go func() {
		if se := s.Serve(lis); se != nil {
			panic(se)
		}
	}()

	t.Cleanup(
		func() {
			t.Logf("stopping server listener")
			s.GracefulStop()
			//lis.Close()
			tcancel()
			t.Logf("cancelled srv context")
			<-srvDone
			t.Logf("cleaning up server")
			s.Stop()
			t.Logf("done")

		},
	)
	return srv, lis
}

func init() {
	log.SetOutput(io.Discard)
	fmt.Println("starting stuff")
	//lis = bufconn.Listen(bufSize)

	//td := os.Getenv("TEST_TIMEOUT")
	//if td != "" {
	//	var err error
	//	testTimeout, err = time.ParseDuration(td)
	//	if err != nil {
	//		panic(fmt.Sprintf("failed to parse TEST_TIMEOUT: %s", err.Error()))
	//	}
	//}

	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		select {
		case <-signals:
			cancel()
			panic("interrupted")
		}
	}()
	//if srv == nil {
	//	var err error
	//	vcfg := DefaultConfig()
	//	vcfg.HashAlgorithm = crypto.MD5
	//	vcfg.RevisionLimit = 2
	//	vcfg.MinLifespan = time.Duration(1) * time.Second
	//	vcfg.MinLockDuration = time.Duration(1) * time.Second
	//	vcfg.EagerPrune = false
	//	vcfg.PruneInterval = 0
	//	srv, err = NewServer(vcfg)
	//	if err != nil {
	//		panic(err)
	//	}
	//}
	//
	//if s == nil {
	//	s = grpc.NewServer(grpc.UnaryInterceptor(ClientIDInterceptor(srv)))
	//}
	//
	//pb.RegisterKeyValueStoreServer(s, srv)
	//
	//
	//go func() {
	//	if e := srv.Start(ctx); e != nil {
	//		panic(e)
	//	}
	//}()
	//go func() {
	//	if err := s.Serve(lis); err != nil {
	//		panic(err)
	//	}
	//}()
}

//
//func newBClient(b *testing.B, srv *KeyValueStore) *kclient.Client {
//	b.Helper()
//
//	//b.Cleanup(
//	//	func() {
//	//		srv.mu.Lock()
//	//		defer srv.mu.Unlock()
//	//		srv.clear()
//	//	},
//	//)
//
//	client := kclient.NewClient(
//		kclient.Config{
//			NoTLS:   true,
//			Address: "bufnet",
//		}, grpc.WithContextDialer(bufDialer),
//	)
//	err := client.Dial(true)
//	if err != nil {
//		b.Fatalf("Failed to dial bufnet: %v", err)
//	}
//
//	//client := pb.NewKeyValueStoreClient(conn)
//
//	go func() {
//		select {
//		case <-signals:
//			panic("interrupted")
//		case <-ctx.Done():
//			panic("done called")
//		}
//	}()
//	return client
//}

func newBadClient(
	t *testing.T,
	lis *bufconn.Listener,
	clientID *string,
) pb.KeyValueStoreClient {
	t.Helper()

	tctx, tcancel := context.WithTimeout(ctx, 30*time.Second)

	dialOpts := []grpc.DialOption{
		grpc.WithContextDialer(
			func(
				context.Context,
				string,
			) (net.Conn, error) {
				return lis.Dial()
			},
		),
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if clientID != nil {
		dialOpts = append(
			dialOpts,
			grpc.WithUnaryInterceptor(kclient.ClientIDInterceptor(*clientID)),
		)
	}

	clientConn, err := grpc.DialContext(
		tctx,
		"bufnet",
		dialOpts...,
	)
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}
	t.Cleanup(
		func() {
			clientConn.Close()
			tcancel()
			//srv.mu.Lock()
			//defer srv.mu.Unlock()
			//srv.clear()
		},
	)

	nc := pb.NewKeyValueStoreClient(clientConn)

	//go func() {
	//	select {
	//	case <-signals:
	//		//t.Fatalf("%s: interrupted", t.Name())
	//		panic(fmt.Sprintf("%s: interrupted", t.Name()))
	//	case <-tctx.Done():
	//		if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
	//			//t.Fatalf("%s: timeout exceeded", t.Name())
	//			panic(fmt.Sprintf("%s: timeout exceeded", t.Name()))
	//		}
	//	case <-ctx.Done():
	//		panic("done called")
	//	}
	//}()
	return nc
}

func newClient(
	t *testing.T,
	srv *KeyValueStore,
	lis *bufconn.Listener,
	clientID string,
) *kclient.Client {
	t.Helper()
	if lis == nil {
		t.Fatalf("lis is nil")
	}

	tctx, tcancel := context.WithTimeout(ctx, testTimeout)

	go func() {
		select {
		case <-tctx.Done():
			if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
				t.Fatalf("%s: timeout exceeded", t.Name())
			}
		}
	}()

	t.Cleanup(
		func() {
			tcancel()

		},
	)
	if clientID == "" {
		clientID = t.Name()
	}
	clientCfg := kclient.DefaultConfig()
	handler := slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{AddSource: true, Level: slog.LevelError},
	)
	logger := slog.New(handler).WithGroup(fmt.Sprintf("%s-client", t.Name()))
	clientCfg.Logger = logger
	clientCfg.Address = "bufnet"
	clientCfg.NoTLS = true
	clientCfg.ClientID = clientID
	if clientCfg.ClientID == "" {
		clientCfg.ClientID = defaultClientID
	}
	client := kclient.NewClient(
		&clientCfg,
		grpc.WithBlock(),
		grpc.WithContextDialer(
			func(
				context.Context,
				string,
			) (net.Conn, error) {
				return lis.Dial()
			},
		),
	)
	connCtx, connCancel := context.WithTimeout(tctx, dialTimeout)
	err := client.Dial(
		connCtx,
		true,
	)
	connCancel()
	if err != nil {
		if status.Code(err) != codes.AlreadyExists {
			t.Fatalf("Failed to dial bufnet: %v", err)
		}
	}

	go func() {
		select {
		case <-signals:
			panic(fmt.Sprintf("%s: interrupted", t.Name()))
		case <-tctx.Done():
			if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
				t.Fatalf("%s: timeout exceeded", t.Name())
			}
		}
	}()
	return client
}

func benchmarkFailOnErr(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatalf("error: %s", err.Error())
	}
}

//
//func BenchmarkKeyValueStore(b *testing.B) {
//	b.Cleanup(
//		func() {
//			srv.clear()
//			srv.resetStats()
//		},
//	)
//
//	conn, err := grpc.DialContext(
//		ctx,
//		"bufnet",
//		grpc.WithContextDialer(bufDialer),
//		grpc.WithInsecure(),
//	)
//	if err != nil {
//		b.Fatalf("Failed to dial bufnet: %v", err)
//	}
//
//	client := pb.NewKeyValueStoreClient(conn)
//
//	numKeys := 1000
//	keys := make([]string, numKeys, numKeys)
//
//	d := time.Duration(9) * time.Second
//	for i := 0; i < numKeys; i++ {
//		kv := &pb.KeyValue{
//			Key:      fmt.Sprintf("Key-%d", i),
//			Value:    []byte(fmt.Sprintf("value-%d", i)),
//			Lifespan: durationpb.New(d),
//		}
//		keys[i] = kv.Key
//		_, _ = client.Set(
//			ctx, kv,
//		)
//	}
//
//	if srv.numKeys.Load() != uint64(numKeys) {
//		b.Fatalf("expected %d keys, got %d", numKeys, srv.numKeys.Load())
//	}
//
//	time.Sleep(8 * time.Second)
//	b.ResetTimer()
//
//	for i := 0; i < b.N; i++ {
//		key := fmt.Sprintf("Key-%d", i)
//		value := []byte(fmt.Sprintf("value-%d", i))
//		_, err := client.Set(
//			ctx, &pb.KeyValue{
//				Key:   key,
//				Value: value,
//			},
//		)
//		benchmarkFailOnErr(b, err)
//
//	}
//
//}

func TestListKeys(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	firstKey := "SomeKeyHere"
	secondKey := "AnotherKeyHere"
	kx := &pb.KeyValue{Key: firstKey, Value: []byte("foo")}
	ky := &pb.KeyValue{Key: secondKey, Value: []byte("bar")}

	// Set a Key-value pair
	_, err := client.Set(ctx, kx)
	failOnErr(t, err)
	_, err = client.Set(ctx, ky)
	failOnErr(t, err)

	keys, err := client.ListKeys(ctx, &pb.ListKeysRequest{})
	failOnErr(t, err)
	assertSliceContains(t, keys.Keys, firstKey, secondKey)
	assertEqual(t, len(keys.Keys), 2, strings.Join(keys.Keys, ", "))

	keys, err = client.ListKeys(ctx, &pb.ListKeysRequest{Limit: 1})
	assertSliceContainsOneOf(t, keys.Keys, firstKey, secondKey)
	assertEqual(t, len(keys.Keys), 1)

	firstKeyPattern := "Some*"
	keys, err = client.ListKeys(
		ctx,
		&pb.ListKeysRequest{Pattern: firstKeyPattern},
	)
	failOnErr(t, err)
	assertSliceContains(t, keys.Keys, firstKey)
	assertEqual(t, len(keys.Keys), 1)

	secondKeyPattern := ".+other.+"
	keys, err = client.ListKeys(
		ctx,
		&pb.ListKeysRequest{Pattern: secondKeyPattern},
	)
	failOnErr(t, err)
	assertSliceContains(t, keys.Keys, secondKey)
	assertEqual(t, len(keys.Keys), 1)

	keys, err = client.ListKeys(ctx, &pb.ListKeysRequest{Pattern: ".+"})
	failOnErr(t, err)
	assertSliceContains(t, keys.Keys, firstKey, secondKey)
	assertEqual(t, len(keys.Keys), 2)

	keys, err = client.ListKeys(
		ctx,
		&pb.ListKeysRequest{Pattern: ".+", Limit: 1},
	)
	failOnErr(t, err)
	assertSliceContainsOneOf(t, keys.Keys, firstKey, secondKey)
	assertEqual(t, len(keys.Keys), 1)

	keys, err = client.ListKeys(
		ctx,
		&pb.ListKeysRequest{Pattern: ".+wat.+"},
	)
	failOnErr(t, err)
	assertEqual(t, len(keys.Keys), 0)
}

func TestRunServer(t *testing.T) {
	tctx, tcancel := context.WithTimeout(ctx, testTimeout)
	t.Cleanup(
		func() {
			tcancel()
		},
	)

	cfg := DefaultConfig()
	cfg.HashAlgorithm = crypto.MD5
	cfg.RevisionLimit = 2
	cfg.MinLifespan = time.Duration(1) * time.Second
	cfg.MinLockDuration = time.Duration(1) * time.Second
	cfg.EagerPrune = false
	cfg.PruneInterval = 0
	cfg.LogLevel = "ERROR"

	handler := slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{AddSource: true, Level: slog.LevelError},
	)
	logger := slog.New(handler).WithGroup(t.Name())
	slog.SetDefault(logger)
	cfg.Logger = logger

	srv, err := NewServer(cfg)
	if err != nil {
		panic(err)
	}
	lis := bufconn.Listen(bufSize)

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(ClientIDInterceptor(srv)),
		grpc.KeepaliveEnforcementPolicy(
			keepalive.EnforcementPolicy{
				MinTime:             5 * time.Second,
				PermitWithoutStream: true,
			},
		),
	)

	stopSignal := make(chan struct{}, 1)
	go func() {
		RunServer(tctx, grpcServer, lis, srv)
		stopSignal <- struct{}{}
	}()

	client := newClient(t, srv, lis, "")
	_, err = client.Set(
		ctx,
		&pb.KeyValue{
			Key:          "foo",
			Value:        []byte("bar"),
			LockDuration: durationpb.New(15 * time.Second),
		},
	)
	fatalOnErr(t, err)
	tcancel()
	select {
	case <-stopSignal:
		t.Logf("stopped!")
	case <-time.After(10 * time.Second):
		t.Fatalf("failed to stop")
	}

}

func TestUpdateLockedKeyToken(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	kv := &pb.KeyValue{
		Key:          "foo",
		LockDuration: durationpb.New(10 * time.Second),
	}
	rv, err := client.Set(ctx, kv)
	failOnErr(t, err)

	assertEqual(t, rv.Success, true)

	t.Logf("attempting to set with other client '%s'", "randomclientid")
	otherClient := newClient(t, srv, lis, "randomclientid")
	t.Logf("connected with othe client")
	_, err = otherClient.Set(
		ctx,
		&pb.KeyValue{Key: "foo", Value: []byte("bar")},
	)
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	t.Logf("setting with original client")
	_, err = client.Set(
		ctx,
		&pb.KeyValue{
			Key:   "foo",
			Value: []byte("bar"),
		},
	)
	failOnErr(t, err)

	vr, err := client.Get(ctx, &pb.Key{Key: "foo"})
	failOnErr(t, err)
	assertEqual(t, string(vr.Value), "bar")
}

func TestGetExpiredKey(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	var seconds uint32 = 1
	expireAfter := time.Duration(seconds) * time.Second

	kv := &pb.KeyValue{
		Key:      "foo",
		Value:    []byte("bar"),
		Lifespan: durationpb.New(expireAfter),
	}
	rv, err := client.Set(ctx, kv)
	failOnErr(t, err)
	assertEqual(t, rv.Success, true)
	assertEqual(t, rv.IsNew, true)

	time.Sleep(expireAfter + 1*time.Second)

	_, err = client.Get(ctx, &pb.Key{Key: kv.Key})
	assertNotNil(t, err)

	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	fmt.Println(e.Message())
	assertErrorCode(t, e.Code(), codes.NotFound)

}

func TestMaxKeyLength(t *testing.T) {
	// validate that we can't create a key greater than the max
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	keychars := make([]byte, 0, DefaultMaxKeySize*2)
	for i := 0; i < int(DefaultMaxKeySize*2); i++ {
		keychars = append(keychars, 'a')
	}
	key := string(keychars)

	var value []byte
	kx := &pb.KeyValue{Key: key, Value: value}

	_, err := client.Set(ctx, kx)
	assertNotNil(t, err)
	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	assertErrorCode(t, e.Code(), codes.FailedPrecondition)
	assertEqual(t, e.Message(), ErrKeyTooLong.Message)

}

func TestKeyNotFound(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	_, err := client.GetKeyInfo(ctx, &pb.Key{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestEagerPrune(t *testing.T) {
	newCfg := DefaultConfig()
	newCfg.EagerPrune = true
	newCfg.LogLevel = "ERROR"
	newCfg.MaxNumberOfKeys = 50
	newCfg.PruneThreshold = 0.5
	newCfg.MinLifespan = time.Duration(1) * time.Second
	newCfg.MinLockDuration = time.Duration(1) * time.Second
	newCfg.PruneInterval = 0
	newCfg.EventStreamSendTimeout = 5 * time.Second

	srv, lis := newServer(t, nil, newCfg)

	client := newClient(t, srv, lis, "")

	//startingKeys := srv.numKeys.Load()
	resKeys, err := client.ListKeys(
		ctx,
		&pb.ListKeysRequest{IncludeReserved: true},
	)
	if err != nil {
		t.Fatalf("failed to list keys: %s", err.Error())
	}
	startingKeys := uint64(len(resKeys.Keys))
	srv.cfgMu.RLock()
	maxKeys := srv.cfg.MaxNumberOfKeys
	//thresholdLimit := uint64(float64(maxKeys) * newCfg.PruneThreshold)

	time.Sleep(5 * time.Second)

	capacityRemaining := maxKeys - startingKeys

	makeNumKeys := capacityRemaining * 2

	keys := make([]string, 0, makeNumKeys)
	for i := 0; i < int(makeNumKeys); i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i+1))
	}
	srv.cfgMu.RUnlock()
	//expectedKeys := keys[capacityRemaining:]
	watcher, err := srv.Subscribe("foo")
	fatalOnErr(t, err)

	expungeSeen := map[string]bool{}
	unexpectedExpunge := []string{}
	for _, k := range keys[:int(capacityRemaining)] {
		expungeSeen[k] = false
	}
	t.Logf(
		"capacity remaining: %d / making: %d / expected expunge ct: %d / starting keys: %d",
		capacityRemaining,
		makeNumKeys,
		len(expungeSeen),
		startingKeys,
	)
	expectedEvents := maxKeys - startingKeys
	expungeCt := 0
	allTrueSignal := make(chan struct{}, 1)
	go func() {
		var done bool
		defer func() {
			t.Logf("subscriber exited")
		}()
		for event := range watcher {
			if ctx.Err() != nil {
				break
			}

			t.Logf("saw event: %+v", event)
			if done {
				continue
			}
			if event.Event == Expunged {
				expungeCt++
				t.Logf("seen %d/%d", expungeCt, expectedEvents)
				_, ok := expungeSeen[event.Key]
				if ok {
					expungeSeen[event.Key] = true
				} else {
					unexpectedExpunge = append(unexpectedExpunge, event.Key)
				}
				allTrue := true
				for exp, v := range expungeSeen {
					if !v {
						allTrue = false
						t.Logf("waiting on: %s", exp)
						break
					}
				}
				if allTrue {
					done = true
					allTrueSignal <- struct{}{}
					t.Logf("sent done signal")
					t.Logf("unsubscribing")
					unsubErr := srv.Unsubscribe("foo")
					if unsubErr != nil {
						t.Fatalf("failed to unsubscribe: %s", unsubErr.Error())
					}
					return
				}
			} else {
				t.Logf("watcher saw non-expunge event %v", event)
			}

		}
	}()

	t.Logf("keys to create: %d", len(keys))
	for ind, k := range keys {
		t.Logf("checking key %s", k)
		currentCt := srv.numKeys.Load() // - srv.numReservedKeys.Load()
		t.Logf("setting %s", k)
		kset, e := client.Set(ctx, &pb.KeyValue{Key: k, Value: []byte("bar")})
		fatalOnErr(t, e, fmt.Sprintf("failed on ind %d, key %s", ind, k))
		t.Logf("set: %v", kset)
		afterCt := srv.numKeys.Load()

		//if currentCt > maxKeys {
		//	t.Fatalf(
		//		"currentCt: %d, max: %d, ind: %d, key: %s",
		//		currentCt,
		//		srv.cfg.MaxNumberOfKeys,
		//		ind,
		//		k,
		//	)
		//	assertEqual(t, currentCt, thresholdLimit)
		//	expectedKeys = append(expectedKeys)
		//	assertErrorCode(
		//		t,
		//		status.Code(e),
		//		codes.ResourceExhausted,
		//		fmt.Sprintf("#%d, %s (%d)", ind, k, currentCt),
		//	)
		if currentCt > maxKeys {
			t.Fatalf(
				"exceeded max keys (max: %d current: %d) at %d (%s)",
				maxKeys,
				currentCt,
				ind,
				k,
			)
		} else if currentCt == srv.cfg.MaxNumberOfKeys {
			assertEqual(t, currentCt, afterCt)
		} else {
			assertEqual(
				t,
				afterCt,
				uint64(ind+1)+startingKeys,
				fmt.Sprintf(
					"ind %d, ct %d, started %d",
					ind,
					currentCt,
					startingKeys,
				),
			)
			assertEqual(t, err, nil)
		}
	}
	finalCt := srv.numKeys.Load()
	assertEqual(t, finalCt, maxKeys)
	time.Sleep(time.Second)
	t.Logf("waiting on done signal")
	select {
	case <-allTrueSignal:
		//
	case <-time.After(60 * time.Second):
		sort.Strings(keys)
		for _, k := range keys {
			t.Logf("%s expunged: %t", k, expungeSeen[k])
		}
		t.Fatalf(
			"timed out waiting for allTrueSignal. current: %+v",
			expungeSeen,
		)
	}
	t.Logf("checking stuff")
	assertEqual(t, expungeCt, int(expectedEvents))
	assertEqual(t, len(unexpectedExpunge), 0)
	assertEqual(t, len(expungeSeen), int(maxKeys-startingKeys))
	for k, v := range expungeSeen {
		assertEqual(t, v, true, fmt.Sprintf("key: %s", k))
	}
	t.Logf("should be finished by now...")

}

func fatalOnErr(t *testing.T, err error, msg ...string) {
	t.Helper()
	if err != nil {
		t.Fatalf("error: %s / %s", err.Error(), strings.Join(msg, " ... "))
	}
}

func TestKeySort(t *testing.T) {

	fooCreated := time.Now()

	barCreated := fooCreated.Add(time.Duration(-12) * time.Hour)
	watUpdated := barCreated.Add(time.Duration(1) * time.Hour)
	expectedOrder := []string{"bar", "wat", "foo", "baz"}

	keys := []keyInfoWithLock{
		{
			kv: &KeyValueInfo{
				Key:     "foo",
				Created: fooCreated, // created now
			},
		},
		{
			kv: &KeyValueInfo{
				Key:     "bar",
				Created: barCreated, // created 12 hours ago
			},
		},
		{
			kv: &KeyValueInfo{
				Key:     "wat",
				Created: barCreated, // created 12 hours ago
				Updated: watUpdated, // updated 11 hours ago
			},
		},
		{
			kv: &KeyValueInfo{
				Key:     "baz",
				Created: fooCreated, // created now
			},
			lock: &KeyLock{
				Created: fooCreated.Add(time.Duration(1) * time.Hour), // locked an hour from now
			},
		},
	}

	sorted := sortKeyValueInfoByDates(keys)
	assertEqual(t, len(sorted), len(keys))
	for ind, k := range sorted {
		assertEqual(t, k.kv.Key, expectedOrder[ind])
	}

}

func TestKeyHistory(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	originalHistoryConfig := srv.cfg.RevisionLimit
	t.Cleanup(
		func() {
			srv.cfg.RevisionLimit = originalHistoryConfig
		},
	)
	var newRevisionLimit int64 = 3

	//srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	srv.cfgMu.Lock()
	srv.cfg.RevisionLimit = newRevisionLimit
	srv.cfgMu.Unlock()

	k := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	originalValue := k.Value
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	keyInfo, ok := srv.store[k.Key]
	assertEqual(t, ok, true)
	assertNotNil(t, keyInfo.History)
	// the value should be on the store only, with no history
	assertEqual(t, keyInfo.History.Length(), 0)

	versions := map[int][]byte{}
	versions[0] = originalValue
	// create a few revisions, with a value including the expected version
	for i := 0; i < int(newRevisionLimit); i++ {
		newValue := []byte(fmt.Sprintf("%s-%d", string(originalValue), i+1))
		k.Value = newValue
		_, err = client.Set(ctx, k)
		failOnErr(t, err)
	}
	assertEqual(t, keyInfo.History.Length(), int(newRevisionLimit))
	versionsSeen := map[uint64]bool{}
	for _, v := range keyInfo.History.List() {
		_, seen := versionsSeen[v.Version]
		if seen {
			t.Fatalf("found duplicate version %d", v.Version)
		}
		versionsSeen[v.Version] = true
	}

	firstRevision, found := keyInfo.History.Version(1)
	if !found {
		t.Fatal("expected to find version 1")
	}
	assertNotNil(t, firstRevision)
	assertNotNil(t, firstRevision.Version)
	assertEqual(t, firstRevision.Version, 1)
	assertSlicesEqual(t, firstRevision.Value, originalValue)

	var previousRevision *KeyValueSnapshot

	for ind, rev := range keyInfo.History.List() {
		rev := rev
		assertEqual(t, rev.Version, uint64(ind+1))

		assertEqual(t, rev.Timestamp.IsZero(), false)
		if previousRevision != nil {
			assertEqual(t, rev.Version-1, previousRevision.Version)
			expectedVal := fmt.Sprintf("%s-%d", string(originalValue), ind)
			currentVal := string(rev.Value)
			assertEqual(t, currentVal, expectedVal)
		}
		previousRevision = rev
	}
	currentVal, err := client.Get(ctx, &pb.Key{Key: k.Key})
	failOnErr(t, err)

	k.Value = []byte("final")
	_, err = client.Set(ctx, k)
	failOnErr(t, err)

	assertEqual(t, keyInfo.History.Length(), int(newRevisionLimit))
	hist := keyInfo.History.List()
	for i := 0; i < len(hist); i++ {
		assertNotEqual(t, hist[i], firstRevision)
	}
	assertSlicesEqual(
		t,
		hist[len(hist)-1].Value,
		currentVal.Value,
	)

	_, err = client.ClearHistory(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, keyInfo.History.Length(), 0)
}

func TestLockUnknownKey(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	_, err := client.Lock(ctx, &pb.LockRequest{Key: "foo"})
	assertErrorCode(t, status.Code(err), codes.OutOfRange)
	_, err = client.Lock(
		ctx,
		&pb.LockRequest{Key: "foo", Duration: durationpb.New(10 * time.Second)},
	)
	assertErrorCode(t, status.Code(err), codes.NotFound)
}

func TestUnlockUnknownKey(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	_, err := client.Unlock(ctx, &pb.UnlockRequest{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestUpdateWithLockDuraton(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	k := &pb.KeyValue{Key: "foo"}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	k.LockDuration = durationpb.New(10 * time.Second)

	_, err = client.Set(ctx, k)
	failOnErr(t, err)

	kv, err := client.GetKeyInfo(ctx, &pb.Key{Key: k.Key})
	failOnErr(t, err)
	assertEqual(t, kv.Locked, true)

	_, ok := srv.store[k.Key]
	assertEqual(t, ok, true)

	keyLock := srv.locks[k.Key]
	if keyLock == nil {
		t.Fatal("expected Key lock")
	}
	triggered := keyLock.t.Stop()
	assertEqual(t, triggered, true)

}

func TestKeyAlreadyLocked(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	k := &pb.KeyValue{
		Key:          "foo",
		LockDuration: durationpb.New(10 * time.Second),
	}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	otherClient := newClient(t, srv, lis, "someotherclientid")

	_, err = otherClient.Lock(
		ctx,
		&pb.LockRequest{Key: "foo", Duration: durationpb.New(10 * time.Second)},
	)
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.PermissionDenied)
}

func TestKeyLockCreateIfMissing(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	kv := &pb.LockRequest{
		Key:             "aeiou",
		Duration:        durationpb.New(1 * time.Hour),
		CreateIfMissing: false,
	}

	_, err := client.Lock(ctx, kv)
	assertNotNil(t, err)
	assertErrorCode(t, status.Code(err), codes.NotFound)

	kv.CreateIfMissing = true

	rv, err := client.Lock(ctx, kv)
	failOnErr(t, err)
	assertEqual(t, rv.Success, true)

	kvInfo, err := client.GetKeyInfo(ctx, &pb.Key{Key: kv.Key})
	failOnErr(t, err)
	assertEqual(t, kvInfo.Locked, true)

	uv, err := client.Unlock(
		ctx,
		&pb.UnlockRequest{Key: kv.Key},
	)
	failOnErr(t, err)
	assertEqual(t, uv.Success, true)
}

func TestUpdateExpiration(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	key := "foo"
	value := []byte("bar")
	expireAfter := 30 * time.Minute

	// set a Key, initially unlocked
	kx := &pb.KeyValue{
		Key:      key,
		Value:    value,
		Lifespan: durationpb.New(expireAfter),
	}
	_, err := client.Set(ctx, kx)
	failOnErr(t, err)

	srv.mu.RLock()
	kvData, ok := srv.store[key]
	if !ok {
		srv.mu.RUnlock()
		t.Fatal("expected Key in store")
	}
	kvData.mu.RLock()
	srv.mu.RUnlock()

	assertEqual(t, kvData.expired, false)
	assertEqual(t, kvData.Lifespan.Seconds(), expireAfter.Seconds())
	kvData.mu.RUnlock()

	newExpiration := 1 * time.Hour
	kx.Lifespan = durationpb.New(newExpiration)
	_, err = client.Set(ctx, kx)
	failOnErr(t, err)

	kvData.mu.RLock()
	assertEqual(t, kvData.expired, false)
	assertEqual(t, kvData.Lifespan.Seconds(), newExpiration.Seconds())
	kvData.mu.RUnlock()
}

func TestKeyLock(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	key := "foo"
	value := []byte("bar")

	// set a Key, initially unlocked
	kx := &pb.KeyValue{Key: key, Value: value}
	_, err := client.Set(ctx, kx)
	failOnErr(t, err)

	pk := &pb.Key{Key: key}
	kvInfo, err := client.GetKeyInfo(ctx, pk)
	failOnErr(t, err)

	assertEqual(t, kvInfo.Locked, false)

	// lock it for 5 seconds
	now := time.Now()
	var lockDuration uint32 = 5
	d := time.Duration(lockDuration) * time.Second
	unlockedAt := now.Add(d)
	lockRequest := &pb.LockRequest{Key: pk.Key, Duration: durationpb.New(d)}
	rv, err := client.Lock(ctx, lockRequest)
	failOnErr(t, err)
	assertEqual(t, rv.Success, true)

	srv.mu.RLock()
	keyInfo := srv.store[key]
	keyInfo.mu.Lock()
	srv.mu.RUnlock()
	t.Logf("val: %#v", keyInfo)
	_, locked := srv.locks[key]
	assertEqual(t, locked, true)
	keyInfo.mu.Unlock()

	otherClient := newClient(t, srv, lis, "someotherclientid")
	// shouldn't be able to update a Locked Key
	_, err = otherClient.Set(ctx, kx)
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	// shouldn't be able to delete a Locked Key
	_, err = otherClient.Delete(ctx, &pb.DeleteRequest{Key: key})
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	// should be able to see it
	kvInfo, err = otherClient.GetKeyInfo(ctx, pk)
	failOnErr(t, err)
	assertEqual(t, kvInfo.Locked, true)

	srv.mu.RLock()
	keyInfo, ok := srv.store[key]
	keyLock, _ := srv.locks[key]
	srv.mu.RUnlock()

	assertEqual(t, ok, true)

	assertNotEqual(t, keyLock, nil)
	assertEqual(t, keyLock.Created.IsZero(), false)
	//assertEqual(t, keyInfo.Created.IsZero(), false)

	for n := time.Now(); n.Before(unlockedAt); n = time.Now() {
		time.Sleep(1 * time.Second)
	}

	kx.Value = []byte("baz")
	//kx.Lock = false
	kx.LockDuration = nil

	// should be unlocked, able to be updated
	// we update it with a lock that doesn't expire
	setResponse, err := client.Set(ctx, kx)
	failOnErr(t, err)
	assertEqual(t, setResponse.Success, true)
	assertEqual(t, setResponse.IsNew, false)
	keyInfo.mu.Lock()
	//assertEqual(t, keyInfo.Locked, false)
	keyInfo.mu.Unlock()
	// set the expiration for 1 second, then wait for it to expire
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: key})
	failOnErr(t, err)

	dur := time.Duration(10) * time.Second
	kx.LockDuration = durationpb.New(dur)
	//kx.Lock = true
	exp := time.Duration(1) * time.Second
	kx.Lifespan = durationpb.New(exp)
	kr, err := client.Set(ctx, kx)
	failOnErr(t, err)
	assertEqual(t, kr.Success, true)
	//unlockToken := kr.UnlockToken

	time.Sleep(3 * time.Second)
	kvInfo, err = client.GetKeyInfo(ctx, pk)
	failOnErr(t, err)

	newKeyInfo := srv.store[key]
	assertNotEqual(t, keyInfo, newKeyInfo)
	keyInfo = newKeyInfo
	// normally, the expiration is checked when getting a Key, and if it's
	// expired, it's deleted and we get nothing back. in this case, the lock
	// should prevent that from happening, and we should get the Key back
	assertEqual(t, kvInfo.Locked, true)
	assertEqual(t, keyInfo.expired, true)
	_, isLocked := srv.locks[key]
	assertEqual(t, isLocked, true)

	// a Locked Key should be immune from clearing the store
	clearResponse, err := client.Clear(ctx, &pb.ClearRequest{})
	failOnErr(t, err)
	assertEqual(t, clearResponse.Success, true)
	assertEqual(t, clearResponse.KeysDeleted, 0)

	// explicitly unlock it... without the right client ID
	unlockResponse, err := otherClient.Unlock(ctx, &pb.UnlockRequest{Key: key})
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	unlockResponse, err = client.Unlock(
		ctx,
		&pb.UnlockRequest{Key: key},
	)
	failOnErr(t, err)
	assertEqual(t, unlockResponse.Success, true)
	_, isLocked = srv.locks[key]
	assertEqual(t, isLocked, false)
	assertEqual(t, keyInfo.expired, true)

	kx.Lifespan = nil
	kx.LockDuration = nil
	_, err = client.Set(ctx, kx)
	failOnErr(t, err)
	// clearing the store should now delete the expired, unlocked Key
	clearResponse, err = client.Clear(ctx, &pb.ClearRequest{})
	failOnErr(t, err)
	assertEqual(t, clearResponse.Success, true)
	assertEqual(t, clearResponse.KeysDeleted, 1)

	// verify it was removed from the store
	_, ok = srv.store[key]
	assertEqual(t, ok, false)
}

func TestPopUnknownKey(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	_, err := client.Pop(ctx, &pb.PopRequest{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestPopLockedKey(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	_, err := client.Set(
		ctx,
		&pb.KeyValue{
			Key:          "foo",
			LockDuration: durationpb.New(1 * time.Hour),
			Value:        []byte("bar"),
		},
	)
	failOnErr(t, err)

	otherClient := newClient(t, srv, lis, "someotherclientid")

	_, err = otherClient.Pop(ctx, &pb.PopRequest{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.PermissionDenied)
}

func TestMissingClientID(t *testing.T) {
	_, lis := newServer(t, nil, nil)
	nc := newBadClient(t, lis, nil)
	_, err := nc.Set(ctx, &pb.KeyValue{Key: "foo", Value: []byte("bar")})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.InvalidArgument)

}

func TestEmptyClientID(t *testing.T) {
	id := ""
	_, lis := newServer(t, nil, nil)
	nc := newBadClient(t, lis, &id)
	_, err := nc.Set(ctx, &pb.KeyValue{Key: "foo", Value: []byte("bar")})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.InvalidArgument)

}

func TestReadLockedKey(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	otherClient := newClient(t, srv, lis, "someOtherID")
	key := "foo"
	lockDuration := 5 * time.Second
	_, err := client.Set(
		ctx,
		&pb.KeyValue{
			Key:          key,
			Value:        []byte("bar"),
			LockDuration: durationpb.New(lockDuration),
		},
	)
	fatalOnErr(t, err)
	tm := time.NewTimer(lockDuration + 1*time.Second)

	clientDone := make(chan error, 1)
	otherClientDone := make(chan error, 1)

	go func() {
		_, e := otherClient.Get(ctx, &pb.Key{Key: key})
		otherClientDone <- e
	}()

	go func() {
		_, e := client.Get(ctx, &pb.Key{Key: key})
		clientDone <- e
	}()
	checksDone := 0
	for checksDone < 3 {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out")
		case ce := <-clientDone:
			checksDone++
			assertEqual(t, ce, nil)

		case oe := <-otherClientDone:
			checksDone++
			assertNotNil(t, oe)
			assertErrorCode(t, status.Code(oe), codes.PermissionDenied)
			assertEqual(
				t,
				strings.Contains(oe.Error(), ErrLocked.Message),
				true,
			)
		case <-tm.C:
			checksDone++
			val, e := otherClient.Get(ctx, &pb.Key{Key: key})
			fatalOnErr(t, e)
			assertSlicesEqual(t, val.Value, []byte("bar"))
		}
	}

}

func TestKeyValueStore(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	key := "testKey"
	secondKey := "bar"
	value := []byte("testValue")
	initialSize := uint64(len(value))

	startKeys, err := client.ListKeys(
		ctx,
		&pb.ListKeysRequest{IncludeReserved: true},
	)
	startKeyCt := uint64(len(startKeys.Keys))
	failOnErr(t, err)
	assertEqual(t, srv.numKeys.Load(), startKeyCt)

	// Set a Key-value pair
	kv, err := client.Set(ctx, &pb.KeyValue{Key: key, Value: value})
	failOnErr(t, err)
	assertEqual(t, kv.IsNew, true)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)
	//assertEqual(t, srv.TotalSize(), initialSize)

	// Get the value by Key and validate it matches what we set
	resp, err := client.Get(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertSlicesEqual(t, resp.Value, value)

	kvInfo, err := client.GetKeyInfo(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)
	assertEqual(t, kvInfo.Version, 1) // first version
	assertEqual(t, kvInfo.Size, initialSize)

	// Value should've been hashed
	originalHash := kvInfo.Hash
	assertNotEqual(t, originalHash, "")

	// Created timestamp should've been populated
	created := kvInfo.Created.AsTime()
	assertEqual(t, created.IsZero(), false)
	assertEqual(t, kvInfo.Updated.AsTime().Unix(), 0) // no updates yet

	// update the value
	newValue := []byte("foo")
	secondSize := uint64(len(newValue))
	kv, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: newValue})
	failOnErr(t, err)
	assertEqual(t, kv.IsNew, false)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)
	//assertEqual(t, srv.TotalSize(), secondSize)

	// Update should match our new value
	resp, err = client.Get(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertSlicesEqual(t, resp.Value, newValue)

	srv.mu.RLock()
	sk := srv.store[key]
	sk.mu.RLock()
	sk.mu.RUnlock()
	srv.mu.RUnlock()

	kvInfo, err = client.GetKeyInfo(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertEqual(t, kvInfo.Version, 2)                    // second version
	assertNotEqual(t, kvInfo.Updated.AsTime().Unix(), 0) // should've been set
	assertLessThanOrEqualTo(
		t,
		kvInfo.Created.AsTime().Unix(),
		kvInfo.Updated.AsTime().Unix(),
	)
	assertEqual(t, kvInfo.Size, secondSize)
	assertNotEqual(t, originalHash, kvInfo.Hash)
	assertNotEqual(t, kvInfo.Hash, "")

	updated := kvInfo.Updated.AsTime()
	assertEqual(t, updated.IsZero(), false)

	var nonNegative bool
	if updated.Unix() > 0 {
		nonNegative = true
	}
	assertEqual(t, nonNegative, true)

	// add a new Key, value
	_, err = client.Set(ctx, &pb.KeyValue{Key: secondKey, Value: value})
	failOnErr(t, err)

	assertEqual(t, srv.numKeys.Load(), 2+startKeyCt)
	//updatedSize := uint64(len(value)) + secondSize
	//assertEqual(t, srv.TotalSize(), updatedSize)

	dr, err := client.Delete(ctx, &pb.DeleteRequest{Key: secondKey})
	failOnErr(t, err)
	assertEqual(t, dr.Deleted, true)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)
	//assertEqual(t, srv.TotalSize(), secondSize)

	popResp, err := client.Pop(ctx, &pb.PopRequest{Key: key})
	failOnErr(t, err)
	assertEqual(t, kv.IsNew, false)
	assertSlicesEqual(t, popResp.Value, newValue)

	_, err = client.Get(ctx, &pb.Key{Key: key})
	assertNotNil(t, err)
	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	assertErrorCode(t, e.Code(), codes.NotFound)

	_, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: value})
	failOnErr(t, err)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)

	clearResp, err := client.Clear(ctx, &pb.ClearRequest{})
	failOnErr(t, err)
	assertEqual(t, clearResp.Success, true)
	assertEqual(t, clearResp.KeysDeleted, 1)
	assertEqual(t, srv.numKeys.Load(), startKeyCt)

	stats, err := client.Stats(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, *stats.Keys, startKeyCt)
	//assertEqual(t, *stats.TotalSize, totalSize)
}

func TestDetectContentType(t *testing.T) {
	logo := getGoLogo(t)
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	k := &pb.KeyValue{Key: "logo", Value: logo}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	keyInfo, err := client.GetKeyInfo(ctx, &pb.Key{Key: k.Key})
	failOnErr(t, err)
	assertEqual(t, keyInfo.ContentType, "image/png")
}

func TestGetVersion(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	// Set initial value 'bar'
	initialValue := []byte("bar")
	k := &pb.KeyValue{Key: "foo", Value: initialValue}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	// Versions start at 1, so getting version 1 of a value that has no
	// history should return that current value
	rev, err := client.GetRevision(
		ctx,
		&pb.GetRevisionRequest{Key: "foo", Version: 1},
	)
	failOnErr(t, err)
	assertSlicesEqual(t, k.Value, rev.Value)

	// Update the value so the current version becomes 2, and 1 goes
	// into the history
	secondValue := []byte("baz")
	_, err = client.Set(ctx, &pb.KeyValue{Key: "foo", Value: secondValue})
	failOnErr(t, err)

	// Version 2 should return the current value
	rev, err = client.GetRevision(
		ctx,
		&pb.GetRevisionRequest{Key: "foo", Version: 2},
	)
	failOnErr(t, err)
	assertEqual(t, string(rev.Value), string(secondValue))
	assertNotNil(t, rev.Timestamp)

	// Getting 1 should return the initial value set
	rev, err = client.GetRevision(
		ctx,
		&pb.GetRevisionRequest{Key: "foo", Version: 1},
	)
	failOnErr(t, err)
	assertEqual(t, string(rev.Value), string(initialValue))
	assertNotNil(t, rev.Timestamp)

}

func getGoLogo(t *testing.T) []byte {
	t.Helper()
	f := filepath.Join("testdata", "go.png")
	file, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("unable to read file: %s: %s", f, err.Error())
	}
	return file
}

func TestRestoreExistingFile(t *testing.T) {
	tempServer, err := NewServer(&Config{})
	failOnErr(t, err)
	assertEqual(t, len(tempServer.store), 0)

	f := filepath.Join("testdata", "restore.json")
	file, err := os.ReadFile(f)
	failOnErr(t, err)

	ct, err := tempServer.Restore(file)
	failOnErr(t, err)
	assertEqual(t, ct, 2)

	assertEqual(t, len(tempServer.store), 2)
	fmt.Printf("%#v\n", tempServer.store)

	kv := tempServer.store["foo"]
	assertSlicesEqual(t, kv.Value, []byte("bar"))

	kv = tempServer.store["bar"]
	assertSlicesEqual(t, kv.Value, []byte("baz"))

	assertEqual(t, tempServer.store["baz"], nil)

}

func TestEvents(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	x, err := srv.Subscribe("x")
	failOnErr(t, err)
	y, err := srv.Subscribe("y")
	failOnErr(t, err)
	z, err := srv.Subscribe("z")
	failOnErr(t, err)

	xResults := make([]Event, 0, 5)
	yResults := make([]Event, 0, 5)
	zResults := make([]Event, 0, 5)

	wg := &sync.WaitGroup{}

	//xDone := make(chan struct{})
	//yDone := make(chan struct{})
	//zDone := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Log("watching x")
		for xv := range x {
			t.Logf("x: got event: %#v", xv)
			if xv.Key != "foo" {
				continue
			}
			xResults = append(xResults, xv)

			if len(xResults) == 3 {
				t.Log("waiting to unsubscribe x")
				//ue := srv.Unsubscribe("x")
				//failOnErr(t, ue)
				break
			}
			if ctx.Err() != nil {
				t.Error("x timed out")
				return
			}

		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Log("watching y")
		for xv := range y {
			t.Logf("y: got event: %#v", xv)
			if xv.Key != "foo" {
				continue
			}
			yResults = append(yResults, xv)
			if len(yResults) == 3 {
				t.Log("waiting to unsubscribe y")
				//ue := srv.Unsubscribe("y")
				//failOnErr(t, ue)
				break
			}
			if ctx.Err() != nil {
				t.Error("y timed out")
				return
			}

		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		t.Log("watching z")
		for xv := range z {
			t.Logf("z event count: %d", len(zResults))

			t.Logf("z: got event: %#v", xv)
			if xv.Key != "foo" {
				continue
			}
			zResults = append(zResults, xv)

			if len(zResults) == 3 {
				t.Log("waiting to unsubscribe z")
				//ue := srv.Unsubscribe("z")
				//failOnErr(t, ue)
				break
			}
			if ctx.Err() != nil {
				t.Error("z timed out")
				return
			}

		}
	}()

	foo := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	fooUpdate := &pb.KeyValue{Key: "foo", Value: []byte("baz")}
	_, err = client.Set(ctx, foo)
	failOnErr(t, err)

	_, err = client.Set(ctx, fooUpdate)
	failOnErr(t, err)

	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: "foo"})
	failOnErr(t, err)

	t.Log("waiting on events to load")
	wg.Wait()
	if ctx.Err() != nil {
		t.Fatalf("context cancelled: %s", ctx.Err().Error())
	}

	//wg.Wait()

	t.Logf("x: %#v", xResults)
	t.Logf("y: %#v", yResults)
	t.Logf("z: %#v", zResults)

	assertEqual(t, len(xResults), 3)
	assertEqual(t, len(yResults), 3)
	assertEqual(t, len(zResults), 3)

	assertEqual(t, xResults[0].Event, Created)
	assertEqual(t, xResults[0].Key, foo.Key)
	assertEqual(t, xResults[1].Event, Updated)
	assertEqual(t, xResults[2].Event, Deleted)

	assertEqual(t, yResults[0].Event, Created)
	assertEqual(t, yResults[0].Key, foo.Key)
	assertEqual(t, yResults[1].Event, Updated)
	assertEqual(t, yResults[2].Event, Deleted)

	assertEqual(t, zResults[0].Event, Created)
	assertEqual(t, zResults[0].Key, foo.Key)
	assertEqual(t, zResults[1].Event, Updated)
	assertEqual(t, zResults[2].Event, Deleted)

}

func TestSnapshot(t *testing.T) {
	tmpdir := t.TempDir()
	d := time.Duration(0)

	cfg := DefaultConfig()
	cfg.Snapshot.Dir = tmpdir
	cfg.Snapshot.Interval = d
	cfg.Snapshot.Limit = 1

	srv, lis := newServer(t, nil, cfg)
	client := newClient(t, srv, lis, "")
	kv := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	_, err := client.Set(ctx, kv)
	failOnErr(t, err)

	snapshotter, err := NewSnapshotter(srv)
	failOnErr(t, err)

	filename, err := snapshotter.Snapshot()
	failOnErr(t, err)

	assertEqual(t, snapshotter.lastSnapshot, filename)

	tmpServer, err := NewServer(&Config{})
	failOnErr(t, err)
	file, err := os.Open(filename)
	failOnErr(t, err)
	defer file.Close()

	reader, err := gzip.NewReader(file)
	failOnErr(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)

	failOnErr(t, err)
	err = json.Unmarshal(data, tmpServer)
	failOnErr(t, err)

	assertEqual(t, len(tmpServer.store), 1)
	assertSlicesEqual(t, tmpServer.store[kv.Key].Value, kv.Value)
}

func TestEncryptedSnapshot(t *testing.T) {
	tmpdir := t.TempDir()
	d := time.Duration(0)

	cfg := DefaultConfig()
	cfg.Snapshot.Enabled = true
	cfg.Snapshot.Dir = tmpdir
	cfg.Snapshot.Limit = 1
	cfg.Snapshot.Encrypt = true
	secretKey := "qwertyuiopasdfgh"
	cfg.Snapshot.SecretKey = secretKey
	cfg.Snapshot.Interval = d

	srv, lis := newServer(t, nil, cfg)
	client := newClient(t, srv, lis, "")
	kv := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	_, err := client.Set(ctx, kv)
	failOnErr(t, err)

	snapshotter, err := NewSnapshotter(srv)
	failOnErr(t, err)

	filename, err := snapshotter.EncryptedSnapshot()
	failOnErr(t, err)
	fileDir := filepath.Dir(filename)
	assertEqual(t, fileDir, tmpdir)

	assertEqual(t, snapshotter.lastSnapshot, filename)

	tmpServer, err := NewServer(&Config{})
	failOnErr(t, err)
	file, err := os.Open(filename)
	failOnErr(t, err)
	defer file.Close()

	reader, err := gzip.NewReader(file)
	failOnErr(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	failOnErr(t, err)

	decryptedData, err := decrypt(secretKey, data)
	failOnErr(t, err)

	err = json.Unmarshal(decryptedData, tmpServer)
	failOnErr(t, err)

	assertEqual(t, len(tmpServer.store), 1)
	assertSlicesEqual(t, tmpServer.store[kv.Key].Value, kv.Value)
}

func TestMarshal(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	originalKeys := []string{}
	d := time.Duration(3600) * time.Second
	for i := 0; i < 50; i++ {
		k := &pb.KeyValue{
			Key:   fmt.Sprintf("Key-%d", i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		if i > 25 && i < 30 {
			k.LockDuration = durationpb.New(d)
		}
		originalKeys = append(originalKeys, k.Key)
		keyCreated, err := client.Set(ctx, k)
		fatalOnErr(t, err)
		t.Logf("created key: %+v", keyCreated)
	}
	srv.mu.RLock()
	data, err := json.MarshalIndent(srv, "", "  ")
	srv.mu.RUnlock()
	failOnErr(t, err)
	t.Logf("data:\n%s", data)

	// Create a separate server, unmarshal the first server's data into it
	newStore, err := NewServer(&Config{HashAlgorithm: crypto.MD5})
	fatalOnErr(t, err)
	t.Logf("unmarshaling to new store")
	newStore.mu.Lock()
	t.Cleanup(
		func() {
			newStore.mu.Unlock()
		},
	)
	err = json.Unmarshal(data, newStore)

	fatalOnErr(t, err)

	newKeys := []string{}
	for k := range newStore.store {
		newKeys = append(newKeys, k)
	}
	requireSliceContains(t, newKeys, originalKeys...)
	requireSliceContains(t, originalKeys, newKeys...)
	assertNotNil(t, newStore.logger)

	for keyName, keyInfo := range srv.store {

		t.Logf("checking key: %s", keyName)
		otherKeyInfo := newStore.store[keyName]
		if strings.HasPrefix(keyName, reservedPrefix) {
			if otherKeyInfo != nil {
				t.Fatalf("key %s found in new store", keyName)
			}
			continue
		}
		if otherKeyInfo == nil {
			t.Fatalf("key %s not found in new store", keyName)
			continue
		}
		assertSlicesEqual(t, keyInfo.Value, otherKeyInfo.Value)
		assertEqual(t, keyInfo.Hash, otherKeyInfo.Hash)
		assertEqual(t, keyInfo.Size, otherKeyInfo.Size)
		//assertEqual(t, keyInfo.Locked, otherKeyInfo.Locked)
		firstLock, locked := srv.locks[keyName]
		otherLock, otherLocked := newStore.locks[keyName]
		assertEqual(
			t,
			locked,
			otherLocked,
			fmt.Sprintf("Key: %s", keyName),
			fmt.Sprintf("Lock data: %+v", firstLock),
			fmt.Sprintf("other lock data: %+v", otherLock),
		)
	}
}

func TestDump(t *testing.T) {
	dir := t.TempDir()
	targetFile := "backup.json"
	targetPath := filepath.Join(dir, targetFile)

	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	kv := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	_, err := client.Set(ctx, kv)
	failOnErr(t, err)

	err = srv.Dump(targetPath)
	failOnErr(t, err)

	data, err := os.ReadFile(targetPath)
	failOnErr(t, err)

	_, err = client.Set(ctx, &pb.KeyValue{Key: "bar", Value: []byte("baz")})
	failOnErr(t, err)

	ct, err := srv.Restore(data)
	failOnErr(t, err)
	assertEqual(t, ct, 1)

	val, err := client.Get(ctx, &pb.Key{Key: "foo"})
	failOnErr(t, err)
	assertSlicesEqual(t, val.Value, []byte("bar"))

	_, err = client.Get(ctx, &pb.Key{Key: "bar"})
	assertNotNil(t, err)
	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestMaxValueSize(t *testing.T) {
	newMaxSize := uint64(5)
	srv, lis := newServer(t, nil, &Config{MaxValueSize: newMaxSize})
	client := newClient(t, srv, lis, "")

	assertEqual(t, srv.cfg.MaxValueSize, newMaxSize)

	key := "testKey"
	value := []byte("123456")

	exists, _ := client.Exists(ctx, &pb.Key{Key: key})
	assertEqual(t, exists.Exists, false)

	// Set a Key-value pair
	_, err := client.Set(ctx, &pb.KeyValue{Key: key, Value: value})
	if err == nil {
		kv, err := client.Get(ctx, &pb.Key{Key: key})
		failOnErr(t, err)
		size := len(kv.Value)
		if size < int(newMaxSize) {
			t.Fatalf(
				"expected size to be less than %d, got %d",
				newMaxSize,
				size,
			)
		}
	}
	assertNotNil(t, err)

	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	assertErrorCode(t, e.Code(), codes.FailedPrecondition)

}

// failOnErr is a helper function that takes the result of a function that
// only has 1 return value (error), and fails the test if the error is not nil.
// It's intended to reduce boilerplate code in tests.
func failOnErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func assertEqual[V comparable](t *testing.T, val V, expected V, msg ...string) {
	t.Helper()
	if val != expected {
		t.Fatalf(
			"expected:\n%+v\n\ngot:\n%+v\n%s",
			expected,
			val,
			strings.Join(msg, "\n"),
		)
	}
}

func assertGreaterOrEqual[V ~int | ~float64 | ~uint | ~int64](
	t *testing.T,
	val V,
	threshold V,
) {
	t.Helper()
	if val < threshold {
		t.Errorf("expected >=%v, got %v", threshold, val)
	}
}

func assertGreaterThan[V ~int | ~float64 | ~uint | ~int64](
	t *testing.T,
	val V,
	target V,
) {
	t.Helper()
	if val <= target {
		t.Errorf("expected >%v, got %v", target, val)
	}
}

func assertLessThanOrEqualTo[V ~int | ~float64 | ~uint | ~int64](
	t *testing.T,
	val V,
	target V,
) {
	t.Helper()
	if val > target {
		t.Errorf("expected <=%v, got %v", target, val)
	}
}

func assertNotEqual[V comparable](t *testing.T, val V, expected V) {
	t.Helper()
	if val == expected {
		t.Errorf(
			"expected:\n%#v\n\ngot:\n%#v",
			expected,
			val,
		)
	}
}

func assertSlicesEqual[V comparable](t *testing.T, value []V, expected []V) {
	t.Helper()
	if len(value) != len(expected) {
		t.Fatalf(
			"expected %d elements, got %d\n%v\n%v",
			len(expected),
			len(value),
			value,
			expected,
		)
	}

	for i, v := range value {
		if v != expected[i] {
			t.Errorf(
				"index %d: expected:\n%#v\n\ngot:\n%#v",
				i,
				expected,
				value,
			)
		}
	}
}

func requireSliceContains[V comparable](
	t *testing.T,
	value []V,
	expected ...V,
) {
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
		t.Fatalf(
			"expected:\n%#v\n\nnot found in:\n%#v", expected, value,
		)
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

func assertSliceContainsOneOf[V comparable](
	t *testing.T,
	value []V,
	expected ...V,
) {
	t.Helper()
	if len(expected) == 0 {
		t.Fatalf("expected slice must not be empty")
	}
	for _, v := range value {
		for _, exp := range expected {
			if v == exp {
				return
			}
		}
	}
	t.Errorf(
		"expected slice to contain one of: %#v (got: %#v)",
		expected,
		value,
	)
}

func assertErrorCode(
	t *testing.T,
	code codes.Code,
	expectedCode codes.Code,
	msg ...string,
) {
	t.Helper()
	if code != expectedCode {
		t.Fatalf(
			"expected:\n%s\n\ngot:\n%s\n%s",
			expectedCode.String(),
			code.String(),
			strings.Join(msg, "\n"),
		)
	}
}

func assertNotNil(t *testing.T, v any) {
	t.Helper()
	if v == nil {
		t.Fatalf("expected non-nil value")

	}
}

//
//func BenchmarkSetKey(b *testing.B) {
//	client := newBClient(b)
//	srv.cfg.HashAlgorithm = crypto.MD5
//	log.SetOutput(io.Discard)
//	b.ResetTimer()
//	//ct := 0
//	b.N = 500
//	for i := 0; i < b.N; i++ {
//		//ct++
//		_, err := client.Set(
//			ctx,
//			&pb.KeyValue{Key: "foo", Value: []byte(fmt.Sprintf("%d", i))},
//		)
//		if err != nil {
//			b.Fatalf("failed on %d: %s", i, err.Error())
//		}
//		//if i > 0 && (r.IsNew || !r.Success) {
//		//	b.Fatalf("expected not new, got: %#v", r)
//		//}
//	}
//
//	//b.Logf("final count: %d", ct)
//	//
//	//finalVal, _ := client.Get(ctx, &pb.Key{Key: "foo"})
//	//b.Logf("final value: %s", string(finalVal.Value))
//	//
//	//info, err := client.GetKeyInfo(ctx, &pb.Key{Key: "foo"})
//	//if err != nil {
//	//	b.Fatalf("%s", err.Error())
//	//}
//	//b.Logf("info: %#v", info.Version)
//	//if info.Version != 10 {
//	//	b.Fatalf("expected version 10, got: %d", info.Version)
//	//}
//	//b.RunParallel(func(pb *testing.PB) {
//	//	var
//	//})
//}

func getKV() *pb.KeyValue {
	return &pb.KeyValue{
		Key:   "foo",
		Value: []byte(strconv.Itoa(rand.Intn(5000))),
	}
}

//func BenchmarkSetKeyP(b *testing.B) {
//	client := newBClient(b)
//	srv.cfg.HashAlgorithm = crypto.MD5
//	b.N = 2
//
//	b.Logf("procs: %d", runtime.GOMAXPROCS(0))
//	log.SetOutput(io.Discard)
//	//b.ResetTimer()
//	//ct := 0
//
//	b.RunParallel(
//		func(pb *testing.PB) {
//			ukv := getKV()
//			for pb.Next() {
//				_, err := client.Set(ctx, ukv)
//				if err != nil {
//					b.Fatalf("failed %s", err.Error())
//				}
//			}
//		},
//	)
//
//}
