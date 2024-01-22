package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	kclient "github.com/arcward/keyquarry/client"
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

var (
	signals     = make(chan os.Signal, 1)
	ctx         context.Context
	cancel      context.CancelFunc
	testTimeout = 60 * time.Second
	dialTimeout = 10 * time.Second
)

func newServer(t testing.TB, lis *bufconn.Listener, cfg *Config) (
	*Server,
	*bufconn.Listener,
) {
	t.Helper()

	var srv *Server

	log.SetOutput(io.Discard)
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
		cfg = NewConfig()
		cfg.RevisionLimit = 2
		cfg.MinLifespan = time.Duration(1) * time.Second
		cfg.MinLockDuration = time.Duration(1) * time.Second
		cfg.PruneInterval = 0
		cfg.LogLevel = "DEBUG"
		cfg.Name = t.Name()
		cfg.MaxLockDuration = time.Duration(1) * time.Hour
	}
	if cfg.Name == "" {
		cfg.Name = t.Name()
	}
	handler := slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{AddSource: true, Level: slog.LevelDebug},
	)
	logger := slog.New(handler).WithGroup(t.Name())
	slog.SetDefault(logger)
	cfg.Logger = logger

	srv, err = New(cfg)
	if err != nil {
		panic(err)
	}
	srv.lis = lis

	tctx, tcancel := context.WithTimeout(ctx, testTimeout)

	go func() {
		select {
		case <-tctx.Done():
			if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
				t.Fatalf("%s: timeout exceeded", t.Name())
			}
		}
	}()

	go func() {
		if e := srv.Serve(tctx); e != nil {
			panic(e)
		}
	}()

	t.Cleanup(
		func() {
			tcancel()
			srv.shutdown <- struct{}{}
		},
	)

	return srv, lis
}

func init() {
	log.SetOutput(io.Discard)
	fmt.Println("starting stuff")

	ctx, cancel = context.WithCancel(context.Background())
	go func() {
		select {
		case <-signals:
			cancel()
			panic("interrupted")
		}
	}()
}

func newBadClient(
	t *testing.T,
	lis *bufconn.Listener,
	clientID *string,
) pb.KeyQuarryClient {
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
			grpc.WithPerRPCCredentials(kclient.NewClientIDCredentials(*clientID)),
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
			_ = clientConn.Close()
			tcancel()
		},
	)

	nc := pb.NewKeyQuarryClient(clientConn)
	return nc
}

func newClient(
	t testing.TB,
	srv *Server,
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
	handler := slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{AddSource: true, Level: slog.LevelError},
	)
	logger := slog.New(handler).WithGroup(fmt.Sprintf("%s-client", t.Name()))

	client := kclient.New(
		"bufnet",
		clientID,
		logger,
		nil,
		grpc.WithContextDialer(
			func(
				context.Context,
				string,
			) (net.Conn, error) {
				return lis.Dial()
			},
		),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:    kclient.DefaultDialKeepAliveTime,
				Timeout: kclient.DefaultDialKeepAliveTimeout,
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

	for {
		srv.mu.RLock()
		started := srv.started
		srv.mu.RUnlock()
		if started {
			break
		}
	}

	return client
}

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
	fatalOnErr(t, err)
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
	keychars := make([]byte, 0, DefaultMaxKeyLength*2)
	for i := 0; i < int(DefaultMaxKeyLength*2); i++ {
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
	_, err := client.Inspect(ctx, &pb.InspectRequest{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestScheduledPrune(t *testing.T) {
	// Key limit 50 with prune pruneAt = 0.9 and pruneTo = 0.8, means
	// once we hit 45 keys, the next pruner run should expunge down to
	// 40 keys.
	newCfg := NewConfig()
	newCfg.LogLevel = "ERROR"

	maxKeys := uint64(50)
	threshold := uint64(45)
	target := uint64(40)

	newCfg.MaxNumberOfKeys = maxKeys
	newCfg.PruneAt = threshold
	newCfg.PruneTo = target

	pruneAfter := threshold
	pruneTo := target
	newCfg.PruneInterval = 1 * time.Second
	newCfg.EventStreamSendTimeout = 5 * time.Second

	srv, lis := newServer(t, nil, newCfg)
	srv.cfgMu.RLock()
	assertEqual(t, srv.cfg.PruneTo, target)
	srv.cfgMu.RUnlock()
	client := newClient(t, srv, lis, "")

	srv.mu.RLock()
	startingKeyCount := srv.numKeys.Load()
	srv.mu.RUnlock()

	// Create keys up to the pruneAt, +1
	keysToCreate := int(pruneAfter - startingKeyCount + 1)
	for i := 0; i < keysToCreate; i++ {
		k := fmt.Sprintf("key-%d", i+1)
		kv, err := client.Set(ctx, &pb.KeyValue{Key: k, Value: []byte("bar")})
		fatalOnErr(t, err)
		assertEqual(t, kv.IsNew, true)
		assertEqual(t, kv.Success, true)
	}

	srv.mu.RLock()
	currentKeyCt := srv.numKeys.Load()
	assertEqual(t, currentKeyCt, pruneAfter+1)

	srv.cfgMu.RLock()
	pressure := srv.pressure()
	srv.cfgMu.RUnlock()
	srv.mu.RUnlock()

	assertEqual(t, pressure.Keys, currentKeyCt)
	assertEqual(t, pressure.Max, maxKeys)
	assertEqual(t, pressure.Used, 0.92)

	// validate we see the key count drop to the pruneTo 40
	pruneTimeout, pruneCancel := context.WithTimeout(ctx, 10*time.Second)
	for {
		if pruneTimeout.Err() != nil {
			t.Fatalf("timed out waiting for prune")
		}
		ct := srv.numKeys.Load()
		t.Logf("keys: %d (prune to: %d)", ct, pruneTo)
		if ct == pruneTo && srv.numPruneCompleted.Load() == 1 {
			pruneCancel()
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	assertEqual(t, srv.numKeys.Load(), pruneTo)

	// we should see expunges for the number of keys we created
	// over the pruneTo limit
	expectedPruneCt := (pruneAfter + 1) - pruneTo
	assertEqual(t, srv.numEventExpunged.Load(), expectedPruneCt)
}

func TestEagerPruneFromLock(t *testing.T) {
	// Validates eager prune behavior when creating keys via
	// locking with CreateMissing=true
	// The difference between this and the eager pruning from Set() is that
	// locked keys aren't pruned, so once we hit the key limit, we'll just
	// keep getting the same error and no new keys will be created.
	newCfg := NewConfig()

	newCfg.LogLevel = "ERROR"
	newCfg.MaxNumberOfKeys = 50
	newCfg.EagerPruneAt = 25
	newCfg.EagerPruneTo = 22
	newCfg.MinLifespan = time.Duration(1) * time.Second
	newCfg.MinLockDuration = time.Duration(1) * time.Second
	newCfg.MaxLockDuration = 1 * time.Hour
	newCfg.PruneInterval = 0
	newCfg.EventStreamSendTimeout = 5 * time.Second

	srv, lis := newServer(t, nil, newCfg)
	client := newClient(t, srv, lis, "")

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

	time.Sleep(5 * time.Second)

	capacityRemaining := maxKeys - startingKeys
	makeNumKeys := capacityRemaining * 2

	keys := make([]string, 0, makeNumKeys)
	for i := 0; i < int(makeNumKeys); i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i+1))
	}
	srv.cfgMu.RUnlock()
	fatalOnErr(t, err)

	expungeSeen := map[string]bool{}
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

	t.Logf("keys to create: %d", len(keys))
	for ind, k := range keys {
		// For whatever reason, I'm running into an issue where this test
		// randomly fails, because one or two keys created report their
		// LastSet as 1 MILLISECOND after the time created by
		// `Server.pruneNumKeys`... which doesn't make sense, because
		// that time.Time is only created on a subsequent key, after having
		// already hit the key limit. This happens even after ensuring it's
		// set before emitting the Created signal. Might be something wonky
		// with virtualbox?
		time.Sleep(1 * time.Millisecond)
		t.Logf("checking key %s", k)
		currentCt := srv.numKeys.Load() // - srv.numReservedKeys.Load()
		t.Logf("setting %s", k)
		lockResp, e := client.Lock(
			ctx,
			&pb.LockRequest{
				Key:             k,
				CreateIfMissing: true,
				Duration:        durationpb.New(10 * time.Minute),
			},
		)
		t.Logf("lock response: %#v / %v", lockResp, e)
		afterCt := srv.numKeys.Load()
		if e != nil && status.Code(e) != codes.ResourceExhausted {
			fatalOnErr(t, e)
		}

		switch {
		case currentCt > maxKeys:
			t.Fatalf(
				"exceeded max keys (max: %d current: %d) at %d (%s)",
				maxKeys,
				currentCt,
				ind,
				k,
			)
		case currentCt == maxKeys:
			assertErrorCode(t, status.Code(e), codes.ResourceExhausted)
		default:
			fatalOnErr(t, e)
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

	eagerPruneCtx, eagerPruneCancel := context.WithTimeout(ctx, 15*time.Second)
	for {
		if eagerPruneCtx.Err() != nil {
			if errors.Is(eagerPruneCtx.Err(), context.DeadlineExceeded) {
				t.Fatal("timed out waiting for eager prune to finish")
			}
		}

		pruneCompleted := srv.numEagerPruneTriggered.Load()
		if pruneCompleted > 0 {
			eagerPruneCancel()
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	eagerTriggered := srv.numEagerPruneTriggered.Load()
	if eagerTriggered == 0 {
		t.Fatalf(
			"expected eager prune to be triggered at least once",
		)
	}
	finalCt := srv.numKeys.Load()
	// we should still be at the max number of keys, as they're all either
	// locked or reserved
	assertEqual(t, finalCt, 50)
}

func TestEagerPrune(t *testing.T) {
	newCfg := NewConfig()
	newCfg.LogLevel = "ERROR"
	newCfg.MaxNumberOfKeys = 50
	newCfg.EagerPruneAt = 25
	newCfg.EagerPruneTo = 22
	newCfg.MinLifespan = time.Duration(1) * time.Second
	newCfg.MinLockDuration = time.Duration(1) * time.Second
	newCfg.PruneInterval = 0
	newCfg.EventStreamSendTimeout = 5 * time.Second

	srv, lis := newServer(t, nil, newCfg)
	client := newClient(t, srv, lis, "")

	resKeys, err := client.ListKeys(
		ctx,
		&pb.ListKeysRequest{IncludeReserved: true},
	)
	if err != nil {
		t.Fatalf("failed to list keys: %s", err.Error())
	}
	startingKeys := uint64(len(resKeys.Keys))
	srv.cfgMu.RLock()

	time.Sleep(5 * time.Second)

	capacityRemaining := newCfg.EagerPruneAt - startingKeys
	makeNumKeys := capacityRemaining * 2

	keys := make([]string, 0, makeNumKeys)
	for i := 0; i < int(makeNumKeys); i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i+1))
	}
	srv.cfgMu.RUnlock()
	fatalOnErr(t, err)

	expungeSeen := map[string]bool{}
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

	t.Logf("keys to create: %d", len(keys))
	var currentCt uint64
	var afterCt uint64
	pruneCompletedCt := srv.numPruneCompleted.Load()
	assertEqual(t, pruneCompletedCt, 0)
	// var pruneCt uint64 = srv.numPruneCompleted.Load()
	for _, k := range keys {
		// For whatever reason, I'm running into an issue where this test
		// randomly fails, because one or two keys created report their
		// LastSet as 1 MILLISECOND after the time created by
		// `Server.pruneNumKeys`... which doesn't make sense, because
		// that time.Time is only created on a subsequent key, after having
		// already hit the key limit. This happens even after ensuring it's
		// set before emitting the Created signal. Might be something wonky
		// with virtualbox?
		time.Sleep(50 * time.Millisecond)
		t.Logf("checking key %s", k)
		currentCt = srv.numKeys.Load()
		// if afterCt == newCfg.EagerPruneAt && (int(capacityRemaining) - in{
		// 	assertEqual(t, currentCt, newCfg.EagerPruneTo+1)
		// }

		_, e := client.Set(ctx, &pb.KeyValue{Key: k, Value: []byte("bar")})
		fatalOnErr(t, e)

		// time.Sleep(1 * time.Millisecond)
		if currentCt == newCfg.EagerPruneAt {
			pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
			for {
				if pctx.Err() != nil {
					if errors.Is(pctx.Err(), context.DeadlineExceeded) {
						t.Fatal("timed out waiting for prune")
					}
				}
				newPruneCt := srv.numPruneCompleted.Load()
				if newPruneCt > pruneCompletedCt {
					pcancel()
					pruneCompletedCt = newPruneCt
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
		}
		srv.mu.RLock()
		srv.mu.RUnlock()
		afterCt = srv.numKeys.Load()
		if afterCt == currentCt {
			t.Fatalf("key count didn't change after set")
		}
		if afterCt > newCfg.EagerPruneAt {
			t.Fatalf(
				"key count exceeded eager prune limit (current: %d, limit: %d)",
				afterCt,
				newCfg.EagerPruneAt,
			)
		}

		if e != nil && status.Code(e) == codes.ResourceExhausted {
			break
		}
	}

	eagerPruneCtx, eagerPruneCancel := context.WithTimeout(ctx, 15*time.Second)
	for {
		if eagerPruneCtx.Err() != nil {
			if errors.Is(eagerPruneCtx.Err(), context.DeadlineExceeded) {
				t.Fatal("timed out waiting for eager prune to finish")
			}
		}

		pruneCompleted := srv.numPruneCompleted.Load()
		if pruneCompleted >= 3 {
			eagerPruneCancel()
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	eagerTriggered := srv.numEagerPruneTriggered.Load()
	if eagerTriggered != 3 {
		t.Fatalf(
			"expected eager prune to be triggered three times, got %d",
			eagerTriggered,
		)
	}
	prunesCompleted := srv.numPruneCompleted.Load()
	if prunesCompleted != 3 {
		t.Fatalf(
			"expected prune to be completed three times, got %d",
			prunesCompleted,
		)
	}
}

func TestKeySort(t *testing.T) {
	fooCreated := time.Now()

	barCreated := fooCreated.Add(time.Duration(-12) * time.Hour)
	watUpdated := barCreated.Add(time.Duration(1) * time.Hour)
	futureLock := fooCreated.Add(time.Duration(1) * time.Hour) // locked an hour from now
	expectedOrder := []string{"bar", "wat", "foo", "baz"}

	keys := []*keyMetricWithName{
		{
			Key: "foo",
			Metric: &keyLifetimeMetric{
				FirstSet: &fooCreated, // created just now
				LastSet:  &fooCreated,
			},
		},
		{
			Key: "bar",
			Metric: &keyLifetimeMetric{
				FirstSet: &barCreated, // created 12 hours ago
				LastSet:  &barCreated,
			},
		},
		{
			Key: "wat",
			Metric: &keyLifetimeMetric{
				FirstSet: &barCreated, // created 12 hours ago
				LastSet:  &watUpdated, // updated 11 hours ago
			},
		},
		{
			Key: "baz",
			Metric: &keyLifetimeMetric{
				FirstSet:    &fooCreated, // created 12 hours ago
				LastSet:     &fooCreated,
				FirstLocked: &futureLock,
				LastLocked:  &futureLock, // locked an hour from now
			},
		},
	}
	scores := map[string]float64{}
	now := time.Now()
	for _, k := range keys {
		scores[k.Key] = k.Metric.StaleScore(now)
	}
	t.Logf("scores: %#v", scores)

	sorted := sortKeyValueInfoByDates(keys)
	assertEqual(t, len(sorted), len(keys))
	for ind, k := range sorted {
		assertEqual(t, k, expectedOrder[ind])
	}
}

func TestDBSnapshot(t *testing.T) {
	tmpdir := t.TempDir()
	dbfile := filepath.Join(tmpdir, "test.db")

	cfg := NewConfig()
	cfg.Name = t.Name()
	connStr := fmt.Sprintf("sqlite://%s", dbfile)
	cfg.Snapshot.Database = connStr
	cfg.Snapshot.Enabled = true
	cfg.MaxLockDuration = 1 * time.Hour

	dialect := GetDialect(connStr)
	if dialect == nil {
		t.Fatalf("failed to get dialect")
	}
	err := dialect.InitDB(ctx, connStr)
	fatalOnErr(t, err)

	srv, lis := newServer(t, nil, cfg)
	client := newClient(t, srv, lis, "")

	srv.cfgMu.RLock()
	revLimit := srv.Config().RevisionLimit
	srv.cfgMu.RUnlock()

	_, err = client.Set(
		ctx, &pb.KeyValue{
			Key:          "foo",
			Value:        []byte("bar"),
			LockDuration: durationpb.New(5 * time.Minute),
			Lifespan:     durationpb.New(120 * time.Minute),
		},
	)
	fatalOnErr(t, err)

	for i := 0; i < int(revLimit)+5; i++ {
		_, err = client.Set(
			ctx, &pb.KeyValue{
				Key:          "foo",
				Value:        []byte(fmt.Sprintf("baz-%d", i)),
				LockDuration: durationpb.New(60 * time.Minute),
			},
		)
		fatalOnErr(t, err)
	}

	_, err = client.Get(ctx, &pb.Key{Key: "foo"})
	fatalOnErr(t, err)

	srv.cfgMu.RLock()
	srv.mu.RLock()
	srv.lockMu.RLock()
	srv.reaperMu.RLock()
	srv.keyStatMu.RLock()
	srv.cmu.RLock()

	snapshotID, err := srv.Snapshot(ctx)
	fatalOnErr(t, err)
	db, err := dialect.DB(connStr)
	fatalOnErr(t, err)
	t.Cleanup(
		func() {
			_ = db.Close()
		},
	)

	snapshotRec, err := dialect.GetSnapshot(ctx, db, snapshotID)
	fatalOnErr(t, err)
	assertEqual(t, snapshotRec.ServerName, t.Name())

}

func TestInspectWithValue(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	k := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	inspect, err := client.Inspect(
		ctx,
		&pb.InspectRequest{Key: k.Key, IncludeValue: true},
	)
	failOnErr(t, err)
	assertEqual(t, inspect.Key, k.Key)
	assertSlicesEqual(t, inspect.Value, k.Value)

	inspect, err = client.Inspect(
		ctx,
		&pb.InspectRequest{
			Key:            k.Key,
			IncludeValue:   true,
			IncludeMetrics: true,
		},
	)
	failOnErr(t, err)
	assertEqual(t, inspect.Metrics.AccessCount, 1)

}

func TestLoadDBSnapshot(t *testing.T) {
	tmpdir := t.TempDir()
	dbfile := filepath.Join(tmpdir, "test.db")

	cfg := NewConfig()
	cfg.Name = t.Name()
	connStr := fmt.Sprintf("sqlite://%s", dbfile)
	cfg.Snapshot.Database = connStr
	cfg.Snapshot.Enabled = true

	dialect := GetDialect(connStr)
	if dialect == nil {
		t.Fatalf("failed to get dialect")
	}
	err := dialect.InitDB(ctx, connStr)
	fatalOnErr(t, err)

	srv, lis := newServer(t, nil, cfg)
	client := newClient(t, srv, lis, "")

	srv.cfgMu.RLock()
	srv.cfgMu.RUnlock()

	fooKV := &pb.KeyValue{
		Key:          "foo",
		Value:        []byte("bar"),
		LockDuration: durationpb.New(5 * time.Minute),
		Lifespan:     durationpb.New(120 * time.Minute),
	}
	_, err = client.Set(
		ctx, fooKV,
	)
	fatalOnErr(t, err)

	srv.cfgMu.RLock()
	srv.mu.RLock()
	srv.lockMu.RLock()
	srv.reaperMu.RLock()
	srv.keyStatMu.RLock()
	srv.cmu.RLock()

	snapshotID, err := srv.Snapshot(ctx)
	fatalOnErr(t, err)

	newCfg := NewConfig()
	newCfg.Name = t.Name()
	newCfg.Snapshot.Database = connStr
	newSrv, err := serverFromDB(ctx, newCfg)
	fatalOnErr(t, err)
	kv, exists := newSrv.store[fooKV.Key]
	if !exists {
		t.Fatalf("expected to find key %s", fooKV.Key)
	}

	assertSlicesEqual(t, kv.Value, fooKV.Value)

	db, err := dialect.DB(connStr)
	fatalOnErr(t, err)

	rec, err := dialect.GetSnapshot(ctx, db, snapshotID)
	fatalOnErr(t, err)
	assertEqual(t, rec.ServerName, cfg.Name)
	assertEqual(t, rec.ID, snapshotID)
	assertEqual(t, rec.Created.IsZero(), false)
}

func TestKeyHistory(t *testing.T) {
	cfg := NewConfig()
	var newRevisionLimit int64 = 3
	cfg.RevisionLimit = newRevisionLimit
	cfg.PersistentRevisions = true
	srv, lis := newServer(t, nil, cfg)

	client := newClient(t, srv, lis, "")

	k := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	originalValue := k.Value
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	keyInfo, ok := srv.store[k.Key]
	assertEqual(t, ok, true)
	srv.hmu.RLock()
	assertNotNil(t, srv.history[k.Key])
	// the value should be on the store only, with no history
	assertEqual(t, len(srv.history[k.Key]), 1)
	srv.hmu.RUnlock()

	versions := map[int][]byte{}
	versions[0] = originalValue

	// create a few revisions, with a value including the expected version
	for i := 0; i < int(newRevisionLimit); i++ {
		newValue := []byte(fmt.Sprintf("%s-%d", string(originalValue), i+1))
		k.Value = newValue
		_, err = client.Set(ctx, k)
		failOnErr(t, err)
	}
	srv.hmu.RLock()
	assertEqual(t, len(srv.history[k.Key]), int(newRevisionLimit))
	versionsSeen := map[uint64]bool{}
	for _, v := range srv.history[k.Key] {
		_, seen := versionsSeen[v.Version]
		if seen {
			t.Fatalf("found duplicate version %d", v.Version)
		}
		versionsSeen[v.Version] = true
	}

	_, v1Found := versionsSeen[1]
	if v1Found {
		t.Errorf("expected to not see version 1 in: %+v", versionsSeen)
	}

	_, v2Found := versionsSeen[2]
	if !v2Found {
		t.Fatalf("expected to see version 1 in: %+v", versionsSeen)
	}

	secondRevision, found := srv.getVersion(keyInfo.Key, 2)
	if !found {
		t.Fatal("expected to find version 1")
	}
	assertNotNil(t, secondRevision)
	assertNotNil(t, secondRevision.Version)
	assertEqual(t, secondRevision.Version, 2)
	// assertSlicesEqual(t, secondRevision.Value, originalValue)

	var previousRevision *keyValueSnapshot

	for ind, rev := range srv.history[keyInfo.Key] {
		rev := rev
		assertEqual(t, rev.Version, uint64(ind+2))

		assertEqual(t, rev.Timestamp.IsZero(), false)
		if previousRevision != nil {
			assertEqual(t, rev.Version-1, previousRevision.Version)
			expectedVal := fmt.Sprintf("%s-%d", string(originalValue), ind+1)
			currentVal := string(rev.Value)
			assertEqual(t, currentVal, expectedVal)
		}
		previousRevision = rev
	}

	srv.hmu.RUnlock()

	k.Value = []byte("final")
	_, err = client.Set(ctx, k)
	fatalOnErr(t, err)

	srv.hmu.RLock()
	assertEqual(t, len(srv.history[keyInfo.Key]), int(newRevisionLimit))
	hist := srv.history[keyInfo.Key]
	for i := 0; i < len(hist); i++ {
		assertNotEqual(t, hist[i], secondRevision)
	}
	assertEqual(
		t,
		string(hist[len(hist)-1].Value),
		string(k.Value),
	)

	srv.hmu.RUnlock()
	_, err = client.ClearHistory(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, len(srv.history[keyInfo.Key]), 0)
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

	kv, err := client.Inspect(ctx, &pb.InspectRequest{Key: k.Key})
	failOnErr(t, err)
	assertEqual(t, *kv.Locked, true)

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

	kvInfo, err := client.Inspect(ctx, &pb.InspectRequest{Key: kv.Key})
	failOnErr(t, err)
	assertEqual(t, *kvInfo.Locked, true)

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

	// set a key, initially unlocked, with 30min lifespan
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

	srv.reaperMu.RLock()
	keyReaper, hasReaper := srv.reapers[key]
	srv.reaperMu.RUnlock()

	kvData.mu.RLock()
	srv.mu.RUnlock()

	assertEqual(t, hasReaper, true)
	assertEqual(t, keyReaper.Lifespan.Seconds(), expireAfter.Seconds())
	kvData.mu.RUnlock()

	newExpiration := 1 * time.Hour
	kx.Lifespan = durationpb.New(newExpiration)
	_, err = client.Set(ctx, kx)
	failOnErr(t, err)

	kvData.mu.RLock()
	srv.reaperMu.RLock()
	newExpiry, hasReaper := srv.reapers[key]
	assertEqual(t, hasReaper, true)
	assertEqual(t, newExpiry.Lifespan.Seconds(), newExpiration.Seconds())
	srv.reaperMu.RUnlock()
	kvData.mu.RUnlock()
}

func TestKeyLock(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	key := "foo"
	value := []byte("bar")

	// set a key, initially unlocked
	kx := &pb.KeyValue{Key: key, Value: value}
	_, err := client.Set(ctx, kx)
	failOnErr(t, err)

	pk := &pb.Key{Key: key}
	kvInfo, err := client.Inspect(ctx, &pb.InspectRequest{Key: key})
	failOnErr(t, err)
	assertEqual(t, *kvInfo.Locked, false)

	// lock it for 5 seconds
	now := time.Now()
	var lockDuration uint32 = 5
	d := time.Duration(lockDuration) * time.Second
	unlockedAt := now.Add(d)
	lockRequest := &pb.LockRequest{Key: pk.Key, Duration: durationpb.New(d)}
	rv, err := client.Lock(ctx, lockRequest)
	failOnErr(t, err)
	assertEqual(t, rv.Success, true)

	// validate the lock is in place
	srv.mu.RLock()
	keyInfo := srv.store[key]
	keyInfo.mu.Lock()
	srv.mu.RUnlock()
	t.Logf("val: %#v", keyInfo)
	_, locked := srv.locks[key]
	assertEqual(t, locked, true)
	keyInfo.mu.Unlock()

	// shouldn't be able to update a locked key from another client ID
	otherClient := newClient(t, srv, lis, "someotherclientid")
	_, err = otherClient.Set(ctx, kx)
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	// shouldn't be able to delete a locked key from another client ID
	_, err = otherClient.Delete(ctx, &pb.DeleteRequest{Key: key})
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	// should be able to see it
	kvInfo, err = otherClient.Inspect(ctx, &pb.InspectRequest{Key: key})
	failOnErr(t, err)
	assertEqual(t, *kvInfo.Locked, true)

	// unless we request the value from another client
	_, err = otherClient.Inspect(
		ctx,
		&pb.InspectRequest{Key: key, IncludeValue: true},
	)
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	// validate the lock is in place
	srv.mu.RLock()
	keyInfo, ok := srv.store[key]
	keyLock, _ := srv.locks[key]
	srv.mu.RUnlock()
	assertEqual(t, ok, true)

	assertNotEqual(t, keyLock, nil)
	assertEqual(t, keyLock.Created.IsZero(), false)

	// wait for the lock to expire, update it with one that doesn't expire,
	// with an expiration of 1 second
	for n := time.Now(); n.Before(unlockedAt); n = time.Now() {
		time.Sleep(1 * time.Second)
	}

	// set a lock duration 10 secs, lifespan 1 sec, and validate it
	// deletes before the lock is up
	dur := time.Duration(10) * time.Second
	kx.LockDuration = durationpb.New(dur)
	exp := time.Duration(1) * time.Second
	kx.Lifespan = durationpb.New(exp)
	kr, err := client.Set(ctx, kx)
	failOnErr(t, err)
	assertEqual(t, kr.Success, true)

	time.Sleep(3 * time.Second)
	kvInfo, err = client.Inspect(ctx, &pb.InspectRequest{Key: pk.Key})
	assertErrorCode(t, status.Code(err), codes.NotFound)
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

func TestLifespan(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	key := "foo"

	_, err := client.Set(
		ctx,
		&pb.KeyValue{
			Key:      key,
			Value:    []byte("bar"),
			Lifespan: durationpb.New(DefaultMinLifespan),
		},
	)
	fatalOnErr(t, err)

	expCtx, expCancel := context.WithTimeout(
		ctx,
		DefaultMinLifespan*2,
	)
	for {
		if expCtx.Err() != nil {
			t.Fatalf("timed out waiting for key to expire")
		}

		_, err = client.Get(expCtx, &pb.Key{Key: key})
		if err != nil {
			errorCode := status.Code(err)
			if errorCode != codes.NotFound {
				t.Fatalf("expected NotFound, got %s", errorCode.String())
			}
			expCancel()
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func TestLifespanRenew(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	key := "foo"

	_, err := client.Set(
		ctx,
		&pb.KeyValue{
			Key:      key,
			Value:    []byte("bar"),
			Lifespan: durationpb.New(DefaultMinLifespan * 10),
		},
	)
	fatalOnErr(t, err)

	srv.reaperMu.RLock()
	reaper, exists := srv.reapers[key]
	if !exists {
		srv.reaperMu.RUnlock()
		t.Fatalf("expected reaper for key %s", key)
	}

	reaperID := reaper.ID
	srv.reaperMu.RUnlock()

	if reaperID == "" {
		t.Fatalf("expected reaper ID to be set, got %s", reaperID)
	}

	_, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: []byte("baz")})
	fatalOnErr(t, err)

	srv.reaperMu.RLock()
	reaper, exists = srv.reapers[key]

	if !exists {
		srv.reaperMu.RUnlock()
		t.Fatalf("expected reaper for key %s", key)
	}
	assertEqual(t, reaper.renewals.Load(), 1)
	newID := reaper.ID
	srv.reaperMu.RUnlock()
	if newID == reaperID {
		t.Fatalf("expected reaper ID to change, got %s", reaperID)
	}

	_, err = client.Set(
		ctx,
		&pb.KeyValue{
			Key:      key,
			Value:    []byte("bar"),
			Lifespan: durationpb.New(DefaultMinLifespan * 20),
		},
	)
	fatalOnErr(t, err)

	srv.reaperMu.RLock()
	reaper, exists = srv.reapers[key]

	if !exists {
		srv.reaperMu.RUnlock()
		t.Fatalf("expected reaper for key %s", key)
	}
	assertEqual(t, reaper.renewals.Load(), 2)
	lastID := reaper.ID
	srv.reaperMu.RUnlock()
	if lastID == reaperID || lastID == newID {
		t.Fatalf("expected reaper ID to change, got %s", lastID)
	}
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

func TestKeyMetrics(t *testing.T) {
	cfg := NewConfig()
	cfg.LogLevel = "DEBUG"
	cfg.LogEvents = true
	srv, lis := newServer(t, nil, cfg)
	client := newClient(t, srv, lis, "")

	var waitTimer *time.Timer

	key := "foo"
	var err error
	var km *pb.KeyMetric

	setSeen := make(chan struct{}, 2)
	getSeen := make(chan struct{}, 2)
	deleteSeen := make(chan struct{}, 1)
	lockSeen := make(chan struct{}, 1)

	watcherName := "test_watcher"
	eventCh, err := srv.Subscribe(ctx, watcherName, nil, nil)
	fatalOnErr(t, err)

	t.Cleanup(
		func() {
			unsubErr := srv.Unsubscribe(watcherName)
			if unsubErr != nil {
				t.Logf("error unsubscribing: %s", unsubErr.Error())
			}
		},
	)
	go func() {
		for ev := range eventCh {
			t.Logf("saw event: %+v (%d)", ev, ev.Time.UnixNano())
			if ctx.Err() != nil {
				return
			}
			if ev.Key != key {
				continue
			}
			//goland:noinspection GoSwitchMissingCasesForIotaConsts
			switch ev.Event {
			case Created, Updated:
				setSeen <- struct{}{}
			case Accessed:
				getSeen <- struct{}{}
			case Deleted:
				deleteSeen <- struct{}{}
			case Locked:
				lockSeen <- struct{}{}
			}
		}
		t.Logf("event channel closed")
	}()

	srv.eventStream.mu.RLock()
	t.Logf("workers: %+v", srv.eventStream.workers)
	srv.eventStream.mu.RUnlock()

	// set the value, validate set count=1, everything else is zero
	_, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: []byte("bar")})
	fatalOnErr(t, err)
	waitTimer = time.NewTimer(20 * time.Second)
	select {
	case <-setSeen:
	//
	case <-waitTimer.C:
		t.Fatalf("timed out waiting for set event")
	}
	mq := &pb.KeyMetricRequest{Key: key}
	km, err = client.GetKeyMetric(ctx, mq)
	fatalOnErr(t, err)

	assertEqual(t, km.SetCount, 1)
	assertEqual(t, km.FirstSet.Seconds > 0, true)
	assertEqual(
		t,
		km.FirstSet.AsTime().UnixNano(),
		km.LastSet.AsTime().UnixNano(),
	)

	assertEqual(t, km.AccessCount, 0)
	assertEqual(t, km.FirstAccessed, nil)

	assertEqual(t, km.LastAccessed, km.FirstAccessed)

	assertEqual(t, km.LockCount, 0)
	assertEqual(t, km.FirstLocked, nil)
	assertEqual(t, km.FirstLocked, km.LastLocked)

	time.Sleep(1500 * time.Millisecond)

	// inspecting the key with IncludeValue should increment the access counter
	// to 1
	_, err = client.Inspect(
		ctx,
		&pb.InspectRequest{Key: key, IncludeValue: true},
	)

	waitTimer = time.NewTimer(10 * time.Second)
	select {
	case <-getSeen:
	//
	case <-waitTimer.C:
		t.Fatalf("timed out waiting for get event")
	}

	km, err = client.GetKeyMetric(ctx, mq)
	fatalOnErr(t, err)

	assertEqual(t, km.SetCount, 1)
	assertEqual(t, km.FirstSet.Seconds > 0, true)
	assertEqual(t, km.FirstSet.Seconds, km.LastSet.Seconds)

	assertEqual(t, km.AccessCount, 1)
	assertEqual(t, km.FirstAccessed.Seconds > 0, true)
	assertEqual(
		t,
		km.LastAccessed.AsTime().UnixNano(),
		km.FirstAccessed.AsTime().UnixNano(),
	)
	firstMetric := km

	// Getting it should also increment the access counter, to 2
	time.Sleep(1500 * time.Millisecond)
	_, err = client.Get(ctx, &pb.Key{Key: key})
	fatalOnErr(t, err)

	waitTimer = time.NewTimer(10 * time.Second)
	select {
	case <-getSeen:
		//
	case <-waitTimer.C:
		t.Fatalf("timed out waiting for get event")
	}

	km, err = client.GetKeyMetric(ctx, mq)
	fatalOnErr(t, err)

	assertEqual(t, km.SetCount, 1)
	assertEqual(t, km.FirstSet.Seconds > 0, true)
	assertEqual(t, km.FirstSet.Seconds, km.LastSet.Seconds)

	assertEqual(t, km.AccessCount, 2)
	assertEqual(t, km.FirstAccessed.Seconds > 0, true)
	firstAccess := km.FirstAccessed.AsTime().UnixNano()
	lastAccess := km.LastAccessed.AsTime().UnixNano()
	prevMetric := km
	if firstAccess >= lastAccess {
		t.Fatalf(
			"firstAccess >= lastAccess: %d >= %d (%d) \n%s\n%s",
			firstAccess,
			lastAccess,
			firstAccess-lastAccess,
			firstMetric.String(),
			km.String(),
		)
	}

	// Deleting it shouldn't clear the metrics
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: key})
	fatalOnErr(t, err)

	waitTimer = time.NewTimer(10 * time.Second)
	select {
	case <-deleteSeen:
	//
	case <-waitTimer.C:
		t.Fatalf("timed out waiting for delete event")
	}

	km, err = client.GetKeyMetric(ctx, mq)
	fatalOnErr(t, err)

	assertEqual(t, km.SetCount, 1)
	assertEqual(t, km.FirstSet.Seconds > 0, true)
	assertEqual(
		t,
		km.FirstSet.AsTime().UnixNano(),
		km.LastSet.AsTime().UnixNano(),
	)

	assertEqual(t, km.AccessCount, 2)
	assertEqual(t, km.FirstAccessed.Seconds > 0, true)
	assertEqual(
		t,
		km.LastAccessed.AsTime().UnixNano(),
		lastAccess,
		fmt.Sprintf(
			"access times should match (%d)",
			km.LastAccessed.AsTime().UnixNano()-lastAccess,
		),
		fmt.Sprintf("%s\n%s", prevMetric.String(), km.String()),
	)

	// Increment the set counter
	time.Sleep(1500 * time.Millisecond)
	_, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: []byte("baz")})
	fatalOnErr(t, err)

	waitTimer = time.NewTimer(10 * time.Second)
	select {
	case <-setSeen:
	//
	case <-waitTimer.C:
		t.Fatalf("timed out waiting for set event")
	}

	km, err = client.GetKeyMetric(ctx, mq)
	fatalOnErr(t, err)

	assertEqual(t, km.SetCount, 2)
	assertEqual(t, km.FirstSet.Seconds > 0, true)
	if km.FirstSet.AsTime().UnixNano() >= km.LastSet.AsTime().UnixNano() {
		t.Errorf(
			"firstSet >= lastSet: %d >= %d",
			km.FirstSet.AsTime().UnixNano(),
			km.LastSet.AsTime().UnixNano(),
		)
	}

	assertEqual(t, km.AccessCount, 2)
	assertEqual(t, km.FirstAccessed.Seconds > 0, true)
	assertEqual(t, km.LastAccessed.AsTime().UnixNano(), lastAccess)

	firstSet := km.FirstSet.AsTime().UnixNano()
	lastSet := km.LastSet.AsTime().UnixNano()
	if firstSet >= lastSet {
		t.Errorf("firstSet >= lastSet: %d >= %d", firstSet, lastSet)
	}

	// Increment the lock counter
	_, err = client.Lock(
		ctx,
		&pb.LockRequest{Key: key, Duration: durationpb.New(10 * time.Second)},
	)
	fatalOnErr(t, err)

	waitTimer = time.NewTimer(10 * time.Second)
	select {
	case <-lockSeen:
	//
	case <-waitTimer.C:
		t.Fatalf("timed out waiting for lock event")
	}

	km, err = client.GetKeyMetric(ctx, mq)
	fatalOnErr(t, err)

	assertEqual(t, km.SetCount, 2)
	assertEqual(t, km.FirstSet.Seconds > 0, true)

	assertEqual(t, km.LastSet.AsTime().UnixNano(), lastSet)

	assertEqual(t, km.AccessCount, 2)
	assertEqual(t, km.FirstAccessed.Seconds > 0, true)
	assertEqual(t, km.LastAccessed.AsTime().UnixNano(), lastAccess)

	assertEqual(t, km.LockCount, 1)
	assertEqual(t, km.FirstLocked.Seconds > 0, true)
	assertEqual(
		t,
		km.LastLocked.Seconds,
		km.FirstLocked.Seconds,
	)
}

func TestSubscriberLimit(t *testing.T) {
	cfg := NewConfig()
	cfg.EventStreamSubscriberLimit = 3
	srv, lis := newServer(t, nil, cfg)

	srv.cfgMu.RLock()
	assertEqual(t, srv.cfg.EventStreamSubscriberLimit, 3)
	srv.cfgMu.RUnlock()

	clientX := newClient(t, srv, lis, "x")
	clientY := newClient(t, srv, lis, "y")
	clientZ := newClient(t, srv, lis, "z")
	clientOver := newClient(t, srv, lis, "over")
	t.Cleanup(
		func() {
			_ = clientX.CloseConnection()
			_ = clientY.CloseConnection()
			_ = clientZ.CloseConnection()
			_ = clientOver.CloseConnection()
		},
	)

	var err error

	xResult := make(chan *pb.Event, 1)
	yResult := make(chan *pb.Event, 1)
	zResult := make(chan *pb.Event, 1)

	_, err = clientX.Set(ctx, &pb.KeyValue{Key: "foo", Value: []byte("bar")})
	fatalOnErr(t, err)

	// Client X
	xWatch, err := clientX.WatchStream(
		ctx,
		&pb.WatchRequest{Keys: []string{}},
	)
	fatalOnErr(t, err)

	go func() {
		xr, xe := xWatch.Recv()
		fatalOnErr(t, xe)
		t.Logf("got X result %v", xr)
		xResult <- xr
		return
	}()

	// Client Y
	yWatch, err := clientY.WatchStream(
		ctx,
		&pb.WatchRequest{Keys: []string{}},
	)
	fatalOnErr(t, err)

	go func() {
		yr, ye := yWatch.Recv()
		fatalOnErr(t, ye)
		t.Logf("got Y result %v", yr)
		yResult <- yr
		return
	}()

	// Client Z
	zWatch, err := clientZ.WatchStream(
		ctx,
		&pb.WatchRequest{Keys: []string{}},
	)
	fatalOnErr(t, err)

	go func() {
		zr, ze := zWatch.Recv()
		fatalOnErr(t, ze)
		t.Logf("got Z result %v", zr)
		zResult <- zr
		return
	}()

	rctx, rcancel := context.WithTimeout(ctx, 60*time.Second)

	allDone := make(chan struct{}, 1)

	for {
		if rctx.Err() != nil {
			break
		}
		if srv.numEventSubscribers.Load() >= 3 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	go func() {
		defer rcancel()
		_, overErr := clientOver.Set(
			ctx,
			&pb.KeyValue{Key: "foo", Value: []byte("baz")},
		)
		fatalOnErr(t, overErr)
		<-xResult
		t.Logf("got x")
		<-yResult
		t.Logf("got y")
		<-zResult
		t.Logf("got z")
		allDone <- struct{}{}
		return
	}()

	select {
	case <-allDone:
		t.Logf("all watchers reported")
	case <-rctx.Done():
		if errors.Is(rctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("timed out waiting for all events")
		}
	}

	assertEqual(t, srv.numEventSubscribers.Load(), 3)

	// 1 over limit
	// apparently with a stream, the error returned by the server is not
	// the second return value here, but the error returned by Recv()
	finalStream, finalErr := clientOver.WatchStream(
		ctx,
		&pb.WatchRequest{Keys: []string{}},
	)
	fatalOnErr(t, finalErr)

	m, me := finalStream.Recv()
	assertEqual(t, m, nil)
	assertNotNil(t, me)
	assertErrorCode(t, status.Code(me), codes.ResourceExhausted)
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
	fatalOnErr(t, err)
	startKeyCt := uint64(len(startKeys.Keys))
	assertEqual(t, srv.numKeys.Load(), startKeyCt)

	// Set a Key-value pair
	kv, err := client.Set(ctx, &pb.KeyValue{Key: key, Value: value})
	failOnErr(t, err)
	assertEqual(t, kv.IsNew, true)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)

	// Get the value by Key and validate it matches what we set
	resp, err := client.Get(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertSlicesEqual(t, resp.Value, value)

	kvInfo, err := client.Inspect(ctx, &pb.InspectRequest{Key: key})
	failOnErr(t, err)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)
	assertEqual(t, kvInfo.Version, 1) // first version
	assertEqual(t, kvInfo.Size, initialSize)

	// Value should've been hashed
	originalHash := kvInfo.Hash
	assertNotEqual(t, originalHash, 0)

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

	// Update should match our new value
	resp, err = client.Get(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertSlicesEqual(t, resp.Value, newValue)

	srv.mu.RLock()
	sk := srv.store[key]
	sk.mu.RLock()
	sk.mu.RUnlock()
	srv.mu.RUnlock()

	kvInfo, err = client.Inspect(ctx, &pb.InspectRequest{Key: key})
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
	assertNotEqual(t, kvInfo.Hash, 0)

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

	dr, err := client.Delete(ctx, &pb.DeleteRequest{Key: secondKey})
	failOnErr(t, err)
	assertEqual(t, dr.Deleted, true)
	assertEqual(t, srv.numKeys.Load(), 1+startKeyCt)

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
}

func TestDetectContentType(t *testing.T) {
	logo := getGoLogo(t)
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	k := &pb.KeyValue{Key: "logo", Value: logo}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	keyInfo, err := client.Inspect(ctx, &pb.InspectRequest{Key: k.Key})
	failOnErr(t, err)
	assertEqual(t, keyInfo.ContentType, "image/png")
}

func TestGetVersion(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	// Set initial value 'bar', version 1
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

func TestEvents(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")

	xSubscriberName := "x"
	ySubscriberName := "y"
	zSubscriberName := "z"

	x, err := srv.Subscribe(ctx, xSubscriberName, []string{"foo"}, nil)
	failOnErr(t, err)

	y, err := srv.Subscribe(ctx, ySubscriberName, []string{"foo"}, nil)
	failOnErr(t, err)

	z, err := srv.Subscribe(ctx, zSubscriberName, []string{"foo"}, nil)
	failOnErr(t, err)

	xResults := make([]Event, 0, 5)
	yResults := make([]Event, 0, 5)
	zResults := make([]Event, 0, 5)

	wg := &sync.WaitGroup{}

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
				break
			}
			if ctx.Err() != nil {
				t.Error("z timed out")
				return
			}

		}
	}()

	srv.eventStream.mu.RLock()
	t.Logf("workers: %+v", srv.eventStream.workers)
	srv.eventStream.mu.RUnlock()

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

func TestMarshal(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	var originalKeys []string
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
	newStore, err := New(&Config{})
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

	var newKeys []string
	for k := range newStore.store {
		newKeys = append(newKeys, k)
	}
	requireSliceContains(t, newKeys, originalKeys...)
	requireSliceContains(t, originalKeys, newKeys...)
	assertNotNil(t, newStore.logger)

	for keyName, keyInfo := range srv.store {
		t.Logf("checking key: %s", keyName)
		otherKeyInfo := newStore.store[keyName]
		if strings.HasPrefix(keyName, ReservedKeyPrefix) {
			if otherKeyInfo != nil {
				t.Fatalf("key %s found in new store", keyName)
			}
			continue
		}
		if otherKeyInfo == nil {
			t.Fatalf("key %s not found in new store", keyName)
		}
		assertSlicesEqual(t, keyInfo.Value, otherKeyInfo.Value)
		assertEqual(t, keyInfo.Hash, otherKeyInfo.Hash)
		assertEqual(t, keyInfo.Size, otherKeyInfo.Size)
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

		firstStats, hasStat := srv.keyStats[keyName]
		assertEqual(t, hasStat, true)

		otherStats, hasStat := newStore.keyStats[keyName]
		assertEqual(t, hasStat, true)

		assertEqual(t, firstStats.AccessCount, otherStats.AccessCount)
		assertEqual(t, firstStats.LockCount, otherStats.LockCount)
		assertEqual(t, firstStats.SetCount, otherStats.SetCount)
		assertTimesEqual(
			t,
			firstStats.FirstAccessed,
			otherStats.FirstAccessed,
		)
		assertTimesEqual(t, firstStats.FirstLocked, otherStats.FirstLocked)
		assertTimesEqual(t, firstStats.FirstSet, otherStats.FirstSet)
		assertTimesEqual(t, firstStats.LastAccessed, otherStats.LastAccessed)
		assertTimesEqual(t, firstStats.LastLocked, otherStats.LastLocked)
		assertTimesEqual(t, firstStats.LastSet, otherStats.LastSet)
	}
}

func assertTimesEqual(t *testing.T, t1 *time.Time, expected *time.Time) {
	t.Helper()
	if t1 == nil && expected == nil {
		return
	}
	if t1 == nil && expected != nil {
		t.Fatalf("expected '%s', got nil", expected)
	}
	if t1 != nil && expected == nil {
		t.Fatalf("expected nil, got '%s'", t1)
	}
	pt1 := *t1
	pexpect := *expected
	if !pt1.Equal(pexpect) {
		t.Errorf("expected '%s', got '%s;", expected, t1)
	}
}

func TestGetRevision(t *testing.T) {
	cfg := NewConfig()
	cfg.RevisionLimit = 10

	srv, lis := newServer(t, nil, cfg)
	client := newClient(t, srv, lis, "")

	key := "foo"

	revisions := map[int]*pb.KeyValue{}

	for i := 1; i < int(cfg.RevisionLimit+2); i++ {
		kv := &pb.KeyValue{Key: key, Value: []byte(fmt.Sprintf("bar-%d", i))}
		_, err := client.Set(ctx, kv)
		fatalOnErr(t, err)
	}

	_, e := client.GetRevision(
		ctx,
		&pb.GetRevisionRequest{Key: key, Version: 0},
	)
	if e == nil {
		t.Fatalf("expected error, got nil")
	}
	assertErrorCode(t, status.Code(e), codes.InvalidArgument)

	for rev, k := range revisions {
		resp, err := client.GetRevision(
			ctx,
			&pb.GetRevisionRequest{Key: key, Version: int64(rev)},
		)
		fatalOnErr(t, err)
		assertSlicesEqual(t, resp.Value, k.Value)
	}

	kv := &pb.KeyValue{Key: key, Value: []byte("bar-final")}
	_, err := client.Set(ctx, kv)
	fatalOnErr(t, err)
	_, err = client.GetRevision(
		ctx,
		&pb.GetRevisionRequest{Key: key, Version: 1},
	)
	if err == nil {
		srv.mu.RLock()
		kvInfo := srv.store[key]
		srv.mu.RUnlock()
		kvInfo.mu.RLock()

		currentVersion := kvInfo.Version
		srv.hmu.RLock()
		revs := len(srv.history[kvInfo.Key])
		var allVersions []string
		for _, v := range srv.history[kvInfo.Key] {
			allVersions = append(allVersions, fmt.Sprintf("%d", v.Version))
		}
		kvInfo.mu.RUnlock()
		srv.hmu.RUnlock()
		t.Fatalf(
			"expected error, got nil (current version: %d, history length: %d, all versions: %s)",
			currentVersion,
			revs,
			strings.Join(allVersions, ", "),
		)
	}
	assertErrorCode(t, status.Code(err), codes.NotFound)

}

func TestPersistentHistory(t *testing.T) {
	var revisionLimit int64 = 3
	srv, lis := newServer(
		t, nil, &Config{
			RevisionLimit:             revisionLimit,
			PersistentRevisions:       true,
			KeepKeyHistoryAfterDelete: true,
		},
	)
	client := newClient(t, srv, lis, "")

	key := "foo"

	// initial value should have version 1 and immediately go into the history
	_, err := client.Set(ctx, &pb.KeyValue{Key: key, Value: []byte("bar")})
	fatalOnErr(t, err)

	srv.hmu.RLock()
	history, exists := srv.history[key]
	if !exists {
		t.Fatalf("expected history for key '%s'", key)
	}
	expectedHistoryCount := 1
	assertEqual(t, len(history), expectedHistoryCount)
	assertEqual(t, cap(history), int(revisionLimit))
	assertEqual(t, history[0].Version, 1)
	srv.hmu.RUnlock()

	insp, err := client.Inspect(ctx, &pb.InspectRequest{Key: key})
	fatalOnErr(t, err)
	assertEqual(t, insp.Version, 1)

	// updating the value should increment to 2, also go into the history
	_, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: []byte("baz")})
	fatalOnErr(t, err)
	expectedHistoryCount++

	insp, err = client.Inspect(ctx, &pb.InspectRequest{Key: key})
	fatalOnErr(t, err)
	assertEqual(t, insp.Version, 2)

	srv.hmu.RLock()
	history, exists = srv.history[key]
	if !exists {
		t.Fatalf("expected history for key '%s'", key)
	}
	assertEqual(t, len(history), expectedHistoryCount)
	assertEqual(t, cap(history), int(revisionLimit))

	mostRecentVersion := history[len(history)-1].Version
	assertEqual(t, mostRecentVersion, insp.Version)

	srv.hmu.RUnlock()

	// deleting the value should retain the history entry
	_, err = client.Delete(ctx, &pb.DeleteRequest{Key: key})
	fatalOnErr(t, err)

	srv.hmu.RLock()
	history, exists = srv.history[key]
	if !exists {
		t.Fatalf("expected history for key '%s'", key)
	}
	assertEqual(t, len(history), expectedHistoryCount)
	assertEqual(t, cap(history), int(revisionLimit))
	mostRecentVersion = history[len(history)-1].Version
	assertEqual(t, mostRecentVersion, 2)
	srv.hmu.RUnlock()

	// setting the value again should be 'new' but the version should
	// reflect the most recent version in the history + 1
	_, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: []byte("baz")})
	fatalOnErr(t, err)
	expectedHistoryCount++

	insp, err = client.Inspect(ctx, &pb.InspectRequest{Key: key})
	fatalOnErr(t, err)
	assertEqual(t, insp.Version, 3)

	srv.hmu.RLock()
	history, exists = srv.history[key]
	if !exists {
		t.Fatalf("expected history for key '%s'", key)
	}
	assertEqual(t, len(history), expectedHistoryCount)
	assertEqual(t, cap(history), int(revisionLimit))
	mostRecentVersion = history[len(history)-1].Version
	assertEqual(t, mostRecentVersion, 3)

	srv.hmu.RUnlock()
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
func failOnErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%v", err)
	}
}

func fatalOnErr(t testing.TB, err error, msg ...string) {
	t.Helper()
	if err != nil {
		t.Fatalf("error: %s / %s", err.Error(), strings.Join(msg, " ... "))
	}
}

func assertEqual[V comparable](t testing.TB, val V, expected V, msg ...string) {
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

func assertNotEqual[V comparable](t testing.TB, val V, expected V) {
	t.Helper()
	if val == expected {
		t.Errorf(
			"expected:\n%#v\n\ngot:\n%#v",
			expected,
			val,
		)
	}
}

func assertSlicesEqual[V comparable](t testing.TB, value []V, expected []V) {
	t.Helper()
	if len(value) != len(expected) {
		t.Fatalf(
			"expected %d elements, got %d\n%+v\n%+v",
			len(expected),
			len(value),
			value,
			expected,
		)
	}

	for i, v := range value {
		if v != expected[i] {
			t.Errorf(
				"index %d: expected:\n%+v\n\ngot:\n%+v",
				i,
				expected,
				value,
			)
		}
	}
}

func requireSliceContains[V comparable](
	t testing.TB,
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

func assertSliceContains[V comparable](
	t testing.TB, value []V, expected ...V,
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
		t.Errorf(
			"expected:\n%#v\n\nnot found in:\n%#v", expected, value,
		)
	}
}

func assertSliceContainsOneOf[V comparable](
	t testing.TB,
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
	t testing.TB,
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

func getGoLogo(t testing.TB) []byte {
	t.Helper()
	f := filepath.Join("testdata", "go.png")
	file, err := os.ReadFile(f)
	if err != nil {
		t.Fatalf("unable to read file: %s: %s", f, err.Error())
	}
	return file
}

func assertNotNil(t testing.TB, v any) {
	t.Helper()
	if v == nil {
		t.Fatalf("expected non-nil value")

	}
}

func BenchmarkSetKey(b *testing.B) {
	srv, lis := newServer(b, nil, nil)
	client := newClient(b, srv, lis, "")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := client.Set(
			ctx,
			&pb.KeyValue{Key: fmt.Sprintf("foo-%d", i), Value: []byte("bar")},
		)
		fatalOnErr(b, err)
	}
}

func BenchmarkLockKey(b *testing.B) {
	srv, lis := newServer(b, nil, nil)
	client := newClient(b, srv, lis, "")

	for i := 0; i < b.N; i++ {
		_, err := client.Set(
			ctx,
			&pb.KeyValue{Key: fmt.Sprintf("foo-%d", i), Value: []byte("bar")},
		)
		fatalOnErr(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := client.Lock(
			ctx,
			&pb.LockRequest{
				Key:      fmt.Sprintf("foo-%d", i),
				Duration: durationpb.New(10 * time.Second),
			},
		)
		fatalOnErr(b, err)
	}
}

func TestGetServerMetric(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	_ = newClient(t, srv, lis, "")
	m := srv.GetStats()
	assertNotNil(t, m)

	data, err := json.Marshal(m)
	fatalOnErr(t, err)
	t.Logf("stats: %s", string(data))
}

func TestStopExpirationTimers(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	var keys []string
	lifespan := 10 * time.Minute
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("foo-%d", i)
		// keys = append(keys, k)
		keys = append(keys, k)
		_, err := client.Set(
			ctx, &pb.KeyValue{
				Key:      k,
				Value:    []byte("bar"),
				Lifespan: durationpb.New(lifespan),
			},
		)
		fatalOnErr(t, err)
	}
	assertEqual(t, srv.numReapers.Load(), 10)

	srv.reaperMu.RLock()
	var reaperKeys []string
	var reapers []*reaper
	assertEqual(t, len(srv.reapers), 10)

	for r := range srv.reapers {
		reaperKeys = append(reaperKeys, r)
		reapers = append(reapers, srv.reapers[r])
		assertNotNil(t, srv.reapers[r].t)
	}

	for _, k := range keys {
		assertSliceContains(t, reaperKeys, k)
	}
	srv.reaperMu.RUnlock()
	srv.reaperMu.Lock()
	srv.stopExpirationTimers()
	srv.reaperMu.Unlock()

	srv.reaperMu.RLock()
	assertEqual(t, len(srv.reapers), 10)
	assertEqual(t, srv.numReapers.Load(), 10)
	srv.reaperMu.RUnlock()

	for _, r := range reapers {
		assertEqual(t, r.t, nil)
	}
}

func TestStopUnlockTimers(t *testing.T) {
	srv, lis := newServer(t, nil, nil)
	client := newClient(t, srv, lis, "")
	var keys []string
	lockDuration := 10 * time.Minute
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("foo-%d", i)
		// keys = append(keys, k)
		keys = append(keys, k)
		_, err := client.Set(
			ctx, &pb.KeyValue{
				Key:          k,
				Value:        []byte("bar"),
				LockDuration: durationpb.New(lockDuration),
			},
		)
		fatalOnErr(t, err)
	}
	assertEqual(t, srv.numLocks.Load(), 10)

	srv.lockMu.RLock()
	var lockKeys []string
	var locks []*kvLock
	assertEqual(t, len(srv.locks), 10)

	for r := range srv.locks {
		lockKeys = append(lockKeys, r)
		locks = append(locks, srv.locks[r])
		assertNotNil(t, srv.locks[r].t)
	}

	for _, k := range keys {
		assertSliceContains(t, lockKeys, k)
	}
	srv.lockMu.RUnlock()
	srv.lockMu.Lock()
	srv.stopUnlockTimers()
	srv.lockMu.Unlock()

	srv.lockMu.RLock()
	assertEqual(t, len(srv.locks), 10)
	assertEqual(t, srv.numLocks.Load(), 10)
	srv.lockMu.RUnlock()

	for _, r := range locks {
		assertEqual(t, r.t, nil)
	}
}
