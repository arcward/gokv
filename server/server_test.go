package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/arcward/gokv/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	pb "github.com/arcward/gokv/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener
var srv *KeyValueStore
var s *grpc.Server
var signals = make(chan os.Signal, 1)
var ctx context.Context
var cancel context.CancelFunc

func init() {
	log.SetOutput(io.Discard)
	lis = bufconn.Listen(bufSize)
	if s == nil {
		s = grpc.NewServer()
	}

	ctx, cancel = context.WithCancel(context.Background())
	if srv == nil {
		srv = NewServer(
			Config{
				Hashing:       HashConfig{Enabled: true},
				RevisionLimit: 2,
			},
		)
	}

	pb.RegisterKeyValueStoreServer(s, srv)
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
	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()
}

func useServerConfig(t *testing.T, cfg Config) {
	t.Helper()
	originalConfig := &srv.cfg
	srv.cfg = cfg
	t.Cleanup(
		func() {
			srv.cfg = *originalConfig
		},
	)
}

func newClient(t *testing.T) *client.Client {
	t.Helper()

	tctx, tcancel := context.WithTimeout(ctx, 30*time.Second)
	t.Cleanup(
		func() {
			tcancel()
		},
	)
	t.Cleanup(
		func() {
			srv.clear()
			srv.resetStats()
		},
	)
	client := client.NewClient(
		client.ClientConfig{
			NoTLS:   true,
			Address: "bufnet",
		}, grpc.WithContextDialer(bufDialer),
	)
	//conn, err := grpc.DialContext(
	//	tctx,
	//	"bufnet",
	//	grpc.WithContextDialer(bufDialer),
	//	grpc.WithInsecure(),
	//)
	err := client.Dial()
	if err != nil {
		t.Fatalf("Failed to dial bufnet: %v", err)
	}

	//client := pb.NewKeyValueStoreClient(conn)

	go func() {
		select {
		case <-signals:
			panic("interrupted")
		case <-tctx.Done():
			if e := tctx.Err(); errors.Is(e, context.DeadlineExceeded) {
				panic("timeout exceeded")
			}
		case <-ctx.Done():
			panic("done called")
			tcancel()
		}
	}()
	return client
}

func bufDialer(context.Context, string) (net.Conn, error) {
	return lis.Dial()
}

func benchmarkFailOnErr(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatalf("error: %s", err.Error())
	}
}

func BenchmarkKeyValueStore(b *testing.B) {
	b.Cleanup(
		func() {
			srv.clear()
			srv.resetStats()
		},
	)

	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithInsecure(),
	)
	if err != nil {
		b.Fatalf("Failed to dial bufnet: %v", err)
	}

	client := pb.NewKeyValueStoreClient(conn)

	numKeys := 1000
	keys := make([]string, numKeys, numKeys)

	for i := 0; i < numKeys; i++ {
		kv := &pb.KeyValue{
			Key:      fmt.Sprintf("key-%d", i),
			Value:    []byte(fmt.Sprintf("value-%d", i)),
			ExpireIn: 9,
		}
		keys[i] = kv.Key
		_, _ = client.Set(
			ctx, kv,
		)
	}

	if srv.numKeys.Load() != uint64(numKeys) {
		b.Fatalf("expected %d keys, got %d", numKeys, srv.numKeys.Load())
	}

	time.Sleep(8 * time.Second)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := []byte(fmt.Sprintf("value-%d", i))
		_, err := client.Set(
			ctx, &pb.KeyValue{
				Key:   key,
				Value: value,
			},
		)
		benchmarkFailOnErr(b, err)

		//_, err = client.Set(
		//	ctx,
		//	&pb.KeyValue{
		//		Key:   key,
		//		Value: []byte(string(value) + "-ext"),
		//	},
		//)
		//benchmarkFailOnErr(b, err)

	}

}

//
//func TestBigStuff(t *testing.T) {
//	t.Skip()
//	client := newClient(t)
//
//	keys := make([]string, 50000, 50000)
//
//	for i := 0; i < 50000; i++ {
//		kv := &pb.KeyValue{
//			Key:   fmt.Sprintf("key-%d", i),
//			Value: []byte(fmt.Sprintf("value-%d", i)),
//		}
//		keys[i] = kv.Key
//		_, _ = client.Set(
//			ctx, kv,
//		)
//	}
//
//	if srv.numKeys.Load() != 50000 {
//		t.Fatalf("expected 50000 keys, got %d", srv.numKeys.Load())
//	}
//
//	updates := make([]*pb.KeyValue, 50000, 50000)
//
//	for i := 0; i < 50000; i++ {
//		key := fmt.Sprintf("key-%d", i)
//		value := []byte(fmt.Sprintf("value-%d", i))
//		updates[i] = &pb.KeyValue{
//			Key:   key,
//			Value: value,
//		}
//	}
//
//	for _, tc := range updates {
//		tc = tc
//		_, err := client.Set(ctx, tc)
//		failOnErr(t, err)
//		tc.Value = []byte(string(tc.Value) + "-ext")
//		//&pb.KeyValue{Key: tc.Key, Value: []byte(string(tc.Value) + "-ext")}
//		_, err = client.Set(ctx, tc)
//		failOnErr(t, err)
//
//		_, err = client.Append(ctx, tc)
//		failOnErr(t, err)
//
//		_, err = client.Delete(ctx, &pb.Key{Key: tc.Key})
//		failOnErr(t, err)
//	}
//	//for _, tc := range updates {
//	//	tc = tc
//	//	t.Run(
//	//		tc.Key, func(t *testing.T) {
//	//			t.Parallel()
//	//			_, err := client.Set(ctx, tc)
//	//			failOnErr(t, err)
//	//		},
//	//	)
//	//}
//
//}

func TestListKeys(t *testing.T) {
	client := newClient(t)

	firstKey := "SomeKeyHere"
	secondKey := "AnotherKeyHere"
	kx := &pb.KeyValue{Key: firstKey, Value: []byte("foo")}
	ky := &pb.KeyValue{Key: secondKey, Value: []byte("bar")}

	// Set a key-value pair
	_, err := client.Set(ctx, kx)
	failOnErr(t, err)
	_, err = client.Set(ctx, ky)
	failOnErr(t, err)

	keys, err := client.ListKeys(ctx, &pb.ListKeysRequest{})
	failOnErr(t, err)
	assertSliceContains(t, keys.Keys, firstKey, secondKey)
	assertEqual(t, len(keys.Keys), 2)

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

func TestGetExpiredKey(t *testing.T) {
	client := newClient(t)

	var seconds uint32 = 1
	expireAfter := time.Duration(seconds+1) * time.Second

	kv := &pb.KeyValue{
		Key:      "foo",
		Value:    []byte("bar"),
		ExpireIn: seconds,
	}
	rv, err := client.Set(ctx, kv)
	failOnErr(t, err)
	assertEqual(t, rv.Success, true)
	assertEqual(t, rv.IsNew, true)

	time.Sleep(expireAfter)

	_, err = client.Get(ctx, &pb.Key{Key: kv.Key})
	assertNotNil(t, err)
	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	assertErrorCode(t, e.Code(), codes.NotFound)

}

func TestKeyHash(t *testing.T) {
	client := newClient(t)

	key := string(make([]byte, DefaultMaxKeySize*2, DefaultMaxKeySize*2))
	//value := make([]byte, DefaultMaxValueSize*2, DefaultMaxValueSize*2)
	var value []byte
	kx := &pb.KeyValue{Key: key, Value: value}

	_, err := client.Set(ctx, kx)
	assertNotNil(t, err)
	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	assertErrorCode(t, e.Code(), codes.FailedPrecondition)
}

func TestKeyNotFound(t *testing.T) {
	client := newClient(t)
	_, err := client.GetKeyInfo(ctx, &pb.Key{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestKeyHistory(t *testing.T) {
	originalHistoryConfig := srv.cfg.RevisionLimit
	t.Cleanup(
		func() {
			srv.cfg.RevisionLimit = originalHistoryConfig
		},
	)
	var newRevisionLimit int64 = 3
	srv.cfg.RevisionLimit = newRevisionLimit

	client := newClient(t)

	k := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	originalValue := k.Value
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	keyInfo, ok := srv.store[k.Key]
	assertEqual(t, ok, true)
	assertEqual(t, len(keyInfo.History), 0)
	assertEqual(t, int64(cap(keyInfo.History)), newRevisionLimit)

	versions := map[int][]byte{}
	versions[0] = originalValue

	for i := 0; i < int(newRevisionLimit); i++ {
		newValue := []byte(fmt.Sprintf("%s-%d", string(originalValue), i+1))
		k.Value = newValue
		_, err = client.Set(ctx, k)
		failOnErr(t, err)
	}
	assertEqual(t, len(keyInfo.History), int(newRevisionLimit))

	firstRevision := keyInfo.History[0]
	assertEqual(t, firstRevision.Version, 0)
	assertSlicesEqual(t, firstRevision.Value, originalValue)
	assertEqual(t, firstRevision.Updated.IsZero(), true)

	var previousRevision *KeyValueSnapshot

	for ind, rev := range keyInfo.History {
		rev := rev
		assertEqual(t, rev.Version, uint64(ind))

		assertEqual(t, rev.Timestamp.IsZero(), false)
		if previousRevision != nil {
			assertEqual(t, rev.Version, previousRevision.Version+1)
			assertEqual(t, rev.Updated.IsZero(), false)
			val := []byte(fmt.Sprintf("%s-%d", string(originalValue), ind))
			assertSlicesEqual(t, rev.Value, val)
		}
		previousRevision = rev
	}
	currentVal, err := client.Get(ctx, &pb.Key{Key: k.Key})
	failOnErr(t, err)

	k.Value = []byte("final")
	_, err = client.Set(ctx, k)
	failOnErr(t, err)

	assertEqual(t, len(keyInfo.History), int(newRevisionLimit))
	for i := 0; i < len(keyInfo.History); i++ {
		assertNotEqual(t, keyInfo.History[i], firstRevision)
	}
	assertSlicesEqual(
		t,
		keyInfo.History[len(keyInfo.History)-1].Value,
		currentVal.Value,
	)
	assertEqual(t, int64(cap(keyInfo.History)), newRevisionLimit)

	_, err = client.ClearHistory(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, len(keyInfo.History), 0)
}

func TestLockUnknownKey(t *testing.T) {
	client := newClient(t)

	_, err := client.Lock(ctx, &pb.LockRequest{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestUnlockUnknownKey(t *testing.T) {
	client := newClient(t)

	_, err := client.Unlock(ctx, &pb.UnlockRequest{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.NotFound)
}

func TestUpdateWithLockDuraton(t *testing.T) {
	client := newClient(t)
	k := &pb.KeyValue{Key: "foo"}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	k.Lock = true
	k.LockDuration = 10

	_, err = client.Set(ctx, k)
	failOnErr(t, err)

	kv, err := client.GetKeyInfo(ctx, &pb.Key{Key: k.Key})
	failOnErr(t, err)
	assertEqual(t, kv.Locked, true)

	keyInfo, ok := srv.store[k.Key]
	assertEqual(t, ok, true)

	triggered := keyInfo.unlockTimer.Stop()
	assertEqual(t, triggered, true)

}

func TestKeyAlreadyLocked(t *testing.T) {
	client := newClient(t)

	k := &pb.KeyValue{Key: "foo", Lock: true}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	_, err = client.Lock(ctx, &pb.LockRequest{Key: "foo"})
	assertNotNil(t, err)
	e, _ := status.FromError(err)
	assertErrorCode(t, e.Code(), codes.FailedPrecondition)
}

func TestKeyLock(t *testing.T) {
	client := newClient(t)

	key := "foo"
	value := []byte("bar")

	// set a key, initially unlocked
	kx := &pb.KeyValue{Key: key, Value: value}
	_, err := client.Set(ctx, kx)
	failOnErr(t, err)

	pk := &pb.Key{Key: key}
	kvInfo, err := client.GetKeyInfo(ctx, pk)
	failOnErr(t, err)

	assertEqual(t, kvInfo.Locked, false)

	// lock it for 5 seconds
	//now := time.Now()
	var lockDuration uint32 = 5
	lockRequest := &pb.LockRequest{Key: pk.Key, Duration: lockDuration}
	rv, err := client.Lock(ctx, lockRequest)
	failOnErr(t, err)
	assertEqual(t, rv.Success, true)

	srv.mu.RLock()
	keyInfo := srv.store[key]
	keyInfo.mu.Lock()
	srv.mu.RUnlock()
	assertEqual(t, keyInfo.IsLocked(), true)
	keyInfo.mu.Unlock()

	// shouldn't be able to update a Locked key
	_, err = client.Set(ctx, kx)
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	// shouldn't be able to delete a Locked key
	_, err = client.Delete(ctx, &pb.Key{Key: key})
	assertErrorCode(t, status.Code(err), codes.PermissionDenied)

	kvInfo, err = client.GetKeyInfo(ctx, pk)
	failOnErr(t, err)
	assertEqual(t, kvInfo.Locked, true)

	srv.mu.RLock()
	keyInfo, ok := srv.store[key]
	srv.mu.RUnlock()

	assertEqual(t, ok, true)
	assertEqual(t, keyInfo.LockedAt.IsZero(), false)

	// sleep until unlocked
	time.Sleep(keyInfo.lockDuration + 1*time.Second)

	kx.Value = []byte("baz")
	kx.Lock = false
	kx.LockDuration = 0

	// should be unlocked, able to be updated
	// we update it with a lock that doesn't expire
	setResponse, err := client.Set(ctx, kx)
	failOnErr(t, err)
	assertEqual(t, setResponse.Success, true)
	assertEqual(t, setResponse.IsNew, false)
	keyInfo.mu.Lock()
	assertEqual(t, keyInfo.IsLocked(), false)
	keyInfo.mu.Unlock()
	// set the expiration for 1 second, then wait for it to expire
	_, err = client.Delete(ctx, &pb.Key{Key: key})
	failOnErr(t, err)

	kx.LockDuration = 10
	kx.Lock = true
	kx.ExpireIn = 1
	_, err = client.Set(ctx, kx)
	failOnErr(t, err)

	time.Sleep(3 * time.Second)
	kvInfo, err = client.GetKeyInfo(ctx, pk)
	failOnErr(t, err)

	newKeyInfo := srv.store[key]
	assertNotEqual(t, keyInfo, newKeyInfo)
	keyInfo = newKeyInfo
	// normally, the expiration is checked when getting a key, and if it's
	// expired, it's deleted and we get nothing back. in this case, the lock
	// should prevent that from happening, and we should get the key back
	assertEqual(t, kvInfo.Locked, true)
	assertEqual(t, keyInfo.IsExpired(), true)
	assertEqual(t, keyInfo.IsLocked(), true)

	// a Locked key should be immune from clearing the store
	clearResponse, err := client.Clear(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, clearResponse.Success, true)
	assertEqual(t, clearResponse.KeysDeleted, 0)

	// explicitly unlock it
	unlockResponse, err := client.Unlock(ctx, &pb.UnlockRequest{Key: key})
	failOnErr(t, err)
	assertEqual(t, unlockResponse.Success, true)
	assertEqual(t, keyInfo.IsLocked(), false)
	assertEqual(t, keyInfo.IsExpired(), true)

	kx.ExpireIn = 0
	kx.Lock = false
	_, err = client.Set(ctx, kx)
	failOnErr(t, err)
	// clearing the store should now delete the expired, unlocked key
	clearResponse, err = client.Clear(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, clearResponse.Success, true)
	assertEqual(t, clearResponse.KeysDeleted, 1)

	// verify it was removed from the store
	_, ok = srv.store[key]
	assertEqual(t, ok, false)
}

func TestKeyValueStore(t *testing.T) {
	client := newClient(t)

	var numKeys uint64 = 0
	var totalSize uint64 = 0
	var getRequests uint64 = 0
	var setRequests uint64 = 0
	var deleteRequests uint64 = 0
	var newKeysSet uint64 = 0
	var keysUpdated uint64 = 0
	var getKeyInfoRequests uint64 = 0
	var listKeysRequests uint64 = 0
	var popRequests uint64 = 0
	var existsRequests uint64 = 0

	key := "testKey"
	secondKey := "bar"
	value := []byte("testValue")
	initialSize := uint64(len(value))

	assertEqual(t, srv.numKeys.Load(), 0)

	// Set a key-value pair
	kv, err := client.Set(ctx, &pb.KeyValue{Key: key, Value: value})
	failOnErr(t, err)
	assertEqual(t, kv.IsNew, true)
	assertEqual(t, srv.numKeys.Load(), 1)
	assertEqual(t, srv.TotalSize(), initialSize)
	assertEqual(t, srv.store[key].Accessed.IsZero(), true)
	newKeysSet++
	setRequests++

	// Get the value by key and validate it matches what we set
	resp, err := client.Get(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertSlicesEqual(t, resp.Value, value)
	getRequests++

	kvInfo, err := client.GetKeyInfo(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertEqual(t, srv.numKeys.Load(), 1)
	assertEqual(t, kvInfo.Version, 0) // first version
	assertEqual(t, kvInfo.Size, initialSize)
	assertNotEqual(t, kvInfo.Accessed, 0)
	originalAccessed := kvInfo.Accessed
	getKeyInfoRequests++

	// Value should've been hashed
	originalHash := kvInfo.Hash
	assertNotEqual(t, originalHash, "")

	// Created timestamp should've been populated
	created := time.Unix(kvInfo.Created, 0)
	assertEqual(t, created.IsZero(), false)
	assertEqual(t, kvInfo.Updated, 0) // no updates yet

	// update the value
	newValue := []byte("foo")
	secondSize := uint64(len(newValue))
	kv, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: newValue})
	failOnErr(t, err)
	assertEqual(t, kv.IsNew, false)
	assertEqual(t, srv.numKeys.Load(), 1)
	assertEqual(t, srv.TotalSize(), secondSize)
	setRequests++
	keysUpdated++

	// Update should match our new value
	resp, err = client.Get(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertSlicesEqual(t, resp.Value, newValue)
	secondAccessed := srv.store[key].Accessed.UnixNano()
	assertGreaterOrEqual(t, secondAccessed, originalAccessed)
	getRequests++

	kvInfo, err = client.GetKeyInfo(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertEqual(t, kvInfo.Version, 1)    // second version
	assertNotEqual(t, kvInfo.Updated, 0) // should've been set
	assertLessThanOrEqualTo(t, kvInfo.Created, kvInfo.Updated)
	assertEqual(t, kvInfo.Size, secondSize)
	assertNotEqual(t, originalHash, kvInfo.Hash)
	assertNotEqual(t, kvInfo.Hash, "")
	getKeyInfoRequests++

	updated := time.Unix(kvInfo.Updated, 0)
	assertEqual(t, updated.IsZero(), false)

	var nonNegative bool
	if updated.Unix() > 0 {
		nonNegative = true
	}
	assertEqual(t, nonNegative, true)

	// add a new key, value
	_, err = client.Set(ctx, &pb.KeyValue{Key: secondKey, Value: value})
	failOnErr(t, err)
	setRequests++
	newKeysSet++

	assertEqual(t, srv.numKeys.Load(), 2)
	updatedSize := uint64(len(value)) + secondSize
	assertEqual(t, srv.TotalSize(), updatedSize)

	dr, err := client.Delete(ctx, &pb.Key{Key: secondKey})
	failOnErr(t, err)
	assertEqual(t, dr.Deleted, true)
	assertEqual(t, srv.numKeys.Load(), 1)
	assertEqual(t, srv.TotalSize(), secondSize)
	deleteRequests++

	popResp, err := client.Pop(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	assertEqual(t, kv.IsNew, false)
	assertSlicesEqual(t, popResp.Value, newValue)
	popRequests++

	_, err = client.Get(ctx, &pb.Key{Key: key})
	assertNotNil(t, err)
	e, ok := status.FromError(err)
	assertEqual(t, ok, true)
	assertErrorCode(t, e.Code(), codes.NotFound)
	getRequests++

	_, err = client.Set(ctx, &pb.KeyValue{Key: key, Value: value})
	failOnErr(t, err)
	assertEqual(t, srv.numKeys.Load(), 1)
	setRequests++
	newKeysSet++

	clearResp, err := client.Clear(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, clearResp.Success, true)
	assertEqual(t, clearResp.KeysDeleted, 1)
	assertEqual(t, srv.numKeys.Load(), 0)

	stats, err := client.Stats(ctx, &pb.EmptyRequest{})
	failOnErr(t, err)
	assertEqual(t, *stats.Keys, numKeys)
	assertEqual(t, *stats.TotalSize, totalSize)
	assertEqual(t, *stats.GetRequests, getRequests)
	assertEqual(t, *stats.SetRequests, setRequests)
	assertEqual(t, *stats.DeleteRequests, deleteRequests)
	assertEqual(t, *stats.NewKeysSet, newKeysSet)
	assertEqual(t, *stats.KeysUpdated, keysUpdated)
	assertEqual(t, *stats.GetKeyInfoRequests, getKeyInfoRequests)
	assertEqual(t, *stats.ListKeysRequests, listKeysRequests)
	assertEqual(t, *stats.PopRequests, popRequests)
	assertEqual(t, *stats.ExistsRequests, existsRequests)
}

func TestJSONThing(t *testing.T) {
	client := newClient(t)
	key := "testKey"
	value := []byte("123456")

	_, err := client.Set(ctx, &pb.KeyValue{Key: key, Value: value})
	failOnErr(t, err)

	kvInfo, err := client.GetKeyInfo(ctx, &pb.Key{Key: key})
	failOnErr(t, err)

	data, err := json.MarshalIndent(kvInfo, "", "  ")
	failOnErr(t, err)
	fmt.Printf("result:\n%s\n", string(data))

	val, err := client.Get(ctx, &pb.Key{Key: key})
	failOnErr(t, err)
	data, err = json.MarshalIndent(val, "", "  ")
	fmt.Printf("result:\n%s\n", string(data))
}

func TestDetectContentType(t *testing.T) {
	logo := getGoLogo(t)
	client := newClient(t)
	k := &pb.KeyValue{Key: "logo", Value: logo}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	keyInfo, err := client.GetKeyInfo(ctx, &pb.Key{Key: k.Key})
	failOnErr(t, err)
	assertEqual(t, keyInfo.ContentType, "image/png")
}

func TestGetVersion(t *testing.T) {

	client := newClient(t)
	k := &pb.KeyValue{Key: "foo", Value: []byte("bar")}
	_, err := client.Set(ctx, k)
	failOnErr(t, err)

	rev, err := client.GetRevision(ctx, &pb.GetRevisionRequest{Key: "foo"})
	failOnErr(t, err)

	assertSlicesEqual(t, k.Value, rev.Value)

	_, err = client.Set(ctx, &pb.KeyValue{Key: "foo", Value: []byte("baz")})
	failOnErr(t, err)

	rev, err = client.GetRevision(
		ctx,
		&pb.GetRevisionRequest{Key: "foo", Version: 1},
	)
	failOnErr(t, err)
	assertSlicesEqual(t, rev.Value, []byte("baz"))
	assertNotNil(t, rev.Timestamp)

	rev, err = client.GetRevision(
		ctx,
		&pb.GetRevisionRequest{Key: "foo", Version: 0},
	)
	failOnErr(t, err)
	assertSlicesEqual(t, rev.Value, []byte("bar"))
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

func TestRestore(t *testing.T) {
	tempServer := NewServer(Config{})
	assertEqual(t, len(tempServer.store), 0)
	f := filepath.Join("testdata", "restore.json")
	file, err := os.ReadFile(f)
	failOnErr(t, err)
	err = tempServer.Restore(file)
	failOnErr(t, err)

	assertEqual(t, len(tempServer.store), 2)
	fmt.Printf("%#v\n", tempServer.store)

	kv := tempServer.store["foo"]
	failOnErr(t, err)
	fmt.Printf("%#v\n", kv)
	assertSlicesEqual(t, kv.Value, []byte("bar"))
	kv = tempServer.store["bar"]
	failOnErr(t, err)
	assertSlicesEqual(t, kv.Value, []byte("baz"))
}

func TestMarshal(t *testing.T) {
	client := newClient(t)
	originalKeys := []string{}
	for i := 0; i < 50; i++ {
		k := &pb.KeyValue{
			Key:   fmt.Sprintf("key-%d", i),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		if i > 25 && i < 30 {
			k.Lock = true
			k.LockDuration = 3600
		}
		originalKeys = append(originalKeys, k.Key)
		_, err := client.Set(ctx, k)
		failOnErr(t, err)
	}

	data, err := json.MarshalIndent(srv, "", "  ")
	failOnErr(t, err)

	newStore := NewServer(Config{Hashing: HashConfig{Enabled: true}})
	err = json.Unmarshal(data, newStore)
	failOnErr(t, err)

	newKeys := []string{}
	for k := range newStore.store {
		newKeys = append(newKeys, k)
	}
	assertSliceContains(t, newKeys, originalKeys...)
	assertSliceContains(t, originalKeys, newKeys...)
	assertNotNil(t, newStore.logger)

	for keyName, keyInfo := range srv.store {
		otherKeyInfo := newStore.store[keyName]
		assertSlicesEqual(t, keyInfo.Value, otherKeyInfo.Value)
		assertEqual(t, keyInfo.Hash, otherKeyInfo.Hash)
		assertEqual(t, keyInfo.Size, otherKeyInfo.Size)
		assertEqual(t, keyInfo.Locked, otherKeyInfo.Locked)
	}
}

func TestMaxValueSize(t *testing.T) {
	client := newClient(t)
	newMaxSize := uint64(5)
	useServerConfig(t, Config{MaxValueSize: newMaxSize})

	assertEqual(t, srv.cfg.MaxValueSize, newMaxSize)

	key := "testKey"
	value := []byte("123456")

	exists, _ := client.Exists(ctx, &pb.Key{Key: key})
	assertEqual(t, exists.Exists, false)

	// Set a key-value pair
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
		t.Errorf("%v", err)
	}
}

func assertEqual[V comparable](t *testing.T, val V, expected V) {
	t.Helper()
	if val != expected {
		t.Errorf(
			"expected:\n%+v\n\ngot:\n%+v",
			expected,
			val,
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

func assertErrorCode(t *testing.T, code codes.Code, expectedCode codes.Code) {
	t.Helper()
	if code != expectedCode {
		t.Fatalf(
			"expected:\n%#v\n\ngot:\n%#v",
			expectedCode,
			code,
		)
	}
}

func assertNotNil(t *testing.T, v any) {
	t.Helper()
	if v == nil {
		t.Fatalf("expected non-nil value")

	}
}
