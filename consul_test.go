package consul

import (
	"context"
	"testing"
	"time"

	"github.com/kvtools/valkeyrie"
	"github.com/kvtools/valkeyrie/store"
	"github.com/kvtools/valkeyrie/testsuite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTimeout = 60 * time.Second

const client = "localhost:8500"

func makeConsulClient(t *testing.T) store.Store {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	config := &Config{
		ConnectionTimeout: 3 * time.Second,
	}

	kv, err := New(ctx, []string{client}, config)
	require.NoErrorf(t, err, "cannot create store")

	return kv
}

func TestRegister(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	kv, err := valkeyrie.NewStore(ctx, StoreName, []string{client}, nil)
	require.NoError(t, err)
	assert.NotNil(t, kv)

	assert.IsTypef(t, kv, new(Store), "Error registering and initializing consul")
}

func TestConsulStore(t *testing.T) {
	kv := makeConsulClient(t)
	lockKV := makeConsulClient(t)
	ttlKV := makeConsulClient(t)

	t.Cleanup(func() {
		testsuite.RunCleanup(t, kv)
	})

	testsuite.RunCleanup(t, kv)
	testsuite.RunTestCommon(t, kv)
	testsuite.RunTestAtomic(t, kv)
	testsuite.RunTestWatch(t, kv)
	testsuite.RunTestLock(t, kv)
	testsuite.RunTestLockTTL(t, kv, lockKV)
	testsuite.RunTestTTL(t, kv, ttlKV)
}

func TestGetActiveSession(t *testing.T) {
	kv := makeConsulClient(t)

	assert.IsTypef(t, kv, new(Store), "Error registering and initializing consul")

	consul, ok := kv.(*Store)
	require.True(t, ok)

	key := "foo"
	value := []byte("bar")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Put the first key with the Ephemeral flag.
	err := kv.Put(ctx, key, value, &store.WriteOptions{TTL: 2 * time.Second})
	require.NoError(t, err)

	// Session should not be empty.
	session, err := consul.getActiveSession(key)
	require.NoError(t, err)
	assert.NotEmpty(t, session)

	// Delete the key.
	err = kv.Delete(ctx, key)
	require.NoError(t, err)

	// Check the session again, it should return nothing.
	session, err = consul.getActiveSession(key)
	require.NoError(t, err)
	assert.Empty(t, session)
}
