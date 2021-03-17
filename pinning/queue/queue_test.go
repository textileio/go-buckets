package queue_test

//
//import (
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//	. "github.com/textileio/go-buckets/ipns/store"
//	"github.com/textileio/go-threads/core/thread"
//	"github.com/textileio/go-threads/db"
//)
//
//func TestStore_Create(t *testing.T) {
//	ds := db.NewTxMapDatastore()
//	defer ds.Close()
//	store := NewStore(ds)
//
//	err := store.Create("foo", "cid", thread.NewRandomIDV1())
//	require.NoError(t, err)
//}
//
//func TestStore_Get(t *testing.T) {
//	ds := db.NewTxMapDatastore()
//	defer ds.Close()
//	store := NewStore(ds)
//
//	threadID := thread.NewRandomIDV1()
//	err := store.Create("foo", "cid", threadID)
//	require.NoError(t, err)
//
//	key, err := store.Get("foo")
//	require.NoError(t, err)
//	assert.Equal(t, "foo", key.Name)
//	assert.Equal(t, "cid", key.Cid)
//	assert.Equal(t, threadID, key.ThreadID)
//}
//
//func TestStore_GetByCid(t *testing.T) {
//	ds := db.NewTxMapDatastore()
//	defer ds.Close()
//	store := NewStore(ds)
//
//	threadID := thread.NewRandomIDV1()
//	err := store.Create("foo", "cid", threadID)
//	require.NoError(t, err)
//
//	key, err := store.GetByCid("cid")
//	require.NoError(t, err)
//	assert.Equal(t, "foo", key.Name)
//	assert.Equal(t, "cid", key.Cid)
//	assert.Equal(t, threadID, key.ThreadID)
//}
//
//func TestStore_Delete(t *testing.T) {
//	ds := db.NewTxMapDatastore()
//	defer ds.Close()
//	store := NewStore(ds)
//
//	err := store.Create("foo", "cid", thread.NewRandomIDV1())
//	require.NoError(t, err)
//
//	err = store.Delete("foo")
//	require.NoError(t, err)
//	_, err = store.Get("foo")
//	require.Error(t, err)
//}
