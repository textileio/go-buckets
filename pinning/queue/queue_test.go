package queue_test

import (
	"context"
	"errors"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	logging "github.com/ipfs/go-log/v2"
	mbase "github.com/multiformats/go-multibase"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
	. "github.com/textileio/go-buckets/pinning/queue"
	"github.com/textileio/go-threads/util"
)

func init() {
	if err := util.SetLogLevels(map[string]logging.LogLevel{
		"buckets/ps-queue": logging.LevelDebug,
	}); err != nil {
		panic(err)
	}
}

func TestQueue_NewID(t *testing.T) {
	//assert.NotEmpty(t, NewID())
}

func TestQueue_ListRequests(t *testing.T) {
	q := newQueue(t)

	limit := 100
	now := time.Now()
	key := newBucketkey(t)
	ids := make([]string, limit)
	for i := 0; i < limit; i++ {
		now = now.Add(time.Second)
		p := newParams(key, now, time.Millisecond, succeed)
		r, err := q.AddRequest(p)
		require.NoError(t, err)
		ids[i] = r.Requestid
	}

	time.Sleep(time.Second) // wait for all to finish

	// Listing from another key should return 0 results
	l, err := q.ListRequests(newBucketkey(t), Query{})
	require.NoError(t, err)
	assert.Len(t, l, 0)

	// Using before and after should error
	l, err = q.ListRequests(key, Query{Before: "foo", After: "bar"})
	require.Error(t, err)

	// Empty query, should return oldest 10 records
	l, err = q.ListRequests(key, Query{})
	require.NoError(t, err)
	assert.Len(t, l, 10)
	assert.Equal(t, ids[0], l[0].Requestid)
	assert.Equal(t, ids[9], l[9].Requestid)

	// Get next page, should return next 10 older records
	l, err = q.ListRequests(key, Query{After: l[len(l)-1].Requestid})
	require.NoError(t, err)
	assert.Len(t, l, 10)
	assert.Equal(t, ids[10], l[0].Requestid)
	assert.Equal(t, ids[19], l[9].Requestid)

	// Get previous page, should return the first page in reverse order
	l, err = q.ListRequests(key, Query{Before: l[0].Requestid})
	require.NoError(t, err)
	assert.Len(t, l, 10)
	assert.Equal(t, ids[0], l[9].Requestid)
	assert.Equal(t, ids[9], l[0].Requestid)

	// Create more request with multiple statuses
	now = time.Now()
	key = newBucketkey(t)
	ids = make([]string, limit)
	var sids, fids []string
	for i := 0; i < limit; i++ {
		now = now.Add(time.Second)
		var o outcomeType
		if i%2 != 0 {
			o = succeed
		} else {
			o = fail
		}
		p := newParams(key, now, time.Millisecond, o)
		r, err := q.AddRequest(p)
		require.NoError(t, err)
		if i%2 != 0 {
			o = succeed
			sids = append(sids, r.Requestid)
		} else {
			o = fail
			fids = append(fids, r.Requestid)
		}
		ids[i] = r.Requestid
	}

	time.Sleep(time.Second) // wait for all to finish

	// List first page of all request statuses, ensure entire order is maintained
	l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.PINNED, openapi.FAILED}})
	require.NoError(t, err)
	assert.Len(t, l, 10)
	for i := 0; i < len(l); i++ {
		assert.Equal(t, ids[i], l[i].Requestid)
	}

	// List only "pinned" statuses
	l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.PINNED}})
	require.NoError(t, err)
	assert.Len(t, l, 10)
	assert.Equal(t, sids[0], l[0].Requestid)
	assert.Equal(t, sids[9], l[9].Requestid)

	// List only "failed" statuses
	l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.FAILED}})
	require.NoError(t, err)
	assert.Len(t, l, 10)
	assert.Equal(t, fids[0], l[0].Requestid)
	assert.Equal(t, fids[9], l[9].Requestid)
}

func TestQueue_AddRequest(t *testing.T) {
	q := newQueue(t)

	p := newParams(newBucketkey(t), time.Now(), time.Millisecond, succeed)
	r, err := q.AddRequest(p)
	require.NoError(t, err)

	// Allow to finish
	time.Sleep(time.Millisecond * 10)

	s, err := q.GetRequest(p.Key, r.Requestid)
	require.NoError(t, err)
	assert.Equal(t, openapi.PINNED, s.Status)
}

func TestQueue_RemoveRequest(t *testing.T) {
	q := newQueue(t)

	p := newParams(newBucketkey(t), time.Now(), time.Millisecond, succeed)
	r, err := q.AddRequest(p)
	require.NoError(t, err)

	// Request will skip status "queued" and go straight to "pinning" since
	// there is no backlog of work. That means we can't remove it until it's
	// "pinned" or "failed"
	err = q.RemoveRequest(p.Key, r.Requestid)
	require.Error(t, err)

	// Allow to finish
	time.Sleep(time.Millisecond * 10)

	err = q.RemoveRequest(p.Key, r.Requestid)
	require.NoError(t, err)

	_, err = q.GetRequest(p.Key, r.Requestid)
	require.Error(t, err)
}

func TestQueueProcessing(t *testing.T) {
	q := newQueue(t)

	limit := 500
	now := time.Now()
	key1 := newBucketkey(t)
	for i := 0; i < limit; i++ {
		now = now.Add(time.Second)
		var o outcomeType
		if i%10 != 0 {
			o = succeed
		} else {
			o = fail
		}
		p := newParams(key1, now, time.Millisecond*100, o)
		_, err := q.AddRequest(p)
		require.NoError(t, err)
	}

	time.Sleep(time.Second * 5) // wait for all to finish

	l, err := q.ListRequests(key1, Query{
		Statuses: []openapi.Status{openapi.PINNING, openapi.QUEUED},
		Limit:    limit,
	})
	require.NoError(t, err)
	assert.Len(t, l, 0) // zero should be queued

	l, err = q.ListRequests(key1, Query{
		Statuses: []openapi.Status{openapi.PINNED},
		Limit:    limit,
	})
	require.NoError(t, err)
	assert.Len(t, l, 450) // expected amount should be pinned

	l, err = q.ListRequests(key1, Query{
		Statuses: []openapi.Status{openapi.FAILED},
		Limit:    limit,
	})
	require.NoError(t, err)
	assert.Len(t, l, 50) // expected amount should be failed
}

func newQueue(t *testing.T) *Queue {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	s, err := util.NewBadgerDatastore(dir, "pinq")
	require.NoError(t, err)
	q, err := NewQueue(s, handler)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, q.Close())
		require.NoError(t, s.Close())
	})
	return q
}

func handler(_ context.Context, r Request) error {
	d, t := parseOutcome(r.Pin.Cid)
	time.Sleep(d)
	if t == succeed {
		return nil
	} else {
		return errors.New("bummer")
	}
}

type outcomeType string

const (
	succeed outcomeType = "success"
	fail                = "failure"
)

func newOutcome(d time.Duration, t outcomeType) string {
	return strings.Join([]string{d.String(), string(t)}, ",")
}

func parseOutcome(o string) (time.Duration, outcomeType) {
	parts := strings.Split(o, ",")
	d, _ := time.ParseDuration(parts[0])
	return d, outcomeType(parts[1])
}

func newParams(k string, t time.Time, d time.Duration, o outcomeType) RequestParams {
	return RequestParams{
		Pin: openapi.Pin{
			Cid: newOutcome(d, o),
		},
		Time: t,
		Key:  k,
	}
}

func newBucketkey(t *testing.T) string {
	k, err := mbase.Encode(mbase.Base36, util.GenerateRandomBytes(20))
	require.NoError(t, err)
	return k
}
