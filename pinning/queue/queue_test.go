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
	t.Parallel()
	q := newQueue(t)

	// Ensure monotonic
	var last string
	for i := 0; i < 10000; i++ {
		id, err := q.NewID(time.Now())
		require.NoError(t, err)

		if i > 0 {
			assert.Greater(t, id, last)
		}
		last = id
	}
}

func TestQueue_ListRequests(t *testing.T) {
	t.Parallel()
	q := newQueue(t)

	t.Run("pagination", func(t *testing.T) {
		limit := 100
		now := time.Now()
		key := newBucketkey(t)
		ids := make([]string, limit)
		for i := 0; i < limit; i++ {
			now = now.Add(time.Millisecond * 10)
			p := newParams(key, now, time.Millisecond, succeed)
			r, err := q.AddRequest(p)
			require.NoError(t, err)
			ids[i] = r.Requestid
		}

		time.Sleep(time.Second) // wait for all to finish

		// Listing from another key should return 0 results
		l, err := q.ListRequests(newBucketkey(t), Query{Statuses: []openapi.Status{openapi.PINNED}})
		require.NoError(t, err)
		assert.Len(t, l, 0)

		// Using before and after should error
		l, err = q.ListRequests(key, Query{Before: time.Now(), After: time.Now()})
		require.Error(t, err)

		// Empty query, should return newest 10 records
		l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.PINNED}})
		require.NoError(t, err)
		assert.Len(t, l, 10)
		assert.Equal(t, ids[limit-1], l[0].Requestid)
		assert.Equal(t, ids[limit-10], l[9].Requestid)

		// Get next page, should return next 10 records
		before := l[len(l)-1].Created
		l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.PINNED}, Before: before})
		require.NoError(t, err)
		assert.Len(t, l, 10)
		assert.Equal(t, ids[limit-11], l[0].Requestid)
		assert.Equal(t, ids[limit-20], l[9].Requestid)

		// Get previous page, should return the first page in reverse order
		after := l[0].Created
		l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.PINNED}, After: after})
		require.NoError(t, err)
		assert.Len(t, l, 10)
		assert.Equal(t, ids[limit-10], l[0].Requestid)
		assert.Equal(t, ids[limit-1], l[9].Requestid)
	})

	t.Run("filter by cids", func(t *testing.T) {
		// This is a bit awkward since these tests use the openapi.Pin.Cid field to encode
		// how long the mock handler should take and whether or not it should succeed.
		// That said, the mechanism is still being tested: Match a string.

		limit := 10
		now := time.Now()
		key := newBucketkey(t)
		ids := make([]string, limit)
		for i := 0; i < limit; i++ {
			now = now.Add(time.Second)
			p := newParams(key, now, time.Duration(i*1000), succeed)
			r, err := q.AddRequest(p)
			require.NoError(t, err)
			ids[i] = r.Requestid
		}

		time.Sleep(time.Second) // wait for all to finish

		l, err := q.ListRequests(key, Query{Cids: []string{
			newOutcome(time.Duration(0), succeed),
		}})
		require.NoError(t, err)
		assert.Len(t, l, 1)
		assert.Equal(t, ids[0], l[0].Requestid)

		l, err = q.ListRequests(key, Query{Cids: []string{
			newOutcome(time.Duration(1000), succeed),
			newOutcome(time.Duration(4000), succeed),
			newOutcome(time.Duration(7000), succeed),
		}})
		require.NoError(t, err)
		assert.Len(t, l, 3)
		assert.Equal(t, ids[7], l[0].Requestid)
		assert.Equal(t, ids[4], l[1].Requestid)
		assert.Equal(t, ids[1], l[2].Requestid)
	})

	t.Run("filter by name", func(t *testing.T) {
		now := time.Now()
		key := newBucketkey(t)

		p := newParams(key, now, time.Millisecond, succeed)
		p.Name = "My Pin"
		_, err := q.AddRequest(p)
		require.NoError(t, err)

		p = newParams(key, now.Add(time.Second), time.Millisecond, succeed)
		p.Name = "Your Pin"
		_, err = q.AddRequest(p)
		require.NoError(t, err)

		time.Sleep(time.Second) // wait for all to finish

		// Case-sensitive exact match
		l, err := q.ListRequests(key, Query{Name: "My Pin"})
		require.NoError(t, err)
		assert.Len(t, l, 1)

		// Case-insensitive exact match
		l, err = q.ListRequests(key, Query{Name: "MY pin", Match: openapi.IEXACT})
		require.NoError(t, err)
		assert.Len(t, l, 1)

		// Case-sensitive partial match
		l, err = q.ListRequests(key, Query{Name: "Pin", Match: openapi.PARTIAL})
		require.NoError(t, err)
		assert.Len(t, l, 2)

		// Case-insensitive partial match
		l, err = q.ListRequests(key, Query{Name: "pin", Match: openapi.IPARTIAL})
		require.NoError(t, err)
		assert.Len(t, l, 2)

		// No match
		l, err = q.ListRequests(key, Query{Name: "Their Pin"})
		require.NoError(t, err)
		assert.Len(t, l, 0)
	})

	t.Run("filter by status", func(t *testing.T) {
		limit := 100
		now := time.Now()
		key := newBucketkey(t)
		ids := make([]string, limit)
		sids := make([]string, 0)
		fids := make([]string, 0)
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
		l, err := q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.PINNED, openapi.FAILED}})
		require.NoError(t, err)
		assert.Len(t, l, 10)
		for i := 0; i < len(l); i++ {
			assert.Equal(t, ids[limit-(i+1)], l[i].Requestid)
		}

		// List only "pinned" statuses
		l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.PINNED}})
		require.NoError(t, err)
		assert.Len(t, l, 10)
		assert.Equal(t, sids[limit/2-1], l[0].Requestid)
		assert.Equal(t, sids[limit/2-10], l[9].Requestid)

		// List only "failed" statuses
		l, err = q.ListRequests(key, Query{Statuses: []openapi.Status{openapi.FAILED}})
		require.NoError(t, err)
		assert.Len(t, l, 10)
		assert.Equal(t, fids[limit/2-1], l[0].Requestid)
		assert.Equal(t, fids[limit/2-10], l[9].Requestid)
	})

	t.Run("filter by meta", func(t *testing.T) {
		now := time.Now()
		key := newBucketkey(t)

		p := newParams(key, now, time.Millisecond, succeed)
		p.Meta = map[string]string{
			"app": "angry dogs",
			"dog": "eddy",
		}
		_, err := q.AddRequest(p)
		require.NoError(t, err)

		p = newParams(key, now.Add(time.Second), time.Millisecond, succeed)
		p.Meta = map[string]string{
			"app": "angry dogs",
			"dog": "clyde",
		}
		_, err = q.AddRequest(p)
		require.NoError(t, err)

		time.Sleep(time.Second) // wait for all to finish

		// No keys provided
		l, err := q.ListRequests(key, Query{Meta: map[string]string{}})
		require.NoError(t, err)
		assert.Len(t, l, 2)

		// Match one key
		l, err = q.ListRequests(key, Query{Meta: map[string]string{
			"app": "angry dogs",
		}})
		require.NoError(t, err)
		assert.Len(t, l, 2)

		// Match two keys
		l, err = q.ListRequests(key, Query{Meta: map[string]string{
			"app": "angry dogs",
			"dog": "eddy",
		}})
		require.NoError(t, err)
		assert.Len(t, l, 1)

		// Partial match should not work
		l, err = q.ListRequests(key, Query{Meta: map[string]string{
			"app": "angry dogs",
			"dog": "biff",
		}})
		require.NoError(t, err)
		assert.Len(t, l, 0)

		// No key matches
		l, err = q.ListRequests(key, Query{Meta: map[string]string{
			"weather": "nice",
		}})
		require.NoError(t, err)
		assert.Len(t, l, 0)

		// No value matches
		l, err = q.ListRequests(key, Query{Meta: map[string]string{
			"app": "angry cats",
		}})
		require.NoError(t, err)
		assert.Len(t, l, 0)
	})
}

func TestQueue_AddRequest(t *testing.T) {
	t.Parallel()
	q := newQueue(t)

	p := newParams(newBucketkey(t), time.Now(), time.Millisecond, succeed)
	r, err := q.AddRequest(p)
	require.NoError(t, err)
	assert.Equal(t, openapi.QUEUED, r.Status)

	// Allow to finish
	time.Sleep(time.Millisecond * 10)

	got, err := q.GetRequest(p.Key, r.Requestid)
	require.NoError(t, err)
	assert.Equal(t, openapi.PINNED, got.Status)
}

func TestQueue_ReplaceRequest(t *testing.T) {
	t.Parallel()
	q := newQueue(t)

	p := newParams(newBucketkey(t), time.Now(), time.Millisecond, fail)
	r, err := q.AddRequest(p)
	require.NoError(t, err)

	newPin := openapi.Pin{
		Cid: newOutcome(time.Millisecond, succeed),
	}

	// Request will skip status "queued" and go straight to "pinning" since
	// there is no backlog of work. That means we can't replace it until it's
	// "pinned" or "failed"
	_, err = q.ReplaceRequest(p.Key, r.Requestid, newPin)
	require.Error(t, err)

	// Allow to finish
	time.Sleep(time.Millisecond * 10)

	got, err := q.GetRequest(p.Key, r.Requestid)
	require.NoError(t, err)
	assert.Equal(t, openapi.FAILED, got.Status)

	_, err = q.ReplaceRequest(p.Key, r.Requestid, newPin)
	require.NoError(t, err)

	// Allow to finish
	time.Sleep(time.Millisecond * 10)

	got, err = q.GetRequest(p.Key, r.Requestid)
	require.NoError(t, err)
	assert.Equal(t, openapi.PINNED, got.Status)
}

func TestQueue_RemoveRequest(t *testing.T) {
	t.Parallel()
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

	// Allow to finish
	time.Sleep(time.Millisecond * 10)

	_, err = q.GetRequest(p.Key, r.Requestid)
	require.Error(t, err)
}

func TestQueueProcessing(t *testing.T) {
	t.Parallel()
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
