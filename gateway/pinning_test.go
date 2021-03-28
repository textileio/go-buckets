package gateway

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"sync/atomic"
	"testing"
	"time"

	ipfsfiles "github.com/ipfs/go-ipfs-files"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	logging "github.com/ipfs/go-log/v2"
	psc "github.com/ipfs/go-pinning-service-http-client"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p-core/crypto"
	maddr "github.com/multiformats/go-multiaddr"
	"github.com/oklog/ulid/v2"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/apitest"
	"github.com/textileio/go-buckets/api/common"
	"github.com/textileio/go-buckets/cmd"
	"github.com/textileio/go-buckets/ipns"
	"github.com/textileio/go-buckets/pinning"
	"github.com/textileio/go-buckets/pinning/queue"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	"github.com/textileio/go-threads/core/thread"
	tdb "github.com/textileio/go-threads/db"
	nc "github.com/textileio/go-threads/net/api/client"
	"github.com/textileio/go-threads/util"
	"golang.org/x/sync/errgroup"
)

var (
	origins   []maddr.Multiaddr
	statusAll = []psc.Status{psc.StatusQueued, psc.StatusPinning, psc.StatusPinned, psc.StatusFailed}
)

func init() {
	if err := util.SetLogLevels(map[string]logging.LogLevel{
		"buckets":          logging.LevelDebug,
		"buckets/ps":       logging.LevelDebug,
		"buckets/ps-queue": logging.LevelDebug,
		"buckets/gateway":  logging.LevelDebug,
	}); err != nil {
		panic(err)
	}
	queue.MaxConcurrency = 10 // Reduce concurrency to test overloading workers
	pinning.PinTimeout = time.Second * 5
}

func TestMain(m *testing.M) {
	cleanup := func() {}
	if os.Getenv("SKIP_SERVICES") != "true" {
		cleanup = apitest.StartServices()
	}

	var err error
	origins, err = getOrigins()
	if err != nil {
		log.Fatalf("failed to get ipfs node origins: %v", err)
	}

	exitVal := m.Run()
	cleanup()
	os.Exit(exitVal)
}

func Test_ListPins(t *testing.T) {
	gw := newGateway(t)

	t.Run("pagination", func(t *testing.T) {
		numBatches := 5
		batchSize := 20 // Must be an even number
		total := numBatches * batchSize

		files := make([]path.Resolved, total)
		for i := 0; i < total; i++ {
			files[i] = createIpfsFile(t, i%(batchSize/2) == 0) // Two per batch should fail (blocks unavailable)
			log.Debugf("created file %d", i)
		}

		// Blast a bunch of requests. Each batch hits a different bucket.
		var done int32
		clients := make([]*psc.Client, numBatches)
		for b := 0; b < numBatches; b++ {
			c := newClient(t, gw) // New client and bucket
			clients[b] = c
			go func(c *psc.Client, b int) {
				eg, gctx := errgroup.WithContext(context.Background())
				for i := 0; i < batchSize; i++ {
					i := i
					j := i + (b * batchSize)
					f := files[j]
					time.Sleep(time.Second)
					eg.Go(func() error {
						if gctx.Err() != nil {
							return nil
						}
						_, err := c.Add(gctx, f.Cid(), psc.PinOpts.WithOrigins(origins...))
						atomic.AddInt32(&done, 1)
						return err
					})
				}
				err := eg.Wait()
				require.NoError(t, err)
			}(c, b)
		}

		time.Sleep(time.Second * 25) // Allow time for requests to be added

		// Test pagination
		for _, c := range clients {
			// Get new page
			res1, err := c.LsSync(context.Background(),
				psc.PinOpts.FilterStatus(statusAll...),
				psc.PinOpts.Limit(batchSize/2),
			)
			require.NoError(t, err)
			assert.Len(t, res1, batchSize/2)

			// Get next newest
			res2, err := c.LsSync(context.Background(),
				psc.PinOpts.FilterStatus(statusAll...),
				psc.PinOpts.FilterBefore(res1[len(res1)-1].GetCreated()),
				psc.PinOpts.Limit(batchSize/2),
			)
			require.NoError(t, err)
			assert.Len(t, res2, batchSize/2)

			// Ensure order is decending
			all := append(res1, res2...)
			for i := 0; i < len(res1)-1; i++ {
				assert.Greater(t, ulid.Timestamp(all[i].GetCreated()), ulid.Timestamp(all[i+1].GetCreated()))
			}

			// Get oldest page in reverse
			res3, err := c.LsSync(context.Background(),
				psc.PinOpts.FilterStatus(statusAll...),
				psc.PinOpts.FilterAfter(time.Now().Add(-time.Hour)), // Far back in the past
				psc.PinOpts.Limit(batchSize/2),
			)
			require.NoError(t, err)
			assert.Len(t, res3, batchSize/2)

			// Get next oldest (first page in reverse)
			res4, err := c.LsSync(context.Background(),
				psc.PinOpts.FilterStatus(statusAll...),
				psc.PinOpts.FilterAfter(res3[len(res3)-1].GetCreated()),
				psc.PinOpts.Limit(batchSize/2),
			)
			require.NoError(t, err)
			assert.Len(t, res4, batchSize/2)

			// Ensure order is ascending
			all = append(res3, res4...)
			for i := 0; i < len(all)-1; i++ {
				assert.Less(t, ulid.Timestamp(all[i].GetCreated()), ulid.Timestamp(all[i+1].GetCreated()))
			}
		}

		// Wait for all to complete
		assert.Eventually(t, func() bool {
			// Check if all requests have been sent
			if atomic.LoadInt32(&done) != int32(total) {
				return false
			}
			// Check if all request have completed
			for _, c := range clients {
				res, err := c.LsSync(context.Background(),
					psc.PinOpts.FilterStatus(psc.StatusQueued, psc.StatusPinning),
					psc.PinOpts.Limit(batchSize),
				)
				require.NoError(t, err)
				if len(res) != 0 {
					return false
				}
			}
			return true
		}, time.Minute*10, time.Second*5)

		// Test expected status counts
		for _, c := range clients {
			res, err := c.LsSync(context.Background(),
				psc.PinOpts.FilterStatus(psc.StatusPinned),
				psc.PinOpts.Limit(batchSize),
			)
			require.NoError(t, err)
			assert.Len(t, res, batchSize-2)

			res, err = c.LsSync(context.Background(),
				psc.PinOpts.FilterStatus(psc.StatusFailed),
				psc.PinOpts.Limit(batchSize),
			)
			require.NoError(t, err)
			assert.Len(t, res, 2)
		}
	})

	// A comprehensive filter test is in pinning/queue/queue_test.go.
	// Here we just make sure the basics are functional.
	t.Run("filters", func(t *testing.T) {
		c := newClient(t, gw)

		file1 := createIpfsFile(t, false)
		file2 := createIpfsFile(t, false)
		file3 := createIpfsFile(t, true)

		_, err := c.Add(
			context.Background(),
			file1.Cid(),
			psc.PinOpts.WithOrigins(origins...),
			psc.PinOpts.WithName("one"),
			psc.PinOpts.AddMeta(map[string]string{
				"color": "blue",
			}),
		)
		require.NoError(t, err)

		_, err = c.Add(
			context.Background(),
			file2.Cid(),
			psc.PinOpts.WithOrigins(origins...),
			psc.PinOpts.WithName("two"),
			psc.PinOpts.AddMeta(map[string]string{
				"name": "mike",
			}),
		)
		require.NoError(t, err)

		time.Sleep(time.Second * 10) // Allow to succeed

		// No cid match
		res, err := c.LsSync(context.Background(),
			psc.PinOpts.FilterStatus(psc.StatusPinned),
			psc.PinOpts.FilterCIDs(file3.Cid()),
		)
		require.NoError(t, err)
		assert.Len(t, res, 0)

		// Cid match
		res, err = c.LsSync(context.Background(),
			psc.PinOpts.FilterStatus(psc.StatusPinned),
			psc.PinOpts.FilterCIDs(file1.Cid()),
		)
		require.NoError(t, err)
		assert.Len(t, res, 1)

		// No name match
		res, err = c.LsSync(context.Background(),
			psc.PinOpts.FilterStatus(psc.StatusPinned),
			psc.PinOpts.FilterName("three"),
		)
		require.NoError(t, err)
		assert.Len(t, res, 0)

		// Name match
		res, err = c.LsSync(context.Background(),
			psc.PinOpts.FilterStatus(psc.StatusPinned),
			psc.PinOpts.FilterName("one"),
		)
		require.NoError(t, err)
		assert.Len(t, res, 1)

		// No meta match
		res, err = c.LsSync(context.Background(),
			psc.PinOpts.FilterStatus(psc.StatusPinned),
			psc.PinOpts.LsMeta(map[string]string{
				"color": "red",
				"name":  "joe",
			}),
		)
		require.NoError(t, err)
		assert.Len(t, res, 0)

		// Meta match
		res, err = c.LsSync(context.Background(),
			psc.PinOpts.FilterStatus(psc.StatusPinned),
			psc.PinOpts.LsMeta(map[string]string{
				"name": "mike",
			}),
		)
		require.NoError(t, err)
		assert.Len(t, res, 1)
	})
}

func Test_AddPin(t *testing.T) {
	gw := newGateway(t)
	c := newClient(t, gw)

	t.Run("add unavailable pin should fail", func(t *testing.T) {
		t.Parallel()
		folder := createIpfsFolder(t, true)
		res, err := c.Add(context.Background(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
		require.NoError(t, err)
		assert.NotEmpty(t, res.GetRequestId())
		assert.NotEmpty(t, res.GetCreated())
		assert.Equal(t, psc.StatusQueued, res.GetStatus())

		time.Sleep(time.Second * 10) // Allow to fail

		res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
		require.NoError(t, err)
		assert.Equal(t, psc.StatusFailed, res.GetStatus())
	})

	t.Run("add available pin should succeed", func(t *testing.T) {
		t.Parallel()
		folder := createIpfsFolder(t, false)
		res, err := c.Add(context.Background(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
		require.NoError(t, err)
		assert.NotEmpty(t, res.GetRequestId())
		assert.NotEmpty(t, res.GetCreated())
		assert.Equal(t, psc.StatusQueued, res.GetStatus())

		time.Sleep(time.Second * 10) // Allow to succeed

		res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
		require.NoError(t, err)
		assert.Equal(t, psc.StatusPinned, res.GetStatus())
		assert.True(t, res.GetPin().GetCid().Equals(folder.Cid()))
	})
}

func Test_GetPin(t *testing.T) {
	gw := newGateway(t)
	c := newClient(t, gw)

	t.Run("get nonexistent pin should fail", func(t *testing.T) {
		t.Parallel()
		_, err := c.GetStatusByID(context.Background(), ulid.MustNew(123, rand.Reader).String())
		require.Error(t, err)
	})

	t.Run("get bad pin should report correct status at each stage", func(t *testing.T) {
		t.Parallel()
		folder := createIpfsFolder(t, true)
		res, err := c.Add(context.Background(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
		require.NoError(t, err)
		assert.Equal(t, psc.StatusQueued, res.GetStatus())

		assert.Eventually(t, func() bool {
			res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
			require.NoError(t, err)
			return res.GetStatus() == psc.StatusPinning
		}, time.Second*10, time.Millisecond*200)

		assert.Eventually(t, func() bool {
			res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
			require.NoError(t, err)
			return res.GetStatus() == psc.StatusFailed
		}, time.Second*10, time.Millisecond*200)
	})

	t.Run("get good pin should report correct status at each stage", func(t *testing.T) {
		t.Parallel()
		folder := createIpfsFolder(t, false)
		res, err := c.Add(context.Background(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
		require.NoError(t, err)
		assert.Equal(t, psc.StatusQueued, res.GetStatus())

		assert.Eventually(t, func() bool {
			res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
			require.NoError(t, err)
			return res.GetStatus() == psc.StatusPinning
		}, time.Second*10, time.Millisecond*200)

		assert.Eventually(t, func() bool {
			res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
			require.NoError(t, err)
			return res.GetStatus() == psc.StatusPinned
		}, time.Second*10, time.Millisecond*200)
	})
}

func Test_ReplacePin(t *testing.T) {
	gw := newGateway(t)
	c := newClient(t, gw)

	folder := createIpfsFolder(t, false)
	res, err := c.Add(context.Background(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
	require.NoError(t, err)

	time.Sleep(time.Second * 10) // Allow to succeed

	t.Run("replace with unavailable pin should fail", func(t *testing.T) {
		folder := createIpfsFolder(t, true)
		res2, err := c.Replace(context.Background(), res.GetRequestId(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
		require.NoError(t, err)
		assert.Equal(t, res.GetRequestId(), res2.GetRequestId())
		assert.Equal(t, psc.StatusQueued, res2.GetStatus())

		time.Sleep(time.Second * 10) // Allow to fail

		res3, err := c.GetStatusByID(context.Background(), res.GetRequestId())
		require.NoError(t, err)
		assert.Equal(t, psc.StatusFailed, res3.GetStatus())
	})

	t.Run("replace with available pin should succeed", func(t *testing.T) {
		folder := createIpfsFolder(t, false)
		res2, err := c.Replace(context.Background(), res.GetRequestId(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
		require.NoError(t, err)
		assert.Equal(t, res.GetRequestId(), res2.GetRequestId())
		assert.Equal(t, psc.StatusQueued, res2.GetStatus())

		time.Sleep(time.Second * 10) // Allow to succeed

		res3, err := c.GetStatusByID(context.Background(), res.GetRequestId())
		require.NoError(t, err)
		assert.Equal(t, psc.StatusPinned, res3.GetStatus())
		assert.True(t, res3.GetPin().GetCid().Equals(folder.Cid()))
	})
}

func Test_RemovePin(t *testing.T) {
	gw := newGateway(t)
	c := newClient(t, gw)

	folder := createIpfsFolder(t, false)
	res, err := c.Add(context.Background(), folder.Cid(), psc.PinOpts.WithOrigins(origins...))
	require.NoError(t, err)

	time.Sleep(time.Second * 10) // Allow to succeed

	err = c.DeleteByID(context.Background(), res.GetRequestId())
	require.NoError(t, err)

	res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
	require.NoError(t, err)
	assert.Equal(t, psc.StatusPinning, res.GetStatus()) // Removal should start right away

	time.Sleep(time.Second * 5) // Allow to succeed

	res, err = c.GetStatusByID(context.Background(), res.GetRequestId())
	require.Error(t, err)
}

func newGateway(t *testing.T) *Gateway {
	threadsAddr := apitest.GetThreadsApiAddr()
	net, err := nc.NewClient(threadsAddr, common.GetClientRPCOpts(threadsAddr)...)
	require.NoError(t, err)

	db, err := dbc.NewClient(threadsAddr, common.GetClientRPCOpts(threadsAddr)...)
	require.NoError(t, err)
	ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
	require.NoError(t, err)
	ipnsms := tdb.NewTxMapDatastore()
	ipnsm, err := ipns.NewManager(ipnsms, ipfs)
	require.NoError(t, err)
	lib, err := buckets.NewBuckets(net, db, ipfs, ipnsm, nil)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	pss, err := util.NewBadgerDatastore(dir, "pinq")
	require.NoError(t, err)
	ps, err := pinning.NewService(lib, pss)
	require.NoError(t, err)

	listenPort, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr := fmt.Sprintf("127.0.0.1:%d", listenPort)
	baseUrl := fmt.Sprintf("http://127.0.0.1:%d", listenPort)
	gw, err := NewGateway(lib, ipfs, ipnsm, ps, Config{
		Addr: addr,
		URL:  baseUrl,
	})
	cmd.ErrCheck(err)
	gw.Start()

	t.Cleanup(func() {
		require.NoError(t, gw.Close())
		require.NoError(t, ps.Close())
		require.NoError(t, pss.Close())
		require.NoError(t, lib.Close())
		require.NoError(t, ipnsm.Close())
		require.NoError(t, ipnsms.Close())
		require.NoError(t, db.Close())
		require.NoError(t, net.Close())
	})
	return gw
}

func newClient(t *testing.T, gw *Gateway) *psc.Client {
	token := newIdentityToken(t)
	buck, _, _, err := gw.lib.Create(context.Background(), token)
	require.NoError(t, err)
	url := fmt.Sprintf("%s/bps/%s", gw.url, buck.Key)
	return psc.NewClient(url, string(token))
}

func newIdentityToken(t *testing.T) did.Token {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id := thread.NewLibp2pIdentity(sk)
	token, err := id.Token("did:key:foo", time.Hour)
	require.NoError(t, err)
	return token
}

func createIpfsFile(t *testing.T, hashOnly bool) (pth path.Resolved) {
	ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
	require.NoError(t, err)
	pth, err = ipfs.Unixfs().Add(
		context.Background(),
		ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
			"file.txt": ipfsfiles.NewBytesFile(util.GenerateRandomBytes(512)),
		}),
		options.Unixfs.HashOnly(hashOnly),
	)
	require.NoError(t, err)
	return pth
}

func createIpfsFolder(t *testing.T, hashOnly bool) (pth path.Resolved) {
	ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
	require.NoError(t, err)
	pth, err = ipfs.Unixfs().Add(
		context.Background(),
		ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
			"file1.txt": ipfsfiles.NewBytesFile(util.GenerateRandomBytes(1024)),
			"folder1": ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
				"file2.txt": ipfsfiles.NewBytesFile(util.GenerateRandomBytes(512)),
			}),
		}),
		options.Unixfs.HashOnly(hashOnly),
	)
	require.NoError(t, err)
	return pth
}

func getOrigins() ([]maddr.Multiaddr, error) {
	ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
	if err != nil {
		return nil, err
	}
	key, err := ipfs.Key().Self(context.Background())
	if err != nil {
		return nil, err
	}
	paddr, err := maddr.NewMultiaddr("/p2p/" + key.ID().String())
	if err != nil {
		return nil, err
	}
	addrs, err := ipfs.Swarm().LocalAddrs(context.Background())
	if err != nil {
		return nil, err
	}
	paddrs := make([]maddr.Multiaddr, len(addrs))
	for i, a := range addrs {
		paddrs[i] = a.Encapsulate(paddr)
	}
	return paddrs, nil
}
