package pinning

import (
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	httpapi "github.com/ipfs/go-ipfs-http-client"
	logging "github.com/ipfs/go-log/v2"
	psc "github.com/ipfs/go-pinning-service-http-client"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/apitest"
	"github.com/textileio/go-buckets/api/client"
	"github.com/textileio/go-buckets/api/common"
	"github.com/textileio/go-buckets/cmd"
	"github.com/textileio/go-buckets/gateway"
	"github.com/textileio/go-buckets/ipns"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	"github.com/textileio/go-threads/core/thread"
	tdb "github.com/textileio/go-threads/db"
	nc "github.com/textileio/go-threads/net/api/client"
	"github.com/textileio/go-threads/util"
)

func TestMain(m *testing.M) {
	cleanup := func() {}
	if os.Getenv("SKIP_SERVICES") != "true" {
		cleanup = apitest.StartServices()
	}
	exitVal := m.Run()
	cleanup()
	os.Exit(exitVal)
}

func newService(t *testing.T) (addr, url string) {
	err := util.SetLogLevels(map[string]logging.LogLevel{
		"buckets":          logging.LevelDebug,
		"buckets/ps":       logging.LevelDebug,
		"buckets/ps-queue": logging.LevelDebug,
		"buckets/gateway":  logging.LevelDebug,
	})
	require.NoError(t, err)

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
	pss, err := util.NewBadgerDatastore(dir, "pinq", false)
	require.NoError(t, err)
	ps := NewService(lib, pss)

	listenPort, err := freeport.GetFreePort()
	require.NoError(t, err)
	addr = fmt.Sprintf("127.0.0.1:%d", listenPort)
	url = fmt.Sprintf("http://127.0.0.1:%d", listenPort)
	gw, err := gateway.NewGateway(lib, ipfs, ipnsm, ps, gateway.Config{
		Addr: addr,
		URL:  url,
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
	return addr, url
}

func newPinningService(t *testing.T) *psc.Client {
	addr, url := newService(t)
	bc, err := client.NewClient(addr, common.GetClientRPCOpts(addr)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, bc.Close())
	})

	ctx, _ := newIdentityCtx(t, bc)
	res, err := bc.Create(ctx)
	require.NoError(t, err)
	bpsurl := fmt.Sprintf("%s/bps/%s", url, res.Bucket.Key)
	token, _ := did.TokenFromContext(ctx)

	return psc.NewClient(bpsurl, string(token))
}

func newIdentityCtx(t *testing.T, c *client.Client) (context.Context, thread.Identity) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id := thread.NewLibp2pIdentity(sk)
	ctx, err := c.NewTokenContext(context.Background(), id, time.Hour)
	require.NoError(t, err)
	return ctx, id
}
