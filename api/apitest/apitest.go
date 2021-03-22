package apitest

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path"
	"runtime"
	"testing"
	"time"

	httpapi "github.com/ipfs/go-ipfs-http-client"
	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/common"
	"github.com/textileio/go-buckets/ipns"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	tdb "github.com/textileio/go-threads/db"
	nc "github.com/textileio/go-threads/net/api/client"
	tutil "github.com/textileio/go-threads/util"
)

func NewService(t *testing.T) (listenAddr string, host did.DID) {
	err := tutil.SetLogLevels(map[string]logging.LogLevel{
		"buckets":      logging.LevelDebug,
		"buckets/api":  logging.LevelDebug,
		"buckets/ipns": logging.LevelDebug,
		"buckets/dns":  logging.LevelDebug,
	})
	require.NoError(t, err)

	threadsAddr := GetThreadsApiAddr()
	net, err := nc.NewClient(threadsAddr, common.GetClientRPCOpts(threadsAddr)...)
	require.NoError(t, err)

	// @todo: Use service description to build client
	doc, err := net.GetServices(context.Background())
	require.NoError(t, err)

	db, err := dbc.NewClient(threadsAddr, common.GetClientRPCOpts(threadsAddr)...)
	require.NoError(t, err)
	ipfs, err := httpapi.NewApi(GetIPFSApiMultiAddr())
	require.NoError(t, err)
	ipnsms := tdb.NewTxMapDatastore()
	ipnsm, err := ipns.NewManager(ipnsms, ipfs)
	require.NoError(t, err)
	lib, err := buckets.NewBuckets(net, db, ipfs, ipnsm, nil)
	require.NoError(t, err)

	listenPort, err := freeport.GetFreePort()
	require.NoError(t, err)
	listenAddr = fmt.Sprintf("127.0.0.1:%d", listenPort)
	server, proxy, err := common.GetServerAndProxy(lib, listenAddr, "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, proxy.Close())
		server.Stop()
		require.NoError(t, lib.Close())
		require.NoError(t, ipnsm.Close())
		require.NoError(t, ipnsms.Close())
		require.NoError(t, db.Close())
		require.NoError(t, net.Close())
	})

	return listenAddr, doc.ID
}

// GetThreadsApiAddr returns env value or default.
func GetThreadsApiAddr() string {
	env := os.Getenv("THREADS_API_ADDR")
	if env != "" {
		return env
	}
	return "127.0.0.1:4002"
}

// GetIPFSApiMultiAddr returns env value or default.
func GetIPFSApiMultiAddr() ma.Multiaddr {
	env := os.Getenv("IPFS_API_MULTIADDR")
	if env != "" {
		return tutil.MustParseAddr(env)
	}
	return tutil.MustParseAddr("/ip4/127.0.0.1/tcp/5012")
}

// StartServices starts an ipfs and threads node for tests.
func StartServices() (cleanup func()) {
	_, currentFilePath, _, _ := runtime.Caller(0)
	dirpath := path.Dir(currentFilePath)

	makeDown := func() {
		cmd := exec.Command(
			"docker-compose",
			"-f",
			fmt.Sprintf("%s/docker-compose.yml", dirpath),
			"down",
			"-v",
			"--remove-orphans",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatalf("docker-compose down: %s", err)
		}
	}
	makeDown()

	cmd := exec.Command(
		"docker-compose",
		"-f",
		fmt.Sprintf("%s/docker-compose.yml", dirpath),
		"build",
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("docker-compose build: %s", err)
	}
	cmd = exec.Command(
		"docker-compose",
		"-f",
		fmt.Sprintf("%s/docker-compose.yml", dirpath),
		"up",
		"-V",
	)
	//cmd.Stdout = os.Stdout
	//cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		log.Fatalf("running docker-compose: %s", err)
	}

	limit := 20
	retries := 0
	var err error
	for retries < limit {
		err = checkServices()
		if err == nil {
			break
		}
		time.Sleep(time.Second)
		retries++
	}
	if retries == limit {
		makeDown()
		if err != nil {
			log.Fatalf("connecting to services: %s", err)
		}
		log.Fatalf("max retries exhausted connecting to services")
	}
	return makeDown
}

func checkServices() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	// Check Threads
	threadsAddr := GetThreadsApiAddr()
	tc, err := nc.NewClient(threadsAddr, common.GetClientRPCOpts(threadsAddr)...)
	if err != nil {
		return err
	}
	if _, err := tc.GetServices(ctx); err != nil {
		return err
	}

	// Check IPFS
	ic, err := httpapi.NewApi(GetIPFSApiMultiAddr())
	if err != nil {
		return err
	}
	if _, err = ic.Key().Self(ctx); err != nil {
		return err
	}
	return nil
}
