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
	ma "github.com/multiformats/go-multiaddr"
	"github.com/phayes/freeport"
	"github.com/stretchr/testify/require"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/common"
	"github.com/textileio/go-buckets/ipns"
	"github.com/textileio/go-buckets/util"
	dbc "github.com/textileio/go-threads/api/client"
	"github.com/textileio/go-threads/core/did"
	tdb "github.com/textileio/go-threads/db"
	nc "github.com/textileio/go-threads/net/api/client"
)

func NewService(t *testing.T) (listenAddr string, host did.DID) {
	threadsAddr := GetThreadsApiAddr()
	net, err := nc.NewClient(threadsAddr, common.GetClientRPCOpts(threadsAddr)...)
	require.NoError(t, err)

	// @todo: Fix me
	doc, err := net.GetServices(context.Background())
	require.NoError(t, err)

	db, err := dbc.NewClient(threadsAddr, common.GetClientRPCOpts(threadsAddr)...)
	require.NoError(t, err)
	ipfs, err := httpapi.NewApi(GetIPFSApiAddr())
	require.NoError(t, err)
	ipnsm, err := ipns.NewManager(tdb.NewTxMapDatastore(), ipfs)
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
	return "127.0.0.1:4000"
}

// GetIPFSApiAddr returns env value or default.
func GetIPFSApiAddr() ma.Multiaddr {
	env := os.Getenv("IPFS_API_ADDR")
	if env != "" {
		return util.MustParseAddr(env)
	}
	return util.MustParseAddr("/ip4/127.0.0.1/tcp/5001")
}

// StartIPFS start an ipfs node.
func StartIPFS() (cleanup func()) {
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

	limit := 5
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
	ic, err := httpapi.NewApi(GetIPFSApiAddr())
	if err != nil {
		return err
	}
	if _, err = ic.Key().Self(ctx); err != nil {
		return err
	}
	return nil
}
