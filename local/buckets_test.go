package local_test

import (
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"

	ipfsfiles "github.com/ipfs/go-ipfs-files"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/textileio/go-buckets/api/apitest"
	"github.com/textileio/go-buckets/api/client"
	"github.com/textileio/go-buckets/api/common"
	. "github.com/textileio/go-buckets/local"
	"github.com/textileio/go-buckets/util"
	"github.com/textileio/go-threads/core/thread"
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

func TestBuckets_NewBucket(t *testing.T) {
	buckets := setup(t)

	t.Run("new bucket", func(t *testing.T) {
		conf := getConf(t)
		buck, err := buckets.NewBucket(context.Background(), conf)
		require.NoError(t, err)
		assert.NotEmpty(t, buck)

		b, err := buck.Get(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, b)

		reloaded, err := buckets.GetLocalBucket(context.Background(), conf)
		require.NoError(t, err)
		assert.NotEmpty(t, reloaded)
	})

	t.Run("new named bucket", func(t *testing.T) {
		conf := getConf(t)
		buck, err := buckets.NewBucket(context.Background(), conf, WithName("bucky"))
		require.NoError(t, err)
		assert.NotEmpty(t, buck)

		b, err := buck.Get(context.Background())
		require.NoError(t, err)
		assert.Equal(t, "bucky", b.Name)

		reloaded, err := buckets.GetLocalBucket(context.Background(), conf)
		require.NoError(t, err)
		assert.NotEmpty(t, reloaded)
	})

	t.Run("new private bucket", func(t *testing.T) {
		conf := getConf(t)
		buck, err := buckets.NewBucket(context.Background(), conf, WithPrivate(true))
		require.NoError(t, err)
		assert.NotEmpty(t, buck)

		b, err := buck.Get(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, b)

		reloaded, err := buckets.GetLocalBucket(context.Background(), conf)
		require.NoError(t, err)
		assert.NotEmpty(t, reloaded)
	})

	t.Run("new bootstrapped bucket", func(t *testing.T) {
		conf := getConf(t)
		pth := createIpfsFolder(t)
		events := make(chan Event)
		defer close(events)
		ec := &eventCollector{}
		go ec.collect(events)
		buck, err := buckets.NewBucket(context.Background(), conf, WithCid(pth.Cid()), WithInitEvents(events))
		require.NoError(t, err)
		assert.NotEmpty(t, buck)
		ec.check(t, 2, 0)

		b, err := buck.Get(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, b)

		items, err := buck.ListRemotePath(context.Background(), "")
		require.NoError(t, err)
		assert.Len(t, items, 3)

		bp, err := buck.Path()
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(bp, "file1.txt"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(bp, "folder1", "file2.txt"))
		require.NoError(t, err)

		reloaded, err := buckets.GetLocalBucket(context.Background(), conf)
		require.NoError(t, err)
		assert.NotEmpty(t, reloaded)
	})

	t.Run("new bucket from existing", func(t *testing.T) {
		conf := getConf(t)
		buck, err := buckets.NewBucket(context.Background(), conf)
		require.NoError(t, err)
		assert.NotEmpty(t, buck)

		addRandomFile(t, buck, "file1", 256)
		addRandomFile(t, buck, "folder/file2", 256)
		_, err = buck.PushLocal(context.Background())
		require.NoError(t, err)

		conf2 := Config{Path: newDir(t)}
		conf2.Key = buck.Key()
		conf2.Thread, err = buck.Thread()
		require.NoError(t, err)
		conf2.Identity, err = buck.Identity()
		require.NoError(t, err)
		buck2, err := buckets.NewBucket(context.Background(), conf2)
		require.NoError(t, err)
		require.NotEmpty(t, buck2)

		items, err := buck2.ListRemotePath(context.Background(), "")
		require.NoError(t, err)
		assert.Len(t, items, 3)

		bp, err := buck.Path()
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(bp, "file1"))
		require.NoError(t, err)
		_, err = os.Stat(filepath.Join(bp, "folder", "file2"))
		require.NoError(t, err)

		reloaded, err := buckets.GetLocalBucket(context.Background(), conf)
		require.NoError(t, err)
		assert.NotEmpty(t, reloaded)
	})

	t.Run("list remote buckets", func(t *testing.T) {
		conf := getConf(t)
		buck, err := buckets.NewBucket(context.Background(), conf)
		require.NoError(t, err)

		id, err := buck.Thread()
		require.NoError(t, err)
		list, err := buckets.RemoteBuckets(context.Background(), id, conf.Identity)
		require.NoError(t, err)
		assert.Len(t, list, 1)
	})
}

func TestBuckets_NewConfigFromCmd(t *testing.T) {
	buckets := setup(t)

	t.Run("no flags", func(t *testing.T) {
		c := initCmd(t, buckets, "", thread.Undef, false, false)
		err := c.Execute()
		require.NoError(t, err)
	})

	t.Run("with flags and no values", func(t *testing.T) {
		c := initCmd(t, buckets, "", thread.Undef, true, false)
		err := c.Execute()
		require.NoError(t, err)
	})

	t.Run("with flags and default values", func(t *testing.T) {
		key := "mykey"
		tid := thread.NewRandomIDV1()
		c := initCmd(t, buckets, key, tid, true, true)
		err := c.Execute()
		require.NoError(t, err)
	})

	t.Run("with flags and set values", func(t *testing.T) {
		key := "mykey"
		tid := thread.NewRandomIDV1()
		c := initCmd(t, buckets, key, tid, true, false)
		err := c.PersistentFlags().Set("key", key)
		require.NoError(t, err)
		err = c.PersistentFlags().Set("thread", tid.String())
		require.NoError(t, err)
		err = c.Execute()
		require.NoError(t, err)
	})

	t.Run("no flags and env values", func(t *testing.T) {
		key := "mykey"
		tid := thread.NewRandomIDV1()
		c := initCmd(t, buckets, key, tid, false, false)
		err := os.Setenv("BUCK_KEY", key)
		require.NoError(t, err)
		err = os.Setenv("BUCK_THREAD", tid.String())
		require.NoError(t, err)
		err = c.Execute()
		require.NoError(t, err)
	})

	t.Run("with flags and env values", func(t *testing.T) {
		key := "mykey"
		tid := thread.NewRandomIDV1()
		c := initCmd(t, buckets, key, tid, true, false)
		err := os.Setenv("BUCK_KEY", key)
		require.NoError(t, err)
		err = os.Setenv("BUCK_THREAD", tid.String())
		require.NoError(t, err)
		err = c.Execute()
		require.NoError(t, err)
	})

	t.Run("with key and no thread", func(t *testing.T) {
		dir := newDir(t)
		c := &cobra.Command{
			Use: "init",
			Run: func(c *cobra.Command, args []string) {
				_, err := buckets.NewConfigFromCmd(c, dir)
				require.Error(t, err)
				assert.Equal(t, ErrThreadRequired, err)
			},
		}
		err := os.Setenv("BUCK_KEY", "mykey")
		require.NoError(t, err)
		err = os.Setenv("BUCK_THREAD", "")
		require.NoError(t, err)
		err = c.Execute()
		require.NoError(t, err)
	})
}

func initCmd(t *testing.T, buckets *Buckets, key string, tid thread.ID, addFlags, setDefaults bool) *cobra.Command {
	dir := newDir(t)
	c := &cobra.Command{
		Use: "init",
		Run: func(c *cobra.Command, args []string) {
			conf, err := buckets.NewConfigFromCmd(c, dir)
			require.NoError(t, err)
			assert.Equal(t, dir, conf.Path)
			assert.Equal(t, key, conf.Key)
			if tid.Defined() {
				assert.Equal(t, tid, conf.Thread)
			} else {
				assert.Equal(t, thread.Undef, conf.Thread)
			}
		},
	}
	var dkey, dtid string
	if setDefaults {
		dkey = key
		dtid = tid.String()
	}
	if addFlags {
		c.PersistentFlags().String("key", dkey, "")
		c.PersistentFlags().String("thread", dtid, "")
	}
	return c
}

func setup(t *testing.T) *Buckets {
	listenAddr, _ := apitest.NewService(t)

	c, err := client.NewClient(listenAddr, common.GetClientRPCOpts(listenAddr)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})

	return NewBuckets(c, DefaultConfConfig())
}

func getConf(t *testing.T) Config {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	return Config{
		Path:     newDir(t),
		Identity: thread.NewLibp2pIdentity(sk),
	}
}

func newDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = os.RemoveAll(dir)
	})
	return dir
}

func createIpfsFolder(t *testing.T) (pth path.Resolved) {
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
	)
	require.NoError(t, err)
	return pth
}

type eventCollector struct {
	fileCompletes int
	fileRemoves   int
	sync.Mutex
}

func (c *eventCollector) collect(events chan Event) {
	for e := range events {
		c.Lock()
		switch e.Type {
		case EventFileComplete:
			c.fileCompletes++
		case EventFileRemoved:
			c.fileRemoves++
		}
		c.Unlock()
	}
}

func (c *eventCollector) check(t *testing.T, numFilesAdded, numFilesRemoved int) {
	time.Sleep(5 * time.Second)
	c.Lock()
	defer c.Unlock()
	assert.Equal(t, numFilesAdded, c.fileCompletes)
	assert.Equal(t, numFilesRemoved, c.fileRemoves)
}
