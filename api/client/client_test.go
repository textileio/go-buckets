package client_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	ipfsfiles "github.com/ipfs/go-ipfs-files"
	httpapi "github.com/ipfs/go-ipfs-http-client"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api/apitest"
	"github.com/textileio/go-buckets/api/client"
	"github.com/textileio/go-buckets/api/common"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/dag"
	"github.com/textileio/go-threads/core/did"
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

func TestClient_Create(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	res, err := c.Create(ctx, buckets.WithName("mybuck"))
	require.NoError(t, err)
	assert.NotEmpty(t, res.Bucket)
	assert.NotEmpty(t, res.Bucket.Thread)
	assert.NotEmpty(t, res.Bucket.Key)
	assert.NotEmpty(t, res.Bucket.Owner)
	assert.NotEmpty(t, res.Bucket.Name)
	assert.NotEmpty(t, res.Bucket.Version)
	assert.NotEmpty(t, res.Bucket.Path)
	assert.NotEmpty(t, res.Bucket.Metadata)
	assert.NotEmpty(t, res.Bucket.CreatedAt)
	assert.NotEmpty(t, res.Bucket.UpdatedAt)
	assert.NotEmpty(t, res.Links)
	assert.NotEmpty(t, res.Seed)

	res2, err := c.Create(
		ctx,
		buckets.WithName("mybuck2"),
		buckets.WithThread(thread.MustDecode(res.Bucket.Thread)),
	)
	require.NoError(t, err)
	assert.Equal(t, res.Bucket.Thread, res2.Bucket.Thread)

	res3, err := c.Create(
		ctx,
		buckets.WithName("myprivatebuck"),
		buckets.WithPrivate(true),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, res3.Bucket)
	assert.NotEmpty(t, res3.Bucket.LinkKey)
}

func TestClient_CreateWithCid(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		createWithCid(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		createWithCid(t, ctx, c, true)
	})
}

func createWithCid(t *testing.T, ctx context.Context, c *client.Client, private bool) {
	file1, err := os.Open("testdata/file1.jpg")
	require.NoError(t, err)
	defer file1.Close()
	file2, err := os.Open("testdata/file2.jpg")
	require.NoError(t, err)
	defer file2.Close()

	ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
	require.NoError(t, err)
	p, err := ipfs.Unixfs().Add(
		ctx,
		ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
			"file1.jpg": ipfsfiles.NewReaderFile(file1),
			"folder1": ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
				"file2.jpg": ipfsfiles.NewReaderFile(file2),
			}),
		}),
	)
	require.NoError(t, err)
	res, err := c.Create(ctx, buckets.WithCid(p.Cid()), buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	// Assert top level bucket.
	list, err := c.ListPath(ctx, id, res.Bucket.Key, "")
	require.NoError(t, err)
	assert.True(t, list.Item.IsDir)
	topLevelNames := make([]string, 3)
	for i, n := range list.Item.Items {
		topLevelNames[i] = n.Name
	}
	assert.Contains(t, topLevelNames, "file1.jpg")
	assert.Contains(t, topLevelNames, "folder1")

	// Assert inner directory.
	list, err = c.ListPath(ctx, id, res.Bucket.Key, "folder1")
	require.NoError(t, err)
	assert.True(t, list.Item.IsDir)
	assert.Len(t, list.Item.Items, 1)
	assert.Equal(t, list.Item.Items[0].Name, "file2.jpg")
}

func TestClient_Get(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	res, err := c.Create(ctx)
	require.NoError(t, err)

	res2, err := c.Get(ctx, thread.MustDecode(res.Bucket.Thread), res.Bucket.Key)
	require.NoError(t, err)
	assert.NotEmpty(t, res2.Bucket)
	assert.NotEmpty(t, res2.Bucket.Thread)
	assert.NotEmpty(t, res2.Bucket.Key)
	assert.NotEmpty(t, res2.Bucket.Owner)
	assert.NotEmpty(t, res2.Bucket.Version)
	assert.NotEmpty(t, res2.Bucket.Path)
	assert.NotEmpty(t, res2.Bucket.Metadata)
	assert.NotEmpty(t, res2.Bucket.CreatedAt)
	assert.NotEmpty(t, res2.Bucket.UpdatedAt)
}

func TestClient_GetLinks(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	res, err := c.Create(ctx)
	require.NoError(t, err)

	res2, err := c.GetLinks(ctx, thread.MustDecode(res.Bucket.Thread), res.Bucket.Key, "")
	require.NoError(t, err)
	assert.NotEmpty(t, res2.Links.Url)
	assert.NotEmpty(t, res2.Links.Ipns)
}

func TestClient_List(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	res, err := c.Create(ctx)
	require.NoError(t, err)

	rep, err := c.List(ctx, thread.MustDecode(res.Bucket.Thread))
	require.NoError(t, err)
	assert.Len(t, rep.Buckets, 1)
	assert.Equal(t, res.Bucket.Key, rep.Buckets[0].Key)
	assert.Equal(t, res.Bucket.Path, rep.Buckets[0].Path)
}

func TestClient_Remove(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		remove(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		remove(t, ctx, c, true)
	})
}

func remove(t *testing.T, ctx context.Context, c *client.Client, private bool) {
	res, err := c.Create(ctx, buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	q, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q.AddFile("file1.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	err = q.AddFile("again/file2.jpg", "testdata/file2.jpg")
	require.NoError(t, err)
	for q.Next() {
		require.NoError(t, q.Err())
	}
	q.Close()

	err = c.Remove(ctx, id, res.Bucket.Key)
	require.NoError(t, err)

	_, err = c.ListPath(ctx, id, res.Bucket.Key, "again/file2.jpg")
	require.Error(t, err)
}

func TestClient_ListPath(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		listPath(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		listPath(t, ctx, c, true)
	})
}

func listPath(t *testing.T, ctx context.Context, c *client.Client, private bool) {
	res, err := c.Create(ctx, buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	t.Run("empty", func(t *testing.T) {
		rep, err := c.ListPath(ctx, id, res.Bucket.Key, "")
		require.NoError(t, err)
		assert.NotEmpty(t, rep.Bucket)
		assert.True(t, rep.Item.IsDir)
		assert.Len(t, rep.Item.Items, 1)
	})

	q, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q.AddFile("dir1/file1.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	err = q.AddFile("dir2/file2.jpg", "testdata/file2.jpg")
	require.NoError(t, err)
	for q.Next() {
		require.NoError(t, q.Err())
	}
	q.Close()
	root := q.Current.Root

	t.Run("root dir", func(t *testing.T) {
		rep, err := c.ListPath(ctx, id, res.Bucket.Key, "")
		require.NoError(t, err)
		assert.True(t, rep.Item.IsDir)
		assert.Len(t, rep.Item.Items, 3)
		dir1i := sort.Search(len(rep.Item.Items), func(i int) bool {
			return rep.Item.Items[i].Name == "dir1"
		})
		assert.True(t, dir1i < len(rep.Item.Items))
		assert.True(t, rep.Item.Items[dir1i].IsDir)
	})

	t.Run("nested dir", func(t *testing.T) {
		rep, err := c.ListPath(ctx, id, res.Bucket.Key, "dir1")
		require.NoError(t, err)
		assert.True(t, rep.Item.IsDir)
		assert.Len(t, rep.Item.Items, 1)
	})

	t.Run("file", func(t *testing.T) {
		rep, err := c.ListPath(ctx, id, res.Bucket.Key, "dir1/file1.jpg")
		require.NoError(t, err)
		assert.True(t, strings.HasSuffix(rep.Item.Path, "file1.jpg"))
		assert.False(t, rep.Item.IsDir)
		assert.Equal(t, root.String(), rep.Bucket.Path)
	})
}

func TestClient_ListIpfsPath(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	file1, err := os.Open("testdata/file1.jpg")
	require.NoError(t, err)
	defer file1.Close()
	file2, err := os.Open("testdata/file2.jpg")
	require.NoError(t, err)
	defer file2.Close()

	ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
	require.NoError(t, err)
	p, err := ipfs.Unixfs().Add(
		ctx,
		ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
			"file1.jpg": ipfsfiles.NewReaderFile(file1),
			"folder1": ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
				"file2.jpg": ipfsfiles.NewReaderFile(file2),
			}),
		}),
	)
	require.NoError(t, err)

	r, err := c.ListIpfsPath(ctx, p)
	require.NoError(t, err)
	require.True(t, r.Item.IsDir)
	require.Len(t, r.Item.Items, 2)
}

func TestClient_PushPaths(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		pushPaths(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		pushPaths(t, ctx, c, true)
	})
}

func pushPaths(t *testing.T, ctx context.Context, c *client.Client, private bool) {
	res, err := c.Create(ctx, buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	progress1 := make(chan int64)
	defer close(progress1)
	go func() {
		for p := range progress1 {
			fmt.Println(fmt.Sprintf("progress: %d", p))
		}
	}()

	q, err := c.PushPaths(ctx, id, res.Bucket.Key, buckets.WithProgress(progress1))
	require.NoError(t, err)
	err = q.AddFile("file1.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	err = q.AddFile("path/to/file2.jpg", "testdata/file2.jpg")
	require.NoError(t, err)
	for q.Next() {
		require.NoError(t, q.Err())
		assert.NotEmpty(t, q.Current.Path)
		assert.NotEmpty(t, q.Current.Root)
	}
	q.Close()

	rep1, err := c.ListPath(ctx, id, res.Bucket.Key, "")
	require.NoError(t, err)
	assert.Len(t, rep1.Item.Items, 3)

	// Try overwriting the path
	q2, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	r := strings.NewReader("seeya!")
	err = q2.AddReader("path/to/file2.jpg", r, r.Size())
	require.NoError(t, err)
	for q2.Next() {
		require.NoError(t, q2.Err())
	}
	q2.Close()

	rep2, err := c.ListPath(ctx, id, res.Bucket.Key, "")
	require.NoError(t, err)
	assert.Len(t, rep2.Item.Items, 3)

	// Overwrite the path again, this time replacing a file link with a dir link
	q3, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q3.AddFile("path/to", "testdata/file2.jpg")
	require.NoError(t, err)
	for q3.Next() {
		require.NoError(t, q3.Err())
	}
	q3.Close()

	rep3, err := c.ListPath(ctx, id, res.Bucket.Key, "")
	require.NoError(t, err)
	assert.Len(t, rep3.Item.Items, 3)

	// Concurrent writes should result in one being rejected due to the fast-forward-only rule
	root, err := dag.NewResolvedPath(rep3.Bucket.Path)
	require.NoError(t, err)
	var err1, err2 error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		q, err := c.PushPaths(ctx, id, res.Bucket.Key, buckets.WithFastForwardOnly(root))
		require.NoError(t, err)
		err = q.AddReader("conflict", strings.NewReader("ready, set, go!"), 0)
		require.NoError(t, err)
		for q.Next() {
			err1 = q.Err()
		}
		q.Close()
		wg.Done()
	}()
	go func() {
		q, err := c.PushPaths(ctx, id, res.Bucket.Key, buckets.WithFastForwardOnly(root))
		require.NoError(t, err)
		err = q.AddReader("conflict", strings.NewReader("ready, set, go!"), 0)
		require.NoError(t, err)
		for q.Next() {
			err2 = q.Err()
		}
		q.Close()
		wg.Done()
	}()
	wg.Wait()
	// We should have one and only one error
	assert.False(t, (err1 != nil && err2 != nil) && (err1 == nil && err2 == nil))
	if err1 != nil {
		assert.True(t, strings.Contains(err1.Error(), "update is non-fast-forward"))
	} else if err2 != nil {
		assert.True(t, strings.Contains(err2.Error(), "update is non-fast-forward"))
	}
}

func TestClient_PullPath(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		pullPath(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		pullPath(t, ctx, c, true)
	})
}

func pullPath(t *testing.T, ctx context.Context, c *client.Client, private bool) {
	res, err := c.Create(ctx, buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	file, err := os.Open("testdata/file1.jpg")
	require.NoError(t, err)
	defer file.Close()
	q, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q.AddFile("file1.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	for q.Next() {
		require.NoError(t, q.Err())
	}
	q.Close()

	tmp, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer tmp.Close()

	progress := make(chan int64)
	go func() {
		for p := range progress {
			fmt.Println(fmt.Sprintf("progress: %d", p))
		}
	}()
	err = c.PullPath(ctx, id, res.Bucket.Key, "file1.jpg", tmp, buckets.WithProgress(progress))
	require.NoError(t, err)
	info, err := tmp.Stat()
	require.NoError(t, err)
	fmt.Println(fmt.Sprintf("wrote file with size %d", info.Size()))

	note := "baps!"
	q2, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q2.AddReader("one/two/note.txt", strings.NewReader(note), 0)
	require.NoError(t, err)
	for q2.Next() {
		require.NoError(t, q2.Err())
	}
	q2.Close()

	var buf bytes.Buffer
	err = c.PullPath(ctx, id, res.Bucket.Key, "one/two/note.txt", &buf)
	require.NoError(t, err)
	assert.Equal(t, note, buf.String())
}

func TestClient_PullIpfsPath(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	file1, err := os.Open("testdata/file1.jpg")
	require.NoError(t, err)
	defer file1.Close()
	file2, err := os.Open("testdata/file2.jpg")
	require.NoError(t, err)
	defer file2.Close()

	ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
	require.NoError(t, err)
	p, err := ipfs.Unixfs().Add(
		ctx,
		ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
			"file1.jpg": ipfsfiles.NewReaderFile(file1),
			"folder1": ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
				"file2.jpg": ipfsfiles.NewReaderFile(file2),
			}),
		}),
	)
	require.NoError(t, err)

	tmpFile, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer tmpFile.Close()
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	tmpName := tmpFile.Name()

	err = c.PullIpfsPath(ctx, path.Join(p, "folder1/file2.jpg"), tmpFile)
	require.NoError(t, err)
	tmpFile.Close()

	file2, err = os.Open("testdata/file2.jpg")
	require.NoError(t, err)
	defer file2.Close()
	origBytes, err := ioutil.ReadAll(file2)
	require.NoError(t, err)
	tmpFile, err = os.Open(tmpName)
	require.NoError(t, err)
	defer tmpFile.Close()
	tmpBytes, err := ioutil.ReadAll(tmpFile)
	require.NoError(t, err)
	require.True(t, bytes.Equal(origBytes, tmpBytes))
}

func TestClient_SetPath(t *testing.T) {
	t.Run("public", func(t *testing.T) {
		setPath(t, false)
	})

	t.Run("private", func(t *testing.T) {
		setPath(t, true)
	})
}

func setPath(t *testing.T, private bool) {
	innerPaths := []struct {
		Name string
		Path string
		// How many files should be expected at the bucket root
		// in the first SetHead.
		NumFilesAtRootFirst int
		// How many files should be expected at the *imported* Cid
		// level.
		NumFilesAtImportedLevelFirst int
		// How many files should be expected at the bucket root
		// in the second SetHead.
		NumFilesAtRootSecond int
		// How many files should be expected at the *imported* Cid
		// level.
		NumFilesAtImportedLevelSecond int
	}{
		{
			Name: "nested",
			Path: "nested",
			// At root level, .seed and nested dir.
			NumFilesAtRootFirst: 2,
			// The first SetHead has one file, and one dir.
			NumFilesAtImportedLevelFirst: 2,
			NumFilesAtRootSecond:         2,
			// The second SetHead only has one file.
			NumFilesAtImportedLevelSecond: 1,
		},
		// Edge case, Path is empty. So "AtRoot" or "AtImportedLevel"
		// is the same.
		{
			Name:                         "root",
			Path:                         "",
			NumFilesAtRootFirst:          3,
			NumFilesAtImportedLevelFirst: 3,
			// In both below cases, the files are the .seed
			// and the fileVersion2.jpg.
			NumFilesAtRootSecond:          2,
			NumFilesAtImportedLevelSecond: 2,
		},
	}

	for _, innerPath := range innerPaths {
		t.Run(innerPath.Name, func(t *testing.T) {
			c := newClient(t)
			ctx, _ := newIdentityCtx(t, c)

			file1, err := os.Open("testdata/file1.jpg")
			require.NoError(t, err)
			defer file1.Close()
			file2, err := os.Open("testdata/file2.jpg")
			require.NoError(t, err)
			defer file2.Close()

			ipfs, err := httpapi.NewApi(apitest.GetIPFSApiMultiAddr())
			require.NoError(t, err)
			p, err := ipfs.Unixfs().Add(
				ctx,
				ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
					"file1.jpg": ipfsfiles.NewReaderFile(file1),
					"folder1": ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
						"file2.jpg": ipfsfiles.NewReaderFile(file2),
					}),
				}),
			)
			require.NoError(t, err)

			res, err := c.Create(ctx, buckets.WithPrivate(private))
			require.NoError(t, err)
			id := thread.MustDecode(res.Bucket.Thread)

			_, err = c.SetPath(ctx, id, res.Bucket.Key, innerPath.Path, p.Cid())
			require.NoError(t, err)

			rep, err := c.ListPath(ctx, id, res.Bucket.Key, "")
			require.NoError(t, err)
			assert.True(t, rep.Item.IsDir)
			assert.Len(t, rep.Item.Items, innerPath.NumFilesAtRootFirst)

			rep, err = c.ListPath(ctx, id, res.Bucket.Key, innerPath.Path)
			require.NoError(t, err)
			assert.True(t, rep.Item.IsDir)
			assert.Len(t, rep.Item.Items, innerPath.NumFilesAtImportedLevelFirst)

			// SetPath again in the same path, but with a different dag.
			// Should replace what was already under that path.
			file1, err = os.Open("testdata/file1.jpg")
			require.NoError(t, err)
			defer file1.Close()
			p, err = ipfs.Unixfs().Add(
				ctx,
				ipfsfiles.NewMapDirectory(map[string]ipfsfiles.Node{
					"fileVersion2.jpg": ipfsfiles.NewReaderFile(file1),
				}),
			)
			require.NoError(t, err)
			_, err = c.SetPath(ctx, id, res.Bucket.Key, innerPath.Path, p.Cid())
			require.NoError(t, err)

			rep, err = c.ListPath(ctx, id, res.Bucket.Key, "")
			require.NoError(t, err)
			assert.True(t, rep.Item.IsDir)
			assert.Len(t, rep.Item.Items, innerPath.NumFilesAtRootSecond)

			rep, err = c.ListPath(ctx, id, res.Bucket.Key, innerPath.Path)
			require.NoError(t, err)
			assert.True(t, rep.Item.IsDir)
			assert.Len(t, rep.Item.Items, innerPath.NumFilesAtImportedLevelSecond)
		})
	}
}

func TestClient_MovePath(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		movePath(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		movePath(t, ctx, c, true)
	})
}

func movePath(t *testing.T, ctx context.Context, c *client.Client, private bool) {
	res, err := c.Create(ctx, buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	q, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q.AddFile("root.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	err = q.AddFile("a/b/c/file1.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	err = q.AddFile("a/b/c/file2.jpg", "testdata/file2.jpg")
	require.NoError(t, err)
	for q.Next() {
		require.NoError(t, q.Err())
	}
	q.Close()

	// move a dir into a new path  => a/c
	err = c.MovePath(ctx, id, res.Bucket.Key, "a/b/c", "a/c")
	require.NoError(t, err)
	// check source files no longer exists
	li, err := c.ListPath(ctx, id, res.Bucket.Key, "a/b/c")
	require.Error(t, err)

	// check source parent remains untouched
	li, err = c.ListPath(ctx, id, res.Bucket.Key, "a/b")
	require.NoError(t, err)
	assert.True(t, li.Item.IsDir)
	assert.Len(t, li.Item.Items, 0)

	// move a dir into an existing path => a/b/c
	err = c.MovePath(ctx, id, res.Bucket.Key, "a/c", "a/b")
	require.NoError(t, err)
	// check source dir no longer exists
	li, err = c.ListPath(ctx, id, res.Bucket.Key, "a/c")
	require.Error(t, err)

	// check source parent contains all the right children
	li, err = c.ListPath(ctx, id, res.Bucket.Key, "a")
	require.NoError(t, err)
	assert.True(t, li.Item.IsDir)
	assert.Len(t, li.Item.Items, 1)

	// check source parent contains all the right children
	li, err = c.ListPath(ctx, id, res.Bucket.Key, "a/b/c")
	require.NoError(t, err)
	assert.True(t, li.Item.IsDir)
	assert.Len(t, li.Item.Items, 2)

	// move and rename file => a/b/c/file2.jpg + a/b/file3.jpg
	err = c.MovePath(ctx, id, res.Bucket.Key, "a/b/c/file1.jpg", "a/b/file3.jpg")
	require.NoError(t, err)

	li, err = c.ListPath(ctx, id, res.Bucket.Key, "a/b/file3.jpg")
	require.NoError(t, err)
	assert.False(t, li.Item.IsDir)
	assert.Equal(t, li.Item.Name, "file3.jpg")

	// move a/b/c to root => c
	err = c.MovePath(ctx, id, res.Bucket.Key, "a/b/c", "")
	require.NoError(t, err)

	li, err = c.ListPath(ctx, id, res.Bucket.Key, "")
	require.NoError(t, err)
	assert.True(t, li.Item.IsDir)
	assert.Len(t, li.Item.Items, 4)

	li, err = c.ListPath(ctx, id, res.Bucket.Key, "root.jpg")
	require.NoError(t, err)

	// move root should fail
	err = c.MovePath(ctx, id, res.Bucket.Key, "", "a")
	require.Error(t, err)

	li, err = c.ListPath(ctx, id, res.Bucket.Key, "a")
	require.NoError(t, err)
	assert.True(t, li.Item.IsDir)
	assert.Len(t, li.Item.Items, 1)

	// move non existant should fail
	err = c.MovePath(ctx, id, res.Bucket.Key, "x", "a")
	require.Error(t, err)
	if private {
		assert.True(t, strings.Contains(err.Error(), "could not resolve path"))
	} else {
		assert.True(t, strings.Contains(err.Error(), "no link named"))
	}
}

func TestClient_RemovePath(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		removePath(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		removePath(t, ctx, c, true)
	})
}

func removePath(t *testing.T, ctx context.Context, c *client.Client, private bool) {
	res, err := c.Create(ctx, buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	q, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q.AddFile("file1.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	err = q.AddFile("again/file2.jpg", "testdata/file2.jpg")
	require.NoError(t, err)
	for q.Next() {
		require.NoError(t, q.Err())
	}
	q.Close()

	pth, err := c.RemovePath(ctx, id, res.Bucket.Key, "again/file2.jpg")
	require.NoError(t, err)
	assert.NotEmpty(t, pth)
	_, err = c.ListPath(ctx, id, res.Bucket.Key, "again/file2.jpg")
	require.Error(t, err)
	rep, err := c.ListPath(ctx, id, res.Bucket.Key, "")
	require.NoError(t, err)
	assert.Len(t, rep.Item.Items, 3)

	_, err = c.RemovePath(ctx, id, res.Bucket.Key, "again")
	require.NoError(t, err)
	_, err = c.ListPath(ctx, id, res.Bucket.Key, "again")
	require.Error(t, err)
	rep, err = c.ListPath(ctx, id, res.Bucket.Key, "")
	require.NoError(t, err)
	assert.Len(t, rep.Item.Items, 2)
}

func TestClient_PushPathAccessRoles(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	t.Run("public", func(t *testing.T) {
		pushPathAccessRoles(t, ctx, c, false)
	})

	t.Run("private", func(t *testing.T) {
		pushPathAccessRoles(t, ctx, c, true)
	})
}

func pushPathAccessRoles(
	t *testing.T,
	ctx context.Context,
	c *client.Client,
	private bool,
) {
	res, err := c.Create(ctx, buckets.WithPrivate(private))
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	t.Run("non-existent path", func(t *testing.T) {
		_, pk, err := crypto.GenerateEd25519Key(rand.Reader)
		require.NoError(t, err)
		reader, err := thread.NewLibp2pPubKey(pk).DID()
		require.NoError(t, err)

		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "nothing/here", map[did.DID]collection.Role{
			reader: collection.ReaderRole,
		})
		require.Error(t, err)
	})

	t.Run("existent path", func(t *testing.T) {
		userCtx, identity := newIdentityCtx(t, c)
		user, err := identity.GetPublic().DID()
		require.NoError(t, err)

		q, err := c.PushPaths(ctx, id, res.Bucket.Key)
		require.NoError(t, err)
		err = q.AddFile("file", "testdata/file1.jpg")
		require.NoError(t, err)
		for q.Next() {
			require.NoError(t, q.Err())
		}
		q.Close()

		// Check initial access (none)
		check := accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "file",
		}
		check.Read = !private // public buckets readable by all
		checkAccess(t, userCtx, c, check)

		// Grant reader
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "file", map[did.DID]collection.Role{
			user: collection.ReaderRole,
		})
		require.NoError(t, err)
		check.Read = true
		checkAccess(t, userCtx, c, check)

		// Grant writer
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "file", map[did.DID]collection.Role{
			user: collection.WriterRole,
		})
		require.NoError(t, err)
		check.Write = true
		checkAccess(t, userCtx, c, check)

		// Grant admin
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "file", map[did.DID]collection.Role{
			user: collection.AdminRole,
		})
		require.NoError(t, err)
		check.Admin = true
		checkAccess(t, userCtx, c, check)

		// Ungrant
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "file", map[did.DID]collection.Role{
			user: collection.NoneRole,
		})
		require.NoError(t, err)
		check.Read = !private
		check.Write = false
		check.Admin = false
		checkAccess(t, userCtx, c, check)
	})

	t.Run("overlapping paths", func(t *testing.T) {
		user1Ctx, identity1 := newIdentityCtx(t, c)
		user1, err := identity1.GetPublic().DID()
		require.NoError(t, err)
		user2Ctx, identity2 := newIdentityCtx(t, c)
		user2, err := identity2.GetPublic().DID()
		require.NoError(t, err)

		q, err := c.PushPaths(ctx, id, res.Bucket.Key)
		require.NoError(t, err)
		err = q.AddFile("a/b/f1", "testdata/file1.jpg")
		require.NoError(t, err)
		for q.Next() {
			require.NoError(t, q.Err())
		}
		q.Close()

		q2, err := c.PushPaths(ctx, id, res.Bucket.Key)
		require.NoError(t, err)
		err = q2.AddFile("a/f2", "testdata/file2.jpg")
		require.NoError(t, err)
		for q2.Next() {
			require.NoError(t, q2.Err())
		}
		q2.Close()

		// Grant nested tree
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "a/b", map[did.DID]collection.Role{
			user1: collection.WriterRole,
		})
		require.NoError(t, err)
		checkAccess(t, user1Ctx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "a/b/f1",
			Read:   true,
			Write:  true,
		})

		// Grant parent of nested tree
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "a", map[did.DID]collection.Role{
			user2: collection.WriterRole,
		})
		require.NoError(t, err)
		checkAccess(t, user2Ctx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "a/b/f1",
			Read:   true,
			Write:  true,
		})

		// Re-check nested access for user1
		checkAccess(t, user1Ctx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "a/b/f1",
			Read:   true,
			Write:  true,
		})

		// Grant root
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "", map[did.DID]collection.Role{
			user2: collection.ReaderRole,
		})
		require.NoError(t, err)
		checkAccess(t, user2Ctx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "a/f2",
			Read:   true,
			Write:  true,
		})

		// Re-check nested access for user1
		checkAccess(t, user1Ctx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "a/b/f1",
			Read:   true,
			Write:  true,
		})
	})

	t.Run("moving path", func(t *testing.T) {
		userCtx, identity := newIdentityCtx(t, c)
		user, err := identity.GetPublic().DID()

		q, err := c.PushPaths(ctx, id, res.Bucket.Key)
		require.NoError(t, err)
		err = q.AddFile("moving/file", "testdata/file1.jpg")
		require.NoError(t, err)
		for q.Next() {
			require.NoError(t, q.Err())
		}
		q.Close()

		// Check initial access (none)
		check := accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "moving/file",
		}
		check.Read = !private // public buckets readable by all
		checkAccess(t, userCtx, c, check)

		// Grant reader
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "moving/file", map[did.DID]collection.Role{
			user: collection.WriterRole,
		})
		require.NoError(t, err)
		check.Write = true
		check.Read = true
		checkAccess(t, userCtx, c, check)

		// move the shared file to a new path
		err = c.MovePath(ctx, id, res.Bucket.Key, "moving/file", "moving/file2")
		require.NoError(t, err)
		q, err = c.PushPaths(ctx, id, res.Bucket.Key)
		require.NoError(t, err)
		err = q.AddFile("moving/file", "testdata/file1.jpg")
		require.NoError(t, err)
		for q.Next() {
			require.NoError(t, q.Err())
		}
		q.Close()

		// Permissions reset with move
		checkAccess(t, userCtx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "moving/file2",
			Admin:  false,
			Read:   !private,
			Write:  false,
		})

		// Grant admin at root
		err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "moving", map[did.DID]collection.Role{
			user: collection.AdminRole,
		})
		require.NoError(t, err)

		// now user has access to new file again
		checkAccess(t, userCtx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "moving/file2",
			Admin:  true,
			Read:   true,
			Write:  true,
		})

		// Move file again
		err = c.MovePath(ctx, id, res.Bucket.Key, "moving/file2", "moving/file3")
		require.NoError(t, err)

		// User still has access to shared file after move
		checkAccess(t, userCtx, c, accessCheck{
			Thread: id,
			Key:    res.Bucket.Key,
			Path:   "moving/file3",
			Admin:  true,
			Read:   true,
			Write:  true,
		})
	})
}

type accessCheck struct {
	Thread thread.ID
	Key    string
	Path   string

	Read  bool
	Write bool
	Admin bool
}

func checkAccess(t *testing.T, ctx context.Context, c *client.Client, check accessCheck) {
	// Check read access
	res, err := c.ListPath(ctx, check.Thread, check.Key, check.Path)
	if check.Read {
		require.NoError(t, err)
		assert.NotEmpty(t, res.Item)
	} else {
		require.Error(t, err)
	}

	// Check write access
	tmp, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	defer tmp.Close()
	_, err = io.CopyN(tmp, rand.Reader, 1024)
	require.NoError(t, err)
	_, err = tmp.Seek(0, 0)
	require.NoError(t, err)
	q, err := c.PushPaths(ctx, check.Thread, check.Key)
	require.NoError(t, err)
	err = q.AddReader(check.Path, tmp, 0)
	require.NoError(t, err)
	for q.Next() {
		if check.Write {
			require.NoError(t, q.Err())
		} else {
			require.Error(t, q.Err())
		}
	}
	q.Close()

	// Check admin access (role editing)
	_, pk, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	admin, err := thread.NewLibp2pPubKey(pk).DID()
	require.NoError(t, err)
	err = c.PushPathAccessRoles(ctx, check.Thread, check.Key, check.Path, map[did.DID]collection.Role{
		admin: collection.ReaderRole,
	})
	if check.Admin {
		require.NoError(t, err)
	} else {
		require.Error(t, err)
	}
}

func TestClient_PullPathAccessRoles(t *testing.T) {
	c := newClient(t)
	ctx, _ := newIdentityCtx(t, c)

	res, err := c.Create(ctx)
	require.NoError(t, err)
	id := thread.MustDecode(res.Bucket.Thread)

	q, err := c.PushPaths(ctx, id, res.Bucket.Key)
	require.NoError(t, err)
	err = q.AddFile("file1.jpg", "testdata/file1.jpg")
	require.NoError(t, err)
	for q.Next() {
		require.NoError(t, q.Err())
	}
	q.Close()

	roles, err := c.PullPathAccessRoles(ctx, id, res.Bucket.Key, "file1.jpg")
	require.NoError(t, err)
	assert.Len(t, roles, 0)

	_, pk, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	reader, err := thread.NewLibp2pPubKey(pk).DID()
	require.NoError(t, err)
	roles = map[did.DID]collection.Role{
		reader: collection.ReaderRole,
	}
	err = c.PushPathAccessRoles(ctx, id, res.Bucket.Key, "file1.jpg", roles)
	require.NoError(t, err)

	roles, err = c.PullPathAccessRoles(ctx, id, res.Bucket.Key, "file1.jpg")
	require.NoError(t, err)
	assert.Len(t, roles, 1)
}

func newClient(t *testing.T) *client.Client {
	listenAddr, _ := apitest.NewService(t)
	c, err := client.NewClient(listenAddr, common.GetClientRPCOpts(listenAddr)...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, c.Close())
	})
	return c
}

func newIdentityCtx(t *testing.T, c *client.Client) (context.Context, thread.Identity) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	id := thread.NewLibp2pIdentity(sk)
	ctx, err := c.NewTokenContext(context.Background(), id, time.Hour)
	require.NoError(t, err)
	return ctx, id
}
