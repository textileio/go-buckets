package gateway_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/textileio/go-threads/core/did"
)

func Test_PushBucketPaths(t *testing.T) {
	gw := newGateway(t)

	token := newIdentityToken(t)
	buck, _, _, err := gw.Buckets().Create(context.Background(), token)
	require.NoError(t, err)

	files := make(map[string]*os.File)
	files["file"] = getRandomFile(t, 1024)
	files["dir/file"] = getRandomFile(t, 1024)
	files["dir/dir/file"] = getRandomFile(t, 1024)

	url := fmt.Sprintf("%s/thread/%s/buckets/%s?root=%s", gw.Url(), buck.Thread, buck.Key, buck.Path)
	pushBucketPaths(t, url, token, files)

	item, _, err := gw.Buckets().ListPath(context.Background(), buck.Thread, buck.Key, token, "")
	require.NoError(t, err)
	assert.True(t, item.IsDir)
	assert.Len(t, item.Items, 3) // .textileseed, file, dir

	item, _, err = gw.Buckets().ListPath(context.Background(), buck.Thread, buck.Key, token, "dir")
	require.NoError(t, err)
	assert.True(t, item.IsDir)
	assert.Len(t, item.Items, 2) // file, dir

	item, _, err = gw.Buckets().ListPath(context.Background(), buck.Thread, buck.Key, token, "dir/dir")
	require.NoError(t, err)
	assert.True(t, item.IsDir)
	assert.Len(t, item.Items, 1) // file

	item, _, err = gw.Buckets().ListPath(context.Background(), buck.Thread, buck.Key, token, "file")
	require.NoError(t, err)
	assert.False(t, item.IsDir)
	assert.Equal(t, 1024, int(item.Size))

	item, _, err = gw.Buckets().ListPath(context.Background(), buck.Thread, buck.Key, token, "dir/file")
	require.NoError(t, err)
	assert.False(t, item.IsDir)
	assert.Equal(t, 1024, int(item.Size))

	item, _, err = gw.Buckets().ListPath(context.Background(), buck.Thread, buck.Key, token, "dir/dir/file")
	require.NoError(t, err)
	assert.False(t, item.IsDir)
	assert.Equal(t, 1024, int(item.Size))
}

func getRandomFile(t *testing.T, size int64) *os.File {
	tmp, err := ioutil.TempFile("", "")
	require.NoError(t, err)
	_, err = io.CopyN(tmp, rand.Reader, size)
	require.NoError(t, err)
	_, err = tmp.Seek(0, 0)
	require.NoError(t, err)
	return tmp
}

func pushBucketPaths(t *testing.T, url string, token did.Token, files map[string]*os.File) {
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	for pth, f := range files {
		fw, err := w.CreateFormFile("push", pth)
		require.NoError(t, err)
		_, err = io.Copy(fw, f)
		require.NoError(t, err)
	}
	err := w.Close()
	require.NoError(t, err)

	req, err := http.NewRequest("POST", url, &b)
	require.NoError(t, err)

	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+string(token))

	c := &http.Client{}
	res, err := c.Do(req)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, res.StatusCode)
}
