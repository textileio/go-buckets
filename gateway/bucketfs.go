package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	gopath "path"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"
	"github.com/textileio/go-assets"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/ipns"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

type fileSystem struct {
	*assets.FileSystem
}

func (f *fileSystem) Exists(prefix, path string) bool {
	pth := strings.TrimPrefix(path, prefix)
	if pth == "/" {
		return false
	}
	_, ok := f.Files[pth]
	return ok
}

type serveBucketFS interface {
	GetThread(key string) (core.ID, error)
	Exists(ctx context.Context, thread core.ID, bucket, pth string, token did.Token) (bool, string)
	Write(c *gin.Context, ctx context.Context, thread core.ID, bucket, pth string, token did.Token) error
	ValidHost() string
}

type bucketFS struct {
	lib    *buckets.Buckets
	ipns   *ipns.Manager
	domain string
}

func serveBucket(fs serveBucketFS) gin.HandlerFunc {
	return func(c *gin.Context) {
		key, err := bucketFromHost(c.Request.Host, fs.ValidHost())
		if err != nil {
			return
		}
		thread, err := fs.GetThread(key)
		if err != nil {
			return
		}
		token := did.Token(c.Query("token"))

		var content string
		ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
		defer cancel()
		exists, target := fs.Exists(ctx, thread, key, c.Request.URL.Path, token)
		if exists {
			content = c.Request.URL.Path
		} else if len(target) != 0 {
			content = gopath.Join(c.Request.URL.Path, target)
		}
		if len(content) != 0 {
			if err := fs.Write(c, ctx, thread, key, content, token); err != nil {
				renderError(c, http.StatusInternalServerError, err)
			} else {
				c.Abort()
			}
		}
	}
}

func (f *bucketFS) GetThread(bkey string) (id core.ID, err error) {
	key, err := f.ipns.Store().GetByCid(bkey)
	if err != nil {
		return
	}
	return key.ThreadID, nil
}

func (f *bucketFS) Exists(ctx context.Context, thread core.ID, key, pth string, token did.Token) (ok bool, name string) {
	if key == "" || pth == "/" {
		return
	}
	rep, _, err := f.lib.ListPath(ctx, thread, key, token, pth)
	if err != nil {
		return
	}
	if rep.IsDir {
		for _, item := range rep.Items {
			if item.Name == "index.html" {
				return false, item.Name
			}
		}
		return
	}
	return true, ""
}

func (f *bucketFS) Write(c *gin.Context, ctx context.Context, thread core.ID, key, pth string, token did.Token) error {
	r, err := f.lib.PullPath(ctx, thread, key, token, pth)
	if err != nil {
		return fmt.Errorf("pulling path: %v", err)
	}
	defer r.Close()

	ct, mr, err := detectReaderOrPathContentType(r, pth)
	if err != nil {
		return fmt.Errorf("detecting content-type: %v", err)
	}
	c.Writer.Header().Set("Content-Type", ct)
	c.Render(200, render.Reader{ContentLength: -1, Reader: mr})
	return nil
}

func (f *bucketFS) ValidHost() string {
	return f.domain
}

// renderWWWBucket renders a bucket as a website.
func (g *Gateway) renderWWWBucket(c *gin.Context, key string) {
	ipnskey, err := g.ipns.Store().GetByCid(key)
	if err != nil {
		render404(c)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	token := did.Token(c.Query("token"))
	rep, _, err := g.lib.ListPath(ctx, ipnskey.ThreadID, key, token, "")
	if err != nil {
		render404(c)
		return
	}
	for _, item := range rep.Items {
		if item.Name == "index.html" {
			r, err := g.lib.PullPath(ctx, ipnskey.ThreadID, key, token, item.Name)
			if err != nil {
				render404(c)
				return
			}

			ct, mr, err := detectReaderOrPathContentType(r, item.Name)
			if err != nil {
				renderError(c, http.StatusInternalServerError, fmt.Errorf("detecting content-type: %v", err))
				return
			}
			c.Writer.Header().Set("Content-Type", ct)
			c.Render(200, render.Reader{ContentLength: -1, Reader: mr})
			r.Close()
		}
	}
	renderError(c, http.StatusNotFound, errors.New("an index.html file was not found in this bucket"))
}

func bucketFromHost(host, valid string) (key string, err error) {
	parts := strings.SplitN(host, ".", 2)
	hostport := parts[len(parts)-1]
	hostparts := strings.SplitN(hostport, ":", 2)
	if hostparts[0] != valid || valid == "" {
		err = errors.New("invalid bucket host")
		return
	}
	return parts[0], nil
}
