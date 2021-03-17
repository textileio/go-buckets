package gateway

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	gopath "path"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
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
	Write(ctx context.Context, thread core.ID, bucket, pth string, token did.Token, writer io.Writer) error
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

		ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
		defer cancel()
		exists, target := fs.Exists(ctx, thread, key, c.Request.URL.Path, token)
		if exists {
			c.Writer.WriteHeader(http.StatusOK)
			ctype := mime.TypeByExtension(filepath.Ext(c.Request.URL.Path))
			if ctype == "" {
				ctype = "application/octet-stream"
			}
			c.Writer.Header().Set("Content-Type", ctype)
			if err := fs.Write(ctx, thread, key, c.Request.URL.Path, token, c.Writer); err != nil {
				renderError(c, http.StatusInternalServerError, err)
			} else {
				c.Abort()
			}
		} else if target != "" {
			content := gopath.Join(c.Request.URL.Path, target)
			ctype := mime.TypeByExtension(filepath.Ext(content))
			c.Writer.WriteHeader(http.StatusOK)
			c.Writer.Header().Set("Content-Type", ctype)
			if err := fs.Write(ctx, thread, key, content, token, c.Writer); err != nil {
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
	rep, _, err := f.lib.ListPath(ctx, thread, key, pth, token)
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

func (f *bucketFS) Write(ctx context.Context, thread core.ID, key, pth string, token did.Token, writer io.Writer) error {
	r, err := f.lib.PullPath(ctx, thread, key, pth, token)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(writer, r)
	return err
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
	rep, _, err := g.lib.ListPath(ctx, ipnskey.ThreadID, key, "", token)
	if err != nil {
		render404(c)
		return
	}
	for _, item := range rep.Items {
		if item.Name == "index.html" {
			c.Writer.WriteHeader(http.StatusOK)
			c.Writer.Header().Set("Content-Type", "text/html")
			r, err := g.lib.PullPath(ctx, ipnskey.ThreadID, key, item.Name, token)
			if err != nil {
				render404(c)
				return
			}
			if _, err := io.Copy(c.Writer, r); err != nil {
				r.Close()
				render404(c)
				return
			}
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
