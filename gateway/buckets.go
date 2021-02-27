package gateway

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	assets "github.com/textileio/go-assets"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/ipns"
	"github.com/textileio/go-buckets/util"
	"github.com/textileio/go-threads/core/did"
	"github.com/textileio/go-threads/core/thread"
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

func (g *Gateway) renderBucket(c *gin.Context, ctx context.Context, threadID thread.ID, token did.Token) {
	rep, err := g.lib.List(ctx, threadID, token)
	if err != nil {
		renderError(c, http.StatusBadRequest, err)
		return
	}
	links := make([]link, len(rep))
	for i, r := range rep {
		var name string
		if r.Name != "" {
			name = r.Name
		} else {
			name = r.Key
		}
		p := path.Join("thread", threadID.String(), collection.Name, r.Key)
		if token.Defined() {
			p += "?token=" + string(token)
		}
		links[i] = link{
			Name:  name,
			Path:  p,
			Size:  "",
			Links: "",
		}
	}
	c.HTML(http.StatusOK, "/public/html/unixfs.gohtml", gin.H{
		"Title":   "Index of " + path.Join("/thread", threadID.String(), collection.Name),
		"Root":    "/",
		"Path":    "",
		"Updated": "",
		"Back":    "",
		"Links":   links,
	})
}

func (g *Gateway) renderBucketPath(
	c *gin.Context,
	ctx context.Context,
	threadID thread.ID,
	id,
	pth string,
	token did.Token,
) {
	rep, buck, err := g.lib.ListPath(ctx, threadID, id, pth, token)
	if err != nil {
		render404(c)
		return
	}
	if !rep.IsDir {
		r, err := g.lib.PullPath(ctx, threadID, buck.Key, pth, token)
		if err != nil {
			render404(c)
			return
		}
		defer r.Close()
		if _, err := io.Copy(c.Writer, r); err != nil {
			render404(c)
			return
		}
	} else {
		var base string
		if g.subdomains {
			base = collection.Name
		} else {
			base = path.Join("thread", threadID.String(), collection.Name)
		}
		var links []link
		for _, item := range rep.Items {
			pth := path.Join(base, strings.Replace(item.Path, buck.Path, buck.Key, 1))
			if token.Defined() {
				pth += "?token=" + string(token)
			}
			links = append(links, link{
				Name:  item.Name,
				Path:  pth,
				Size:  util.ByteCountDecimal(item.Size),
				Links: strconv.Itoa(len(item.Items)),
			})
		}
		var name string
		if buck.Name != "" {
			name = buck.Name
		} else {
			name = buck.Key
		}
		root := strings.Replace(rep.Path, buck.Path, name, 1)
		back := path.Dir(path.Join(base, strings.Replace(rep.Path, buck.Path, buck.Key, 1)))
		if token.Defined() {
			back += "?token=" + string(token)
		}
		c.HTML(http.StatusOK, "/public/html/unixfs.gohtml", gin.H{
			"Title":   "Index of /" + root,
			"Root":    "/" + root,
			"Path":    rep.Path,
			"Updated": time.Unix(0, buck.UpdatedAt).String(),
			"Back":    back,
			"Links":   links,
		})
	}
}

type serveBucketFS interface {
	GetThread(key string) (thread.ID, error)
	Exists(ctx context.Context, threadID thread.ID, bucket, pth string, token did.Token) (bool, string)
	Write(ctx context.Context, threadID thread.ID, bucket, pth string, token did.Token, writer io.Writer) error
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
		threadID, err := fs.GetThread(key)
		if err != nil {
			return
		}
		token := did.Token(c.Query("token"))

		ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
		defer cancel()
		exists, target := fs.Exists(ctx, threadID, key, c.Request.URL.Path, token)
		if exists {
			c.Writer.WriteHeader(http.StatusOK)
			ctype := mime.TypeByExtension(filepath.Ext(c.Request.URL.Path))
			if ctype == "" {
				ctype = "application/octet-stream"
			}
			c.Writer.Header().Set("Content-Type", ctype)
			if err := fs.Write(ctx, threadID, key, c.Request.URL.Path, token, c.Writer); err != nil {
				renderError(c, http.StatusInternalServerError, err)
			} else {
				c.Abort()
			}
		} else if target != "" {
			content := path.Join(c.Request.URL.Path, target)
			ctype := mime.TypeByExtension(filepath.Ext(content))
			c.Writer.WriteHeader(http.StatusOK)
			c.Writer.Header().Set("Content-Type", ctype)
			if err := fs.Write(ctx, threadID, key, content, token, c.Writer); err != nil {
				renderError(c, http.StatusInternalServerError, err)
			} else {
				c.Abort()
			}
		}
	}
}

func (f *bucketFS) GetThread(bkey string) (id thread.ID, err error) {
	key, err := f.ipns.Store().GetByCid(bkey)
	if err != nil {
		return
	}
	return key.ThreadID, nil
}

func (f *bucketFS) Exists(ctx context.Context, threadID thread.ID, key, pth string, token did.Token) (ok bool, name string) {
	if key == "" || pth == "/" {
		return
	}
	rep, _, err := f.lib.ListPath(ctx, threadID, key, pth, token)
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

func (f *bucketFS) Write(ctx context.Context, threadID thread.ID, key, pth string, token did.Token, writer io.Writer) error {
	r, err := f.lib.PullPath(ctx, threadID, key, pth, token)
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
	renderError(c, http.StatusNotFound, fmt.Errorf("an index.html file was not found in this bucket"))
}

func bucketFromHost(host, valid string) (key string, err error) {
	parts := strings.SplitN(host, ".", 2)
	hostport := parts[len(parts)-1]
	hostparts := strings.SplitN(hostport, ":", 2)
	if hostparts[0] != valid || valid == "" {
		err = fmt.Errorf("invalid bucket host")
		return
	}
	return parts[0], nil
}
