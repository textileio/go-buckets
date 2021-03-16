package gateway

import (
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	gopath "path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ipfs/interface-go-ipfs-core/path"
	assets "github.com/textileio/go-assets"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-buckets/ipns"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

var UploadTimeout = time.Hour

const chunkSize = 1024 * 32

type PostError struct {
	Error string `json:"error"`
}

type chanErr struct {
	code int
	err  error
}

type PushPathsResult struct {
	Path string `json:"path"`
	Cid  string `json:"cid"`
	Size int64  `json:"size"`
}

type PushPathsResults struct {
	Results []PushPathsResult `json:"results"`
	Pinned  int64             `json:"pinned"`
	Bucket  *buckets.Bucket   `json:"bucket"`
}

func (g *Gateway) pushPaths(c *gin.Context) {
	thread, err := getThread(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, PostError{
			Error: fmt.Sprintf("invalid thread ID: %v", err),
		})
		return
	}
	key := getKey(c)
	identity, ok := getIdentity(c)
	if !ok {
		c.AbortWithStatusJSON(http.StatusUnauthorized, PostError{
			Error: fmt.Sprintf("authorization required"),
		})
		return
	}

	// @todo: get root from path
	var root path.Resolved

	_, params, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, PostError{
			Error: fmt.Sprintf("parsing content-type: %v", err),
		})
		return
	}
	boundary, ok := params["boundary"]
	if !ok {
		c.AbortWithStatusJSON(http.StatusBadRequest, PostError{
			Error: "invalid multipart boundary",
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), UploadTimeout)
	defer cancel()
	in, out, errs := g.lib.PushPaths(ctx, thread, key, root, identity)
	if len(errs) != 0 {
		err := <-errs
		c.AbortWithStatusJSON(http.StatusBadRequest, PostError{
			Error: fmt.Sprintf("starting push: %v", err),
		})
		return
	}

	errCh := make(chan chanErr)
	go func() {
		defer close(in)
		mr := multipart.NewReader(c.Request.Body, boundary)
		buf := make([]byte, chunkSize)
		for {
			part, err := mr.NextPart()
			if err == io.EOF {
				return
			} else if err != nil {
				errCh <- chanErr{
					code: http.StatusInternalServerError,
					err:  fmt.Errorf("reading part: %v", err),
				}
				return
			}
			for {
				n, err := part.Read(buf)
				input := buckets.PushPathsInput{
					Path: part.FileName(),
				}
				if n > 0 {
					input.Chunk = make([]byte, n)
					copy(input.Chunk, buf[:n])
					in <- input
				} else if err == io.EOF {
					in <- input
					part.Close()
					break
				} else if err != nil {
					errCh <- chanErr{
						code: http.StatusInternalServerError,
						err:  fmt.Errorf("reading part: %v", err),
					}
					part.Close()
					return
				}
			}
		}
	}()

	results := PushPathsResults{}
	for {
		select {
		case res := <-out:
			results.Results = append(results.Results, PushPathsResult{
				Path: res.Path,
				Cid:  res.Cid.String(),
				Size: res.Size,
			})
			results.Bucket = res.Bucket
			results.Pinned = res.Pinned
		case err := <-errs:
			if err != nil {
				c.AbortWithStatusJSON(http.StatusBadRequest, PostError{
					Error: err.Error(),
				})
			} else {
				c.JSON(http.StatusCreated, results)
			}
			return
		case err := <-errCh:
			c.AbortWithStatusJSON(err.code, PostError{
				Error: err.err.Error(),
			})
			return
		}
	}
}

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

func (g *Gateway) renderBucket(c *gin.Context, ctx context.Context, thread core.ID, token did.Token) {
	rep, err := g.lib.List(ctx, thread, token)
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
		p := gopath.Join("thread", thread.String(), collection.Name, r.Key)
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
		"Title":   "Index of " + gopath.Join("/thread", thread.String(), collection.Name),
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
	thread core.ID,
	id,
	pth string,
	token did.Token,
) {
	rep, buck, err := g.lib.ListPath(ctx, thread, id, pth, token)
	if err != nil {
		render404(c)
		return
	}
	if !rep.IsDir {
		r, err := g.lib.PullPath(ctx, thread, buck.Key, pth, token)
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
			base = gopath.Join("thread", thread.String(), collection.Name)
		}
		var links []link
		for _, item := range rep.Items {
			pth := gopath.Join(base, strings.Replace(item.Path, buck.Path, buck.Key, 1))
			if token.Defined() {
				pth += "?token=" + string(token)
			}
			links = append(links, link{
				Name:  item.Name,
				Path:  pth,
				Size:  byteCountDecimal(item.Size),
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
		back := gopath.Dir(gopath.Join(base, strings.Replace(rep.Path, buck.Path, buck.Key, 1)))
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
