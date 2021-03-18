package gateway

import (
	"context"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/textileio/go-buckets"
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
	thread, key, err := g.getThreadAndKey(c)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, PostError{
			Error: err.Error(),
		})
		return
	}
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
