package gateway

import (
	"context"
	"fmt"
	"net/http"
	gopath "path"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

// bucketHandler handles bucket requests.
func (g *Gateway) bucketHandler(c *gin.Context) {
	thread, err := g.getThread(c)
	if err != nil {
		renderError(c, http.StatusBadRequest, err)
		return
	}
	g.renderBucket(c, thread, c.Param("key"), c.Param("path"))
}

// renderBucket renders a bucket instance in a collection.
func (g *Gateway) renderBucket(c *gin.Context, thread core.ID, key, pth string) {
	pth = strings.TrimPrefix(pth, "/")
	token := did.Token(c.Query("token"))

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	rep, buck, err := g.lib.ListPath(ctx, thread, key, token, pth)
	if err != nil {
		render404(c)
		return
	}
	if !rep.IsDir {
		r, err := g.lib.PullPath(ctx, thread, buck.Key, token, pth)
		if err != nil {
			render404(c)
			return
		}
		defer r.Close()

		ct, mr, err := detectReaderOrPathContentType(r, pth)
		if err != nil {
			renderError(c, http.StatusInternalServerError, fmt.Errorf("detecting content-type: %v", err))
			return
		}
		c.Writer.Header().Set("Content-Type", ct)
		c.Render(200, render.Reader{ContentLength: -1, Reader: mr})
	} else {
		var base string
		if !g.subdomains {
			base = collection.Name
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
		if len(buck.Name) != 0 {
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
