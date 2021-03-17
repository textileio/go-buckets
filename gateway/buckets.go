package gateway

import (
	"context"
	"io"
	"net/http"
	gopath "path"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

func (g *Gateway) bucketHandler(c *gin.Context) {
	g.renderBucket(c, c.Param("key"), c.Param("path"))
}

// renderInstance renders a bucket instance in a collection.
func (g *Gateway) renderBucket(c *gin.Context, id, pth string) {
	pth = strings.TrimPrefix(pth, "/")
	token := did.Token(c.Query("token"))

	ipnskey, err := g.ipns.Store().GetByCid(id)
	if err != nil {
		render404(c)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	g.renderBucketPath(c, ctx, ipnskey.ThreadID, id, pth, token)
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
