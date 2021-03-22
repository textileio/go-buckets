package gateway

import (
	"context"
	"errors"
	"net/http"
	gopath "path"

	"github.com/gin-gonic/gin"
	"github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

// threadHandler handles thread requests.
func (g *Gateway) threadHandler(c *gin.Context) {
	thread, err := core.Decode(c.Param("thread"))
	if err != nil {
		renderError(c, http.StatusBadRequest, errors.New("invalid thread ID"))
		return
	}
	g.renderThread(c, thread)
}

// renderThread renders all buckets in a thread.
func (g *Gateway) renderThread(c *gin.Context, thread core.ID) {
	token := did.Token(c.Query("token"))

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	rep, err := g.lib.List(ctx, thread, token)
	if err != nil {
		render404(c)
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
		p := gopath.Join(collection.Name, r.Key)
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
		"Title":   "Index of " + gopath.Join("/thread", thread.String()),
		"Root":    "/",
		"Path":    "",
		"Updated": "",
		"Back":    "",
		"Links":   links,
	})
}
