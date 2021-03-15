package gateway

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	col "github.com/textileio/go-buckets/collection"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
	"github.com/textileio/go-threads/db"
)

// collectionHandler handles collection requests.
func (g *Gateway) collectionHandler(c *gin.Context) {
	thread, err := core.Decode(c.Param("thread"))
	if err != nil {
		renderError(c, http.StatusBadRequest, fmt.Errorf("invalid thread ID"))
		return
	}
	g.renderCollection(c, thread, c.Param("collection"))
}

// renderCollection renders all instances in a collection.
func (g *Gateway) renderCollection(c *gin.Context, thread core.ID, collection string) {
	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	token := did.Token(c.Query("token"))

	jsn := c.Query("json") == "true"
	if collection == col.Name && !jsn {
		g.renderBucket(c, ctx, thread, token)
		return
	} else {
		var dummy interface{}
		res, err := g.lib.DB().Find(ctx, thread, collection, &db.Query{}, &dummy, db.WithTxnToken(token))
		if err != nil {
			render404(c)
			return
		}
		c.JSON(http.StatusOK, res)
	}
}

// instanceHandler handles collection instance requests.
func (g *Gateway) instanceHandler(c *gin.Context) {
	thread, err := core.Decode(c.Param("thread"))
	if err != nil {
		renderError(c, http.StatusBadRequest, fmt.Errorf("invalid thread ID"))
		return
	}
	g.renderInstance(c, thread, c.Param("collection"), c.Param("id"), c.Param("path"))
}

// renderInstance renders an instance in a collection.
// If the collection is buckets, the built-in buckets UI in rendered instead.
// This can be overridden with the query param json=true.
func (g *Gateway) renderInstance(c *gin.Context, thread core.ID, collection, id, pth string) {
	pth = strings.TrimPrefix(pth, "/")
	jsn := c.Query("json") == "true"
	if (collection != col.Name || jsn) && pth != "" {
		render404(c)
		return
	}
	token := did.Token(c.Query("token"))

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	if collection == col.Name && !jsn {
		g.renderBucketPath(c, ctx, thread, id, pth, token)
		return
	} else {
		var res interface{}
		if err := g.lib.DB().FindByID(ctx, thread, collection, id, &res, db.WithTxnToken(token)); err != nil {
			render404(c)
			return
		}
		c.JSON(http.StatusOK, res)
	}
}
