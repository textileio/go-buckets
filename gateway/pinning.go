package gateway

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/textileio/go-buckets/pinning"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
	"github.com/textileio/go-buckets/pinning/queue"
)

func (g *Gateway) listPinsHandler(c *gin.Context) {
	g.listPins(c, c.Param("key"))
}

func (g *Gateway) listPins(c *gin.Context, key string) {
	thread, err := g.getThread(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	token, ok := getAuth(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}

	var query openapi.Query
	if err := c.ShouldBindQuery(&query); err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	pins, err := g.ps.ListPins(ctx, thread, key, token, oapiQueryToQuery(query))
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	res := openapi.PinResults{
		Count:   int32(len(pins)),
		Results: pins,
	}
	c.JSON(http.StatusOK, res)
}

func oapiQueryToQuery(q openapi.Query) queue.Query {
	var (
		match         openapi.TextMatchingStrategy
		statuses      []openapi.Status
		before, after time.Time
		limit         int
		meta          map[string]string
	)
	switch q.Match {
	case "exact":
		match = openapi.EXACT
	case "iexact":
		match = openapi.IEXACT
	case "partial":
		match = openapi.PARTIAL
	case "ipartial":
		match = openapi.IPARTIAL
	}
	if len(q.Status) != 0 {
		for _, p := range strings.Split(q.Status, ",") {
			var s openapi.Status
			switch p {
			case "queued":
				s = openapi.QUEUED
			case "pinning":
				s = openapi.PINNING
			case "pinned":
				s = openapi.PINNED
			case "failed":
				s = openapi.FAILED
			default:
				continue
			}
			statuses = append(statuses, s)
		}
	}
	if q.Before != nil {
		before = *q.Before
	}
	if q.After != nil {
		after = *q.After
	}
	if q.Limit != nil {
		limit = int(*q.Limit)
	}
	if q.Meta != nil {
		meta = *q.Meta
	}
	return queue.Query{
		Cids:     q.Cid,
		Name:     q.Name,
		Match:    match,
		Statuses: statuses,
		Before:   before,
		After:    after,
		Limit:    limit,
		Meta:     meta,
	}
}

func (g *Gateway) addPinHandler(c *gin.Context) {
	g.addPin(c, c.Param("key"))
}

func (g *Gateway) addPin(c *gin.Context, key string) {
	thread, err := g.getThread(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	token, ok := getAuth(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}

	var pin openapi.Pin
	if err := c.ShouldBind(&pin); err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	status, err := g.ps.AddPin(ctx, thread, key, token, pin)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	c.JSON(http.StatusAccepted, status)
}

func (g *Gateway) getPinHandler(c *gin.Context) {
	g.getPin(c, c.Param("key"), c.Param("requestid"))
}

func (g *Gateway) getPin(c *gin.Context, key, id string) {
	thread, err := g.getThread(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	token, ok := getAuth(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	status, err := g.ps.GetPin(ctx, thread, key, token, id)
	if errors.Is(pinning.ErrPinNotFound, err) {
		newFailure(c, http.StatusNotFound, err)
		return
	} else if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	c.JSON(http.StatusOK, status)
}

func (g *Gateway) replacePinHandler(c *gin.Context) {
	g.replacePin(c, c.Param("key"), c.Param("requestid"))
}

func (g *Gateway) replacePin(c *gin.Context, key, id string) {
	thread, err := g.getThread(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	token, ok := getAuth(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}

	var pin openapi.Pin
	if err := c.ShouldBind(&pin); err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	status, err := g.ps.ReplacePin(ctx, thread, key, token, id, pin)
	if errors.Is(pinning.ErrPinNotFound, err) {
		newFailure(c, http.StatusNotFound, err)
		return
	} else if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	c.JSON(http.StatusAccepted, status)
}

func (g *Gateway) removePinHandler(c *gin.Context) {
	g.removePin(c, c.Param("key"), c.Param("requestid"))
}

func (g *Gateway) removePin(c *gin.Context, key, id string) {
	thread, err := g.getThread(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	token, ok := getAuth(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	if err := g.ps.RemovePin(ctx, thread, key, token, id); errors.Is(pinning.ErrPinNotFound, err) {
		newFailure(c, http.StatusNotFound, err)
		return
	} else if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	c.Status(http.StatusAccepted)
}

func newFailure(c *gin.Context, code int, err error) {
	c.JSON(code, openapi.Failure{
		Error: openapi.FailureError{
			Reason:  http.StatusText(code),
			Details: err.Error(),
		},
	})
}
