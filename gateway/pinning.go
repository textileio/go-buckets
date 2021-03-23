package gateway

import (
	"context"
	"errors"
	"net/http"
	"strings"

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
	pins, err := g.ps.ListPins(ctx, thread, key, oapiQueryToQuery(query), token)
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

// @todo: Validate Cids
func oapiQueryToQuery(q openapi.Query) queue.Query {
	var (
		status        []openapi.Status
		before, after string
		limit         int
		meta          map[string]string
	)
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
			status = append(status, s)
		}
	}
	if q.Before != nil {
		before = queue.NewIDFromTime(*q.Before)
	}
	if q.After != nil {
		after = queue.NewIDFromTime(*q.After)
	}
	if q.Limit != nil {
		limit = int(*q.Limit)
	}
	if q.Meta != nil {
		meta = *q.Meta
	}
	return queue.Query{
		Cid:    q.Cid,
		Name:   q.Name,
		Match:  q.Match,
		Status: status,
		Before: before,
		After:  after,
		Limit:  limit,
		Meta:   meta,
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
	status, err := g.ps.AddPin(ctx, thread, key, pin, token)
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
	status, err := g.ps.GetPin(ctx, thread, key, id, token)
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
	status, err := g.ps.ReplacePin(ctx, thread, key, id, pin, token)
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
	if err := g.ps.RemovePin(ctx, thread, key, id, token); errors.Is(pinning.ErrPinNotFound, err) {
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
