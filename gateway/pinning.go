package gateway

import (
	"errors"
	"net/http"

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

	pins, err := g.ps.ListPins(thread, key, oapiQueryToQuery(query), token)
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
		before, after string
		limit         int
		meta          map[string]string
	)
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
		Status: q.Status,
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

	status, err := g.ps.AddPin(thread, key, pin, token)
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

	status, err := g.ps.GetPin(thread, key, id, token)
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

	status, err := g.ps.ReplacePin(thread, key, id, pin, token)
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

	if err := g.ps.RemovePin(thread, key, id, token); errors.Is(pinning.ErrPinNotFound, err) {
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
