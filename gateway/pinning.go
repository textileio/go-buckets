package gateway

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/textileio/go-buckets/pinning"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
	"github.com/textileio/go-buckets/pinning/queue"
)

var queryMapRx *regexp.Regexp

func init() {
	queryMapRx = regexp.MustCompile(`\[(.*?)]`)
}

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
		newFailure(c, http.StatusUnauthorized, errors.New("authorization required"))
		return
	}

	var query openapi.Query
	if err := c.ShouldBindQuery(&query); err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	sq := getQuery(query)

	if m, ok := c.GetQuery("meta"); ok {
		sq.Meta = getQueryMap(m)
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	pins, err := g.ps.ListPins(ctx, thread, key, token, sq)
	if err != nil {
		handleServiceErr(c, err)
		return
	}

	res := openapi.PinResults{
		Count:   int32(len(pins)),
		Results: pins,
	}
	c.JSON(http.StatusOK, res)
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
		newFailure(c, http.StatusUnauthorized, errors.New("authorization required"))
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
		handleServiceErr(c, err)
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
		newFailure(c, http.StatusUnauthorized, errors.New("authorization required"))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	status, err := g.ps.GetPin(ctx, thread, key, token, id)
	if err != nil {
		handleServiceErr(c, err)
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
		newFailure(c, http.StatusUnauthorized, errors.New("authorization required"))
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
	if err != nil {
		handleServiceErr(c, err)
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
		newFailure(c, http.StatusUnauthorized, errors.New("authorization required"))
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), handlerTimeout)
	defer cancel()
	if err := g.ps.RemovePin(ctx, thread, key, token, id); err != nil {
		handleServiceErr(c, err)
		return
	}

	c.Status(http.StatusAccepted)
}

func getQuery(q openapi.Query) queue.Query {
	var (
		match    openapi.TextMatchingStrategy
		statuses []openapi.Status
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
	return queue.Query{
		Cids:     q.Cid,
		Name:     q.Name,
		Match:    match,
		Statuses: statuses,
		Before:   q.Before,
		After:    q.After,
		Limit:    int(q.Limit),
	}
}

func getQueryMap(s string) map[string]string {
	m := make(map[string]string)
	match := queryMapRx.FindStringSubmatch(s)
	if len(match) != 2 {
		return m
	}
	for _, p := range strings.Split(match[1], " ") {
		parts := strings.Split(p, ":")
		if len(parts) == 2 {
			m[parts[0]] = parts[1]
		}
	}
	return m
}

func newFailure(c *gin.Context, code int, err error) {
	c.JSON(code, openapi.Failure{
		Error: openapi.FailureError{
			Reason:  http.StatusText(code),
			Details: err.Error(),
		},
	})
}

func handleServiceErr(c *gin.Context, err error) {
	if errors.Is(err, queue.ErrNotFound) {
		newFailure(c, http.StatusNotFound, err)
		return
	} else if strings.Contains(err.Error(), "parsing token") {
		newFailure(c, http.StatusUnauthorized, err)
		return
	} else if errors.Is(err, pinning.ErrPermissionDenied) {
		newFailure(c, http.StatusForbidden, err)
		return
	} else {
		newFailure(c, http.StatusInternalServerError, err)
		return
	}
}
