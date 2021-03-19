package gateway

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/textileio/go-buckets/pinning"
	openapi "github.com/textileio/go-buckets/pinning/openapi/go"
)

func (g *Gateway) listPins(c *gin.Context) {
	thread, key, err := g.getThreadAndKey(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	identity, ok := getIdentity(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}

	var query pinning.Query
	if err := c.ShouldBindQuery(&query); err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	pins, err := g.ps.ListPins(thread, key, query, identity)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	res := openapi.PinResults{
		Count:   int32(len(pins)),
		Results: pins,
	}
	c.JSON(200, res)
}

func (g *Gateway) addPin(c *gin.Context) {
	thread, key, err := g.getThreadAndKey(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	identity, ok := getIdentity(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}

	var pin openapi.Pin
	if err := c.ShouldBind(&pin); err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	status, err := g.ps.AddPin(thread, key, pin, identity)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	c.JSON(http.StatusAccepted, status)
}

func (g *Gateway) getPin(c *gin.Context) {
	thread, key, err := g.getThreadAndKey(c)
	if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}
	identity, ok := getIdentity(c)
	if !ok {
		newFailure(c, http.StatusBadRequest, errors.New("authorization required"))
		return
	}
	id := c.Param("requestid")

	status, err := g.ps.GetPin(thread, key, id, identity)
	if errors.Is(pinning.ErrPinNotFound, err) {
		newFailure(c, http.StatusNotFound, err)
		return
	} else if err != nil {
		newFailure(c, http.StatusBadRequest, err)
		return
	}

	c.JSON(http.StatusAccepted, status)
}

func (g *Gateway) replacePin(c *gin.Context) {

}

func (g *Gateway) removePin(c *gin.Context) {

}

func newFailure(c *gin.Context, code int, err error) {
	c.JSON(code, openapi.Failure{
		Error: openapi.FailureError{
			Reason:  http.StatusText(code),
			Details: err.Error(),
		},
	})
}
