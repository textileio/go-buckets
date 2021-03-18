package gateway

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
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

	//g.ps.
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

//func (g *Gateway) getPins(c *gin.Context) {
//var q pinning.GetPinsQuery
//if err := c.ShouldBind(&q); err != nil {
//	c.JSON(http.StatusBadRequest, err.Error())
//	return
//}
//
//// Get all Pending jobs
//jobs, err := s.pinqueue.GetJobsBySub(sub, false)
//if err != nil {
//	s.log.Error(err)
//	returnInvalidAuthError(c, "there was an issue fetching jobs")
//	return
//}
//
//results := pinning.PinResults{
//	Count:   0,
//	Results: []pinning.PinStatus{},
//}
//
//for _, j := range jobs {
//	results.Count++
//	results.Results = append(results.Results, j.PinStatus)
//}
//
//ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//defer cancel()
//ctx, err = ctxWithThread(ctx, thrd)
//if err != nil {
//	s.log.Error(err)
//	returnError(c, err, "")
//	return
//}
//
//pth, err := s.buckets.ListPath(ctx, buck, "/.pins/requests")
//if err != nil {
//	s.log.Error(err)
//	// TODO: should only return nicely if the error was "no link named..."
//	c.JSON(200, results)
//	return
//}
//
//for _, res := range pth.Item.Items {
//	pin, err := s.getBuckPinByRequestID(ctx, buck, res.Name)
//	if err != nil {
//		s.log.Error(err)
//		continue
//	}
//	// TODO: Skip (?) requestid also in the queue as they are being replaced
//	results.Count++
//	results.Results = append(results.Results, *pin)
//}
//c.JSON(200, results)
//return
//}

//func (g *Gateway) PostPins(c *gin.Context) {
//	var pin Pin
//	if err := c.ShouldBind(&pin); err != nil {
//		c.JSON(http.StatusBadRequest, err.Error())
//		return
//	}
//
//}

//func (g *Gateway) DeletePinsRequestid(c *gin.Context) {
//	requestid := c.Param("requestid")
//	if requestid == "" {
//		c.JSON(http.StatusBadRequest, gin.H{"error": "missing requestid"})
//		return
//	}
//	h.Handler.DeletePinsRequestid(c, requestid)
//}
//
//// GetPinsRequestid operation middleware
//func (g *Gateway) GetPinsRequestid(c *gin.Context) {
//	requestid := c.Param("requestid")
//	if requestid == "" {
//		c.JSON(http.StatusBadRequest, gin.H{"error": "missing requestid"})
//		return
//	}
//	h.Handler.GetPinsRequestid(c, requestid)
//}
//
//// PostPinsRequestid operation middleware
//func (g *Gateway) PostPinsRequestid(c *gin.Context) {
//	requestid := c.Param("requestid")
//	if requestid == "" {
//		c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Errorf("missing request id")})
//		return
//	}
//
//	var pin Pin
//	if err := c.ShouldBind(&pin); err != nil {
//		c.JSON(http.StatusBadRequest, err.Error())
//		return
//	}
//
//	h.Handler.PostPinsRequestid(c, requestid, pin)
//}

//// ServerInterfaceImpl implements the routing service
//type ServerInterfaceImpl struct {
//	buckets  *bc.Client
//	pinqueue *requestq.Queue
//	secret   []byte
//	log      *logging.ZapEventLogger
//}

//func (g *Gateway) GetPins(c *gin.Context, params pinning.GetPinsParams) {
//	s.log.Debug("GetPins")
//	ok, thrd, buck, sub := parseContext(c)
//	if !ok {
//		returnInvalidAuthError(c, "there was an issue with your token")
//		return
//	}
//
//	// Get all Pending jobs
//	jobs, err := s.pinqueue.GetJobsBySub(sub, false)
//	if err != nil {
//		s.log.Error(err)
//		returnInvalidAuthError(c, "there was an issue fetching jobs")
//		return
//	}
//
//	results := pinning.PinResults{
//		Count:   0,
//		Results: []pinning.PinStatus{},
//	}
//
//	for _, j := range jobs {
//		results.Count++
//		results.Results = append(results.Results, j.PinStatus)
//	}
//
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	ctx, err = ctxWithThread(ctx, thrd)
//	if err != nil {
//		s.log.Error(err)
//		returnError(c, err, "")
//		return
//	}
//
//	pth, err := s.buckets.ListPath(ctx, buck, "/.pins/requests")
//	if err != nil {
//		s.log.Error(err)
//		// TODO: should only return nicely if the error was "no link named..."
//		c.JSON(200, results)
//		return
//	}
//
//	for _, res := range pth.Item.Items {
//		pin, err := s.getBuckPinByRequestID(ctx, buck, res.Name)
//		if err != nil {
//			s.log.Error(err)
//			continue
//		}
//		// TODO: Skip (?) requestid also in the queue as they are being replaced
//		results.Count++
//		results.Results = append(results.Results, *pin)
//	}
//	c.JSON(200, results)
//	return
//}

//func (g *Gateway) PostPins(c *gin.Context, pin pinning.Pin) {
//	s.log.Debug("PostPins")
//	ok, thrd, buck, sub := parseContext(c)
//	if !ok {
//		returnInvalidAuthError(c, "there was an issue with your token")
//		return
//	}
//
//	requestid := getNewID()
//
//	pinStatus := pinning.PinStatus{
//		Requestid: requestid,
//		Status:    "queued",
//		Created:   time.Now(),
//		Delegates: []string{},
//		Pin:       pin,
//	}
//
//	_, err := s.pinqueue.PushBytes(thrd, buck, sub, requestid, pinStatus)
//	if err != nil {
//		returnError(c, err, "")
//		return
//	}
//
//	c.JSON(200, pinStatus)
//}
//
//func (g *Gateway) GetPinsRequestid(c *gin.Context, requestid string) {
//	s.log.Debug("GetPinsRequestid %s", requestid)
//
//	ok, thrd, buck, sub := parseContext(c)
//	if !ok {
//		returnInvalidAuthError(c, "there is an issue with your token")
//		return
//	}
//
//	job, _, err := s.pinqueue.GetPendingJobByRequestID(sub, requestid)
//	if err != nil {
//		returnError(c, err, "")
//		return
//	}
//
//	if job != nil {
//		c.JSON(200, job.PinStatus)
//		return
//	}
//
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	ctx, err = ctxWithThread(ctx, thrd)
//	if err != nil {
//		s.log.Error(err)
//		returnError(c, err, "")
//		return
//	}
//
//	pin, err := s.getBuckPinByRequestID(ctx, buck, requestid)
//	if err != nil {
//		s.log.Error(err)
//		returnError(c, err, "")
//		return
//	}
//
//	c.JSON(200, pin)
//	return
//}
//
//func (g *Gateway) PostPinsRequestid(c *gin.Context, requestid string, pin pinning.Pin) {
//	s.log.Debug("PostPinsRequestid %s", requestid)
//
//	ok, thrd, buck, sub := parseContext(c)
//	if !ok {
//		returnInvalidAuthError(c, "there was an issue with your token")
//		return
//	}
//
//	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//	defer cancel()
//	ctx, err := ctxWithThread(ctx, thrd)
//	if err != nil {
//		s.log.Error(err)
//		returnError(c, err, "")
//		return
//	}
//
//	update, err := s.getBuckPinByRequestID(ctx, buck, requestid)
//	if err != nil {
//		s.log.Error(err)
//		returnError(c, err, "")
//		return
//	}
//
//	update.Status = "queued"
//	update.Pin = pin
//	_, err = s.pinqueue.PushBytes(thrd, buck, sub, requestid, *update)
//	if err != nil {
//		returnError(c, err, "")
//		return
//	}
//
//	c.JSON(200, update)
//}
//
//func (g *Gateway) DeletePinsRequestid(c *gin.Context, requestid string) {
//	s.log.Debug("DeletePinsRequestid %s", requestid)
//}
//
//func (g *Gateway) getBuckPinByRequestID(ctx context.Context, buck string, requestid string) (*pinning.PinStatus, error) {
//	var buf bytes.Buffer
//	pin := fmt.Sprintf("/.pins/requests/%s/pin.json", requestid)
//	if err := s.buckets.PullPath(ctx, buck, pin, &buf); err != nil {
//		// possibly mark for removal
//		return nil, err
//	}
//	var b pinning.PinStatus
//	if err := json.Unmarshal(buf.Bytes(), &b); err != nil {
//		// possibly mark for removal
//		return nil, err
//	}
//	return &b, nil
//}

//// Gets a new Key but strips the leading "/"
//func getNewID() string {
//	s := ds.RandomKey().String()
//	_, i := utf8.DecodeRuneInString(s)
//	return s[i:]
//}

//// parseContext gets the thread/bucket/sub mandatory fields set by Middleware
//func parseContext(c *gin.Context) (ok bool, thrd string, buck string, sub string) {
//	thrd, ok = c.MustGet("thread").(string)
//	if !ok {
//		return false, "", "", ""
//	}
//	buck, ok = c.MustGet("bucket").(string)
//	if !ok {
//		return false, "", "", ""
//	}
//	sub, ok = c.MustGet("sub").(string)
//	if !ok {
//		return false, "", "", ""
//	}
//	return true, thrd, buck, sub
//}
//
//func ctxWithThread(ctx context.Context, t string) (context.Context, error) {
//	tk, err := thread.Decode(t)
//	if err != nil {
//		return nil, err
//	}
//	return common.NewThreadIDContext(ctx, tk), nil
//}

// Attaches any auth provided by middleware
// func applyAuth(ctx context.Context, c *gin.Context) context.Context {
// 	var token thread.Token
// 	token, ok = c.MustGet("threadToken").(string)
// 	if ok {
// 		ctx = thread.NewTokenContext(ctx, token)
// 	}

// 	userGroupKey, ok := c.MustGet("userGroupKey").(string)
// 	userGroupSecret, ook := c.MustGet("userGroupSecret").(string)
// 	if ok && ook {
// 		secCtx, err := common.CreateAPISigContext(
// 			common.NewAPIKeyContext(ctx, userGroupKey),
// 			time.Now().Add(time.Minute),
// 			userGroupSecret,
// 		)
// 		if err == nil {
// 			return secCtx
// 		}
// 	}

// 	return ctx
// }

//const (
//	//InvalidAuth occurs when there is an issue with token auth
//	InvalidAuth = "invalid auth"
//	//InvalidCid occurs when there is an issue with a cid
//	InvalidCid = "invalid cid"
//	//BucketError occurs when there is an issue with a cid
//	BucketError = "issue reading bucket"
//)
//
//func returnInvalidAuthError(c *gin.Context, detail string) {
//	c.JSON(http.StatusBadRequest, pinning.CreateFailure(InvalidAuth, detail))
//	c.Abort()
//	return
//}
//
//func returnInvalidCidError(c *gin.Context, detail string) {
//	c.JSON(http.StatusBadRequest, pinning.CreateFailure(InvalidCid, detail))
//	c.Abort()
//	return
//}
//
//func returnBucketError(c *gin.Context, detail string) {
//	c.JSON(http.StatusBadRequest, pinning.CreateFailure(BucketError, detail))
//	c.Abort()
//	return
//}
//
//func returnError(c *gin.Context, err error, detail string) {
//	c.JSON(http.StatusBadRequest, pinning.CreateFailure(err.Error(), detail))
//	c.Abort()
//	return
//}
