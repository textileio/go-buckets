package gateway

// @todo: Migrate away from the current asset generator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-contrib/location"
	"github.com/gin-contrib/static"
	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	iface "github.com/ipfs/interface-go-ipfs-core"
	isd "github.com/jbenet/go-is-domain"
	"github.com/libp2p/go-libp2p-core/peer"
	mbase "github.com/multiformats/go-multibase"
	"github.com/rs/cors"
	gincors "github.com/rs/cors/wrapper/gin"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/ipns"
	"github.com/textileio/go-buckets/pinning"
	"github.com/textileio/go-threads/core/did"
	core "github.com/textileio/go-threads/core/thread"
)

var log = logging.Logger("buckets/gateway")

const handlerTimeout = time.Minute

func init() {
	gin.SetMode(gin.ReleaseMode)
}

// link is used for Unixfs directory templates.
type link struct {
	Name  string
	Path  string
	Size  string
	Links string
}

// Gateway provides HTTP-based access to buckets.
type Gateway struct {
	server *http.Server
	lib    *buckets.Buckets
	ipfs   iface.CoreAPI
	ipns   *ipns.Manager
	ps     *pinning.Service

	addr       string
	url        string
	domain     string
	subdomains bool
}

// Config defines the gateway configuration.
type Config struct {
	Addr       string
	URL        string
	Domain     string
	Subdomains bool
}

// NewGateway returns a new gateway.
func NewGateway(
	lib *buckets.Buckets,
	ipfs iface.CoreAPI,
	ipns *ipns.Manager,
	ps *pinning.Service,
	conf Config,
) (*Gateway, error) {
	return &Gateway{
		lib:        lib,
		ipfs:       ipfs,
		ipns:       ipns,
		ps:         ps,
		addr:       conf.Addr,
		url:        conf.URL,
		domain:     conf.Domain,
		subdomains: conf.Subdomains,
	}, nil
}

// Start the gateway.
func (g *Gateway) Start() {
	router := gin.Default()

	temp, err := loadTemplate()
	if err != nil {
		log.Fatal(err)
	}
	router.SetHTMLTemplate(temp)

	router.Use(location.Default())
	router.Use(static.Serve("", &fileSystem{Assets}))
	router.Use(serveBucket(&bucketFS{
		lib:    g.lib,
		ipns:   g.ipns,
		domain: g.domain,
	}))
	router.Use(gincors.New(cors.Options{}))

	router.GET("/health", func(c *gin.Context) {
		c.Writer.WriteHeader(http.StatusNoContent)
	})

	// IPFS
	router.GET("/ipfs/:root", g.subdomainOptionHandler, g.ipfsHandler)
	router.GET("/ipfs/:root/*path", g.subdomainOptionHandler, g.ipfsHandler)

	// IPNS
	router.GET("/ipns/:key", g.subdomainOptionHandler, g.ipnsHandler)
	router.GET("/ipns/:key/*path", g.subdomainOptionHandler, g.ipnsHandler)

	// P2P
	router.GET("/p2p/:key", g.subdomainOptionHandler, g.p2pHandler)

	// IPLD
	router.GET("/ipld/:root", g.subdomainOptionHandler, g.ipldHandler)
	router.GET("/ipld/:root/*path", g.subdomainOptionHandler, g.ipldHandler)

	// Buckets
	router.GET("/thread/:thread/buckets", g.subdomainOptionHandler, g.threadHandler)
	router.POST("/thread/:thread/buckets/:key", g.subdomainOptionHandler, g.bucketPushPathsHandler)
	router.GET("/thread/:thread/buckets/:key", g.subdomainOptionHandler, g.bucketHandler)
	router.GET("/thread/:thread/buckets/:key/*path", g.subdomainOptionHandler, g.bucketHandler)

	// Buckets shorthand
	router.POST("/b/:key", g.bucketPushPathsHandler)
	router.GET("/b/:key", g.bucketHandler)
	router.GET("/b/:key/*path", g.bucketHandler)

	// Buckets Pinning Service
	router.GET("/bps/:key/pins", g.subdomainOptionHandler, g.listPinsHandler)
	router.POST("/bps/:key/pins", g.subdomainOptionHandler, g.addPinHandler)
	router.GET("/bps/:key/pins/:requestid", g.subdomainOptionHandler, g.getPinHandler)
	router.POST("/bps/:key/pins/:requestid", g.subdomainOptionHandler, g.replacePinHandler)
	router.DELETE("/bps/:key/pins/:requestid", g.subdomainOptionHandler, g.removePinHandler)

	router.NoRoute(g.subdomainHandler)

	g.server = &http.Server{
		Addr:    g.addr,
		Handler: router,
	}
	go func() {
		if err := g.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("gateway error: %s", err)
		}
	}()
	log.Infof("gateway listening at %s", g.server.Addr)
}

// Close the gateway.
func (g *Gateway) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutting down server: %v", err)
	}
	return nil
}

// Addr returns the gateway's listen address.
func (g *Gateway) Addr() string {
	return g.server.Addr
}

// Url returns the gateway's externally configured URL.
func (g *Gateway) Url() string {
	return g.url
}

// Buckets returns the gateway's bucket lib.
func (g *Gateway) Buckets() *buckets.Buckets {
	return g.lib
}

// dashboardHandler renders a dev or org dashboard.
// @todo: Use or remove dashboard handler
func (g *Gateway) dashboardHandler(c *gin.Context) {
	render404(c)
}

// render404 renders the 404 template.
func render404(c *gin.Context) {
	c.HTML(http.StatusNotFound, "/public/html/404.gohtml", nil)
}

// renderError renders the error template.
func renderError(c *gin.Context, code int, err error) {
	c.HTML(code, "/public/html/error.gohtml", gin.H{
		"Code":  code,
		"Error": formatError(err),
	})
}

// formatError formats a go error for browser display.
func formatError(err error) string {
	words := strings.SplitN(err.Error(), " ", 2)
	words[0] = strings.Title(words[0])
	return strings.Join(words, " ") + "."
}

// getThread returns core.ID from request params or the IPNS key store.
func (g *Gateway) getThread(c *gin.Context) (core.ID, error) {
	threadp := c.Param("thread")
	if len(threadp) != 0 {
		thread, err := core.Decode(threadp)
		if err != nil {
			return "", errors.New("invalid thread ID")
		}
		return thread, nil
	}
	// @todo: Use a fixed length in-mem cache of thread IDs
	ipnskey, err := g.ipns.Store().GetByCid(c.Param("key"))
	if err != nil {
		return "", fmt.Errorf("looking up thread: %v", err)
	}
	return ipnskey.ThreadID, nil
}

// getAuth returns did.Token from the authorization header.
func getAuth(c *gin.Context) (did.Token, bool) {
	auth := strings.Split(c.Request.Header.Get("Authorization"), " ")
	if len(auth) < 2 {
		return "", false
	}
	return did.Token(auth[1]), true
}

// subdomainOptionHandler redirects valid namespaces to subdomains if the option is enabled.
func (g *Gateway) subdomainOptionHandler(c *gin.Context) {
	if !g.subdomains {
		return
	}
	loc, ok := g.toSubdomainURL(c.Request)
	if !ok {
		render404(c)
		return
	}

	// See security note:
	// https://github.com/ipfs/go-ipfs/blob/dbfa7bf2b216bad9bec1ff66b1f3814f4faac31e/core/corehttp/hostname.go#L105
	c.Request.Header.Set("Clear-Site-Data", "\"cookies\", \"storage\"")

	c.Redirect(http.StatusPermanentRedirect, loc)
}

// subdomainHandler handles requests by parsing the request subdomain.
func (g *Gateway) subdomainHandler(c *gin.Context) {
	c.Status(200)

	parts := strings.Split(c.Request.Host, ".")
	key := parts[0]

	// Render buckets if the domain matches
	if g.domain != "" && strings.HasSuffix(c.Request.Host, g.domain) {
		g.renderWWWBucket(c, key)
		return
	}

	if len(parts) < 3 {
		render404(c)
		return
	}
	ns := parts[1]
	if !isSubdomainNamespace(ns) {
		render404(c)
		return
	}
	switch ns {
	case "ipfs":
		g.renderIPFSPath(c, "ipfs/"+key, "/ipfs/"+key+c.Request.URL.Path)
	case "ipns":
		g.renderIPNSKey(c, key, c.Request.URL.Path)
	case "p2p":
		g.renderP2PKey(c, key)
	case "ipld":
		g.renderIPLDPath(c, key+c.Request.URL.Path)
	case "thread":
		thread, err := core.Decode(key)
		if err != nil {
			renderError(c, http.StatusBadRequest, errors.New("invalid thread ID"))
			return
		}
		parts := strings.SplitN(strings.TrimSuffix(c.Request.URL.Path, "/"), "/", 4)
		if len(parts) < 2 || parts[1] != "buckets" {
			render404(c)
			return
		}
		switch len(parts) {
		case 2:
			g.renderThread(c, thread)
		case 3:
			if c.Request.Method == "POST" {
				g.pushBucketPaths(c, thread, parts[2])
			} else {
				g.renderBucket(c, thread, parts[2], "")
			}
		case 4:
			g.renderBucket(c, thread, parts[2], parts[3])
		default:
			render404(c)
		}
	case "bps":
		parts := strings.SplitN(strings.TrimSuffix(c.Request.URL.Path, "/"), "/", 3)
		if len(parts) < 2 || parts[1] != "pins" {
			render404(c)
			return
		}
		switch len(parts) {
		case 2:
			if c.Request.Method == "POST" {
				g.addPin(c, key)
			} else {
				g.listPins(c, key)
			}
		case 3:
			if c.Request.Method == "POST" {
				g.replacePin(c, key, parts[2])
			} else if c.Request.Method == "DELETE" {
				g.removePin(c, key, parts[2])
			} else {
				g.getPin(c, key, parts[2])
			}
		default:
			render404(c)
		}
	default:
		render404(c)
	}
}

// Modified from:
// https://github.com/ipfs/go-ipfs/blob/dbfa7bf2b216bad9bec1ff66b1f3814f4faac31e/core/corehttp/hostname.go#L251
func isSubdomainNamespace(ns string) bool {
	switch ns {
	case "ipfs", "ipns", "p2p", "ipld", "thread", "bps":
		return true
	default:
		return false
	}
}

// Copied from:
// https://github.com/ipfs/go-ipfs/blob/dbfa7bf2b216bad9bec1ff66b1f3814f4faac31e/core/corehttp/hostname.go#L260
func isPeerIDNamespace(ns string) bool {
	switch ns {
	case "ipns", "p2p":
		return true
	default:
		return false
	}
}

// Converts a hostname/path to a subdomain-based URL, if applicable.
// Modified from:
// https://github.com/ipfs/go-ipfs/blob/dbfa7bf2b216bad9bec1ff66b1f3814f4faac31e/core/corehttp/hostname.go#L270
func (g *Gateway) toSubdomainURL(r *http.Request) (redirURL string, ok bool) {
	var ns, rootID, rest string

	query := r.URL.RawQuery
	parts := strings.SplitN(r.URL.Path, "/", 4)
	safeRedirectURL := func(in string) (out string, ok bool) {
		safeURI, err := url.ParseRequestURI(in)
		if err != nil {
			return "", false
		}
		return safeURI.String(), true
	}

	switch len(parts) {
	case 4:
		rest = parts[3]
		fallthrough
	case 3:
		ns = parts[1]
		rootID = parts[2]
	default:
		return "", false
	}

	if !isSubdomainNamespace(ns) {
		return "", false
	}

	// add prefix if query is present
	if query != "" {
		query = "?" + query
	}

	// Normalize problematic PeerIDs (eg. ed25519+identity) to CID representation
	if isPeerIDNamespace(ns) && !isd.IsDomain(rootID) {
		peerID, err := peer.Decode(rootID)
		// Note: PeerID CIDv1 with protobuf multicodec will fail, but we fix it
		// in the next block
		if err == nil {
			rootID = peer.ToCid(peerID).String()
		}
	}

	// If rootID is a CID, ensure it uses DNS-friendly text representation
	if rootCid, err := cid.Decode(rootID); err == nil {
		multicodec := rootCid.Type()

		// PeerIDs represented as CIDv1 are expected to have libp2p-key
		// multicodec (https://github.com/libp2p/specs/pull/209).
		// We ease the transition by fixing multicodec on the fly:
		// https://github.com/ipfs/go-ipfs/issues/5287#issuecomment-492163929
		if isPeerIDNamespace(ns) && multicodec != cid.Libp2pKey {
			multicodec = cid.Libp2pKey
		}

		// if object turns out to be a valid CID,
		// ensure text representation used in subdomain is CIDv1 in Base32
		// https://github.com/ipfs/in-web-browsers/issues/89
		rootID, err = cid.NewCidV1(multicodec, rootCid.Hash()).StringOfBase(mbase.Base32)
		if err != nil {
			// should not error, but if it does, its clealy not possible to
			// produce a subdomain URL
			return "", false
		}
	}

	urlparts := strings.Split(g.url, "://")
	if len(urlparts) < 2 {
		return "", false
	}
	scheme := urlparts[0]
	host := urlparts[1]
	return safeRedirectURL(fmt.Sprintf("%s://%s.%s.%s/%s%s", scheme, rootID, ns, host, rest, query))
}

// detectReaderOrPathContentType detects the best available mime type for a reader,
// considering file extensions. http.DetectContentType does not properly detect content-type
// for web assets: htm, html, css, and js.
func detectReaderOrPathContentType(r io.Reader, pth string) (string, io.Reader, error) {
	var buf [512]byte
	n, err := io.ReadAtLeast(r, buf[:], len(buf))
	if err != nil && err != io.ErrUnexpectedEOF {
		return "", nil, fmt.Errorf("reading reader: %s", err)
	}
	reader := io.MultiReader(bytes.NewReader(buf[:n]), r)
	switch filepath.Ext(pth) {
	case ".htm", ".html":
		return "text/html", reader, nil
	case ".css":
		return "text/css", reader, nil
	case ".js":
		return "application/javascript", reader, nil
	case ".txt", ".md":
		return "text/plain", reader, nil
	default:
		return http.DetectContentType(buf[:]), reader, nil
	}
}

// byteCountDecimal returns a human readable byte size.
func byteCountDecimal(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "kMGTPE"[exp])
}

// loadTemplate loads HTML templates.
func loadTemplate() (*template.Template, error) {
	t := template.New("")
	for name, file := range Assets.Files {
		if file.IsDir() || !strings.HasSuffix(name, ".gohtml") {
			continue
		}
		h, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}
		t, err = t.New(name).Parse(string(h))
		if err != nil {
			return nil, err
		}
	}
	return t, nil
}
