package common

import (
	"crypto/tls"
	"errors"
	gnet "net"
	"net/http"
	"strings"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
	logging "github.com/ipfs/go-log/v2"
	"github.com/textileio/go-buckets"
	"github.com/textileio/go-buckets/api"
	"github.com/textileio/go-buckets/api/pb"
	"github.com/textileio/go-threads/core/did"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var log = logging.Logger("buckets-api")

func GetClientRPCOpts(target string) (opts []grpc.DialOption) {
	creds := did.RPCCredentials{}
	if strings.Contains(target, "443") {
		tcreds := credentials.NewTLS(&tls.Config{})
		opts = append(opts, grpc.WithTransportCredentials(tcreds))
		creds.Secure = true
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	opts = append(opts, grpc.WithPerRPCCredentials(creds))
	return opts
}

func GetServerAndProxy(lib *buckets.Buckets, listenAddr, listenAddrProxy string) (*grpc.Server, *http.Server, error) {
	server := grpc.NewServer()
	listener, err := gnet.Listen("tcp", listenAddr)
	if err != nil {
		return nil, nil, err
	}
	go func() {
		pb.RegisterAPIServiceServer(server, api.NewService(lib))
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Errorf("server error: %v", err)
		}
	}()
	webrpc := grpcweb.WrapServer(
		server,
		grpcweb.WithOriginFunc(func(origin string) bool {
			return true
		}),
		grpcweb.WithAllowedRequestHeaders([]string{"Origin"}),
		grpcweb.WithWebsockets(true),
		grpcweb.WithWebsocketOriginFunc(func(req *http.Request) bool {
			return true
		}))
	proxy := &http.Server{
		Addr: listenAddrProxy,
	}
	proxy.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if webrpc.IsGrpcWebRequest(r) ||
			webrpc.IsAcceptableGrpcCorsRequest(r) ||
			webrpc.IsGrpcWebSocketRequest(r) {
			webrpc.ServeHTTP(w, r)
		}
	})
	go func() {
		if err := proxy.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("proxy error: %v", err)
		}
	}()
	return server, proxy, nil
}
