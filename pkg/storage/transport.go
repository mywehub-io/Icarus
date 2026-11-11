package storage

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"golang.org/x/net/http2"
)

// MaxIdleConnsPerStorageHost is how many idle connections the blob clients keep open to one
// storage account.
//
// azcore's default transport keeps 10. Every unit on a pod reads and writes the same account,
// and the document lane writes node documents concurrently alongside the units' own reads and
// writes, so with 10 most requests past the tenth would open a new TLS connection and throw it
// away after use.
const MaxIdleConnsPerStorageHost = 64

// sharedHTTPClient is the one HTTP client every blob client in the process uses: the service
// client and the per-URL clients built for ranged reads, so all of them draw on one pool of warm
// connections.
var sharedHTTPClient = newSharedHTTPClient()

// newSharedHTTPClient copies azcore's default HTTP client (azcore runtime
// transport_default_http_client.go), HTTP/2 health-check ping included, except for the idle
// pool: MaxIdleConnsPerStorageHost per host, and a total that leaves room for it.
func newSharedHTTPClient() *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          2 * MaxIdleConnsPerStorageHost,
		MaxIdleConnsPerHost:   MaxIdleConnsPerStorageHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSClientConfig: &tls.Config{
			MinVersion:    tls.VersionTLS12,
			Renegotiation: tls.RenegotiateFreelyAsClient,
		},
	}
	if h2, err := http2.ConfigureTransports(transport); err == nil {
		// As azcore: ping a connection idle for 10 s, and close it if the ping gets no answer in 5 s.
		h2.ReadIdleTimeout = 10 * time.Second
		h2.PingTimeout = 5 * time.Second
	}
	return &http.Client{Transport: transport}
}

// clientOptions returns the azcore options every blob client is built with. insecureHTTP allows a
// shared key over plain HTTP, which only Azurite uses.
func clientOptions(insecureHTTP bool) azcore.ClientOptions {
	return azcore.ClientOptions{
		Transport:                       sharedHTTPClient,
		InsecureAllowCredentialWithHTTP: insecureHTTP,
	}
}
