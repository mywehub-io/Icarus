package storage

import (
	"net/http"
	"testing"
)

func TestBlobClientsShareOneTunedTransport(t *testing.T) {
	for _, insecure := range []bool{false, true} {
		opts := clientOptions(insecure)
		if opts.Transport != sharedHTTPClient {
			t.Fatalf("insecure=%v: transport is not the shared client", insecure)
		}
		if opts.InsecureAllowCredentialWithHTTP != insecure {
			t.Fatalf("insecure=%v: InsecureAllowCredentialWithHTTP = %v", insecure, opts.InsecureAllowCredentialWithHTTP)
		}
	}
	tr := sharedHTTPClient.Transport.(*http.Transport)
	if tr.MaxIdleConnsPerHost != MaxIdleConnsPerStorageHost {
		t.Fatalf("MaxIdleConnsPerHost = %d, want %d", tr.MaxIdleConnsPerHost, MaxIdleConnsPerStorageHost)
	}
}
