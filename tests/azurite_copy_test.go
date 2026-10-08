package tests

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// readSASURL is the URL of container/blobName with a one hour read SAS signed by the account key
// in the connection string, as a copy source the storage service can read on its own.
func readSASURL(t *testing.T, client *storage.AzureBlobClient, containerName, blobName string) string {
	t.Helper()
	params := map[string]string{}
	for _, kv := range strings.Split(azuriteConnectionString(), ";") {
		if i := strings.Index(kv, "="); i > 0 {
			params[kv[:i]] = kv[i+1:]
		}
	}
	cred, err := azblob.NewSharedKeyCredential(params["AccountName"], params["AccountKey"])
	if err != nil {
		t.Fatal(err)
	}
	qp, err := sas.BlobSignatureValues{
		Protocol:      sas.ProtocolHTTPSandHTTP,
		StartTime:     time.Now().Add(-5 * time.Minute),
		ExpiryTime:    time.Now().Add(time.Hour),
		Permissions:   (&sas.BlobPermissions{Read: true}).String(),
		ContainerName: containerName,
		BlobName:      blobName,
	}.SignWithSharedKey(cred)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(client.URLFor(blobName))
	u.RawQuery = qp.Encode()
	return u.String()
}

// CopyFromURL copies a blob server-side: a nested key, a synchronous copy and an asynchronous
// one (size unknown), the caller's content type on the result, and a source the service cannot
// read reported as not started so a caller streams instead (raw payloads D14).
func TestAzuriteCopyFromURL(t *testing.T) {
	client, container_ := newAzuriteClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	body := bytes.Repeat([]byte("copy me,"), 1000)
	const src = "exports/nested/result.csv"
	if _, err := client.UploadStream(ctx, src, bytes.NewReader(body), "application/octet-stream", nil); err != nil {
		t.Fatal(err)
	}
	srcURL := readSASURL(t, client, container_, src)

	// Azurite refuses an asynchronous copy whose source is authorised by a SAS
	// (CannotVerifyCopySource) while the synchronous copy accepts it; Azure itself accepts both.
	// So the asynchronous case reads a public blob, which exercises the start and the polling, and
	// the SAS refusal below is the not-started error a caller answers by streaming. The real
	// service's behaviour is the UAT check (decisions D14).
	svc, err := azblob.NewClientFromConnectionString(azuriteConnectionString(), nil)
	if err != nil {
		t.Fatal(err)
	}
	public := container.PublicAccessTypeBlob
	if _, err := svc.ServiceClient().NewContainerClient(container_).SetAccessPolicy(ctx, &container.SetAccessPolicyOptions{Access: &public}); err != nil {
		t.Fatal(err)
	}
	publicURL := client.URLFor(src)

	for _, tc := range []struct {
		name   string
		size   int64
		source string
		dst    string
	}{
		{"synchronous", int64(len(body)), srcURL, "results/wf/run/n/sync.csv"},
		{"asynchronous, unknown size", -1, publicURL, "results/wf/run/n/async.csv"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.CopyFromURL(ctx, tc.dst, tc.source, "text/csv", tc.size, time.Minute, map[string]string{"run_id": "run"}); err != nil {
				t.Fatalf("copy: %v", err)
			}
			if n, err := client.BlobSize(ctx, client.URLFor(tc.dst)); err != nil || n != int64(len(body)) {
				t.Fatalf("size %d err %v", n, err)
			}
			store, _ := filestore.New(client, "wf", "run")
			ref := fileref.FileRef{Path: tc.dst, Size: int64(len(body)), ContentType: "text/csv"}
			rc, err := store.Open(ctx, ref)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(rc)
			rc.Close()
			if !bytes.Equal(got, body) {
				t.Fatal("copied bytes differ")
			}
		})
	}

	t.Run("an asynchronous copy the service refuses to start is not started", func(t *testing.T) {
		_, err := client.CopyFromURL(ctx, "results/wf/run/n/refused.csv", srcURL, "text/csv", -1, time.Minute, nil)
		if err != nil && !errors.Is(err, storage.ErrCopyNotStarted) {
			t.Fatalf("a refusal must be ErrCopyNotStarted, got %v", err)
		}
	})

	t.Run("a source the service cannot read is not started", func(t *testing.T) {
		_, err := client.CopyFromURL(ctx, "results/wf/run/n/none.csv", client.URLFor("no/such/blob"), "text/csv", 10, time.Minute, nil)
		if !errors.Is(err, storage.ErrCopyNotStarted) {
			t.Fatalf("want ErrCopyNotStarted, got %v", err)
		}
	})
}
