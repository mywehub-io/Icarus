package runtime

import (
	"context"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/filestore"
	"github.com/wehubfusion/Icarus/pkg/filestore/memfs"
)

func TestProcessInputTrustsOnlyByteMappedFiles(t *testing.T) {
	const wf, run = "w", "r"
	b := memfs.New()
	ref := fileref.FileRef{Path: fileref.RunPrefix(wf, run) + "p/payload.json", Size: 7}
	b.Put(ref.Path, []byte(`{"a":1}`))
	store, _ := filestore.New(b, wf, run)
	mappings := []FieldMapping{
		{SourceEndpoint: "/payload", DestinationEndpoints: []string{"/data"}, ValueType: "BYTE"},
		{SourceEndpoint: "/other", DestinationEndpoints: []string{"/note"}},
	}
	in := ProcessInput{Ctx: context.Background(), Files: store, ByteFields: byteFields(mappings),
		Data: map[string]interface{}{"data": fileref.Value(ref), "note": fileref.Value(ref)}}

	data, isFile, err := in.ReadTrustedFile("data", 0)
	if err != nil || !isFile || string(data) != `{"a":1}` {
		t.Fatalf("byte-mapped file not read: %q %v %v", data, isFile, err)
	}
	if _, ok := in.TrustedFile("note"); ok {
		t.Fatal("a reference under a data mapping must not be trusted")
	}
	if _, _, err := in.ReadTrustedFile("data", 3); err == nil {
		t.Fatal("the size cap was not applied")
	}
	if got := byteFields([]FieldMapping{{SourceEndpoint: "/out/encoded", ValueType: "BYTE"}}); !got["encoded"] {
		t.Fatalf("root destination must take the source's last segment: %v", got)
	}
}
