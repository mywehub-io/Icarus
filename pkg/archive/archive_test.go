package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// The load-bearing property of the whole format: the container changes, the data does not.
// If a document survives a build/read round trip with the same key set and the same value
// bytes, then nothing downstream of the resolver can tell the two representations apart,
// and any difference a differential harness reports is a container bug rather than a
// disagreement about how data should have been reshaped.
func TestRoundTripPreservesTheDocument(t *testing.T) {
	doc := []byte(`{
		"abc-/name": "david",
		"abc-/rows": [1,2,3],
		"abc-/meta/count": 3,
		"abc-/data//name": "nested",
		"abc-/auth[0]": {"id":1},
		"abc-/auth[10]/sub[2]": "deep",
		"abc-/empty": null,
		"abc-/flag": false,
		"abc-/unicode": "wehub éè 🚀"
	}`)

	raw, _, err := Build(doc)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	got, err := r.Document()
	if err != nil {
		t.Fatalf("Document: %v", err)
	}

	var want, have map[string]interface{}
	if err := json.Unmarshal(doc, &want); err != nil {
		t.Fatalf("unmarshal source: %v", err)
	}
	if err := json.Unmarshal(got, &have); err != nil {
		t.Fatalf("unmarshal round trip: %v", err)
	}
	if !reflect.DeepEqual(want, have) {
		t.Fatalf("round trip changed the document\nwant %v\n got %v", want, have)
	}
}

// Entry names are flat keys verbatim. Keys contain "/", "[" and "]", and the "//" form is
// the one that matters: some ZIP implementations normalise path separators, which would
// silently corrupt an array path. A reserved entry must not appear among the payload keys.
func TestEntryNamesAreFlatKeysVerbatim(t *testing.T) {
	keys := []string{
		"abc-/name",
		"abc-/data//name",
		"abc-/a//b//c",
		"abc-/auth[0]/sub[10]",
		"abc-/trailing/",
	}
	flat := map[string]string{}
	for i, k := range keys {
		flat[k] = fmt.Sprintf("%q", fmt.Sprintf("v%d", i))
	}
	var sb strings.Builder
	sb.WriteByte('{')
	first := true
	for k, v := range flat {
		if !first {
			sb.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&sb, "%q:%s", k, v)
	}
	sb.WriteByte('}')

	raw, _, err := Build([]byte(sb.String()))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	names := r.Names()
	if len(names) != len(keys) {
		t.Fatalf("got %d payload entries, want %d: %v", len(names), len(keys), names)
	}
	for _, k := range keys {
		if !r.Has(k) {
			t.Errorf("key %q did not survive the round trip byte-identically; got %v", k, names)
		}
	}
	for _, n := range names {
		if strings.HasPrefix(n, ReservedPrefix) {
			t.Errorf("reserved entry %q leaked into the payload key set", n)
		}
	}
	if r.Has(ManifestName) {
		t.Error("Has() reported the manifest as a payload key")
	}
}

// The manifest records which entries hold arrays, and that single fact decides whether a
// "//" mapping can be served from one key or degrades into a prefix scan over every key of
// the node. An entry that exists but holds an object must not be reported as an array.
func TestManifestRecordsArrayValuedEntriesOnly(t *testing.T) {
	doc := []byte(`{
		"n-/rows": [1,2,3],
		"n-/empty": [],
		"n-/spaced":   [  1 ],
		"n-/obj": {"a":1},
		"n-/str": "[not an array]",
		"n-/num": 7,
		"n-/null": null
	}`)

	raw, _, err := Build(doc)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	for _, k := range []string{"n-/rows", "n-/empty", "n-/spaced"} {
		if !r.IsArray(k) {
			t.Errorf("%q should be reported as an array", k)
		}
	}
	for _, k := range []string{"n-/obj", "n-/str", "n-/num", "n-/null"} {
		if r.IsArray(k) {
			t.Errorf("%q must not be reported as an array", k)
		}
	}
}

// Deterministic output is what lets a differential harness assert byte-identity at all, and
// what makes a diff between two runs mean something.
func TestBuildIsDeterministic(t *testing.T) {
	doc := []byte(`{"n-/a":1,"n-/b":[1],"n-/c":"x","n-/d":{"k":1},"n-/e":null}`)

	first, _, err := Build(doc)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for i := 0; i < 50; i++ {
		again, _, err := Build(doc)
		if err != nil {
			t.Fatalf("Build iteration %d: %v", i, err)
		}
		if !bytes.Equal(first, again) {
			t.Fatalf("iteration %d produced different bytes for identical input", i)
		}
	}
}

// Values are stored verbatim, so a point lookup returns exactly the bytes the document
// carried — no re-marshalling, no number reformatting, no key reordering inside a value.
func TestGetReturnsStoredBytesVerbatim(t *testing.T) {
	doc := []byte(`{"n-/v":{"b":2,"a":1.50,"z":[1,2]}}`)

	raw, _, err := Build(doc)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	got, err := r.Get("n-/v")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if want := `{"b":2,"a":1.50,"z":[1,2]}`; string(got) != want {
		t.Fatalf("Get returned %s, want %s — values must be stored byte for byte", got, want)
	}
}

func TestReaderRejectsAFutureFormatVersion(t *testing.T) {
	doc := []byte(`{"n-/v":1}`)
	raw, _, err := Build(doc)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Rebuild with a bumped manifest version, which is what a newer writer would produce.
	bumped := bytes.Replace(raw,
		[]byte(fmt.Sprintf(`"version":%d`, FormatVersion)),
		[]byte(fmt.Sprintf(`"version":%d`, FormatVersion+1)), 1)
	if bytes.Equal(bumped, raw) {
		t.Skip("manifest version string not found in the archive bytes; adjust this test")
	}

	if _, err := NewReader(bytes.NewReader(bumped), int64(len(bumped))); err == nil {
		t.Fatal("expected a reader to refuse a manifest newer than it understands")
	}
}

func TestBuildRejectsAPayloadUsingTheReservedPrefix(t *testing.T) {
	doc := []byte(`{"_wehub/manifest.json":"spoofed"}`)
	if _, _, err := Build(doc); err == nil {
		t.Fatal("expected Build to refuse a payload key under the reserved prefix")
	}
}

func TestIsArchiveDetectsFormat(t *testing.T) {
	raw, _, err := Build([]byte(`{"n-/v":1}`))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if !IsArchive(raw) {
		t.Error("an archive was not detected as one")
	}
	for _, notArchive := range [][]byte{
		[]byte(`{"n-/v":1}`), []byte(""), []byte("PK"), []byte("PKxx"), nil,
	} {
		if IsArchive(notArchive) {
			t.Errorf("%q was wrongly detected as an archive", notArchive)
		}
	}
}

func TestBuildHandlesAnEmptyDocument(t *testing.T) {
	for _, doc := range []string{"", "  ", "{}"} {
		raw, _, err := Build([]byte(doc))
		if err != nil {
			t.Fatalf("Build(%q): %v", doc, err)
		}
		r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			t.Fatalf("NewReader for %q: %v", doc, err)
		}
		if n := len(r.Names()); n != 0 {
			t.Fatalf("empty document produced %d payload entries", n)
		}
	}
}

// Flat is what assembles the map the extraction code consumes. A name that is absent is
// skipped rather than erroring, because a mapping naming a key the producer never wrote is
// routine and failing it would break runs that succeed today.
func TestFlatSkipsAbsentKeysRatherThanFailing(t *testing.T) {
	raw, _, err := Build([]byte(`{"n-/a":1,"n-/b":"two"}`))
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	flat, err := r.Flat([]string{"n-/a", "n-/missing", ManifestName})
	if err != nil {
		t.Fatalf("Flat: %v", err)
	}
	if len(flat) != 1 || flat["n-/a"] != float64(1) {
		t.Fatalf("Flat = %v, want just n-/a", flat)
	}
}
