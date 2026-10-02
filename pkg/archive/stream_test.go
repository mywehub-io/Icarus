package archive

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"
)

// buildDocument now delegates to WriteDocument. These hashes were taken from the writer
// before that refactor, so a match proves the archive bytes did not move at all — not
// merely that the two current paths agree with each other.
func TestBuildBytesUnchangedByStreamingRefactor(t *testing.T) {
	cases := []struct{ doc, sha string }{
		{`{}`, "ad218ea4897e3d21eace7b41215c3260d420da2a75500c429397f3a1811178c7"},
		{`{"abc-/name":"david","abc-/rows":[1,2,3],"abc-/meta/count":3,"abc-/data//name":"nested","abc-/auth[0]":{"id":1},"abc-/empty":null,"abc-/unicode":"wehub éè 🚀"}`,
			"aaccef6d25cc92146f0a1e2ab315b8321016698ab9b53a71c0ea615e1a0f5414"},
		{`{"esr-/output":"U0VSUyBkYXRhYmFzZQ==","esr-/trailing/":"x","esr-/error":null}`,
			"f184259eadd5691a97199d86503ac20cf9ff2d5cfc89483c3e00f2b384a195d2"},
		{`{"sftp-/payload/files":[{"file_data":"SERSfg==","file_name":"a.dat"}],"sftp-/action":"Download File"}`,
			"252bb6ea4167a97dfc1ce0d3c49b6f8ccb34d4e21a23b7350939a6ad2bb1f94b"},
	}
	for _, c := range cases {
		raw, _, err := Build([]byte(c.doc))
		if err != nil {
			t.Fatalf("Build(%s): %v", c.doc, err)
		}
		sum := sha256.Sum256(raw)
		if got := hex.EncodeToString(sum[:]); got != c.sha {
			t.Errorf("archive bytes changed for %s\nwant %s\n got %s", c.doc, c.sha, got)
		}
	}
}

func streamedBytes(b []byte) StreamedValue {
	return StreamedValue{
		Size: int64(len(b)),
		Open: func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil },
	}
}

// A streamed value must be indistinguishable from the same bytes base64-encoded and handed
// over in memory: same archive bytes, so no reader can tell which path wrote it. Lengths
// cover every base64 padding case.
func TestStreamedEntryIsByteIdenticalToInlineEntry(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 5, 1 << 16, 1<<16 + 1} {
		raw := bytes.Repeat([]byte{0, 0xff, 'D', 'U', 'C', 'K'}, n/6+1)[:n]
		encoded, _ := json.Marshal(base64.StdEncoding.EncodeToString(raw))

		small := map[string]json.RawMessage{"esr-/error": json.RawMessage(`null`)}

		inline := map[string]json.RawMessage{"esr-/output": encoded, "esr-/error": json.RawMessage(`null`)}
		want, _, err := buildDocument(inline)
		if err != nil {
			t.Fatalf("n=%d: buildDocument: %v", n, err)
		}

		var got bytes.Buffer
		if _, err := WriteDocument(&got, small, map[string]StreamedValue{"esr-/output": streamedBytes(raw)}); err != nil {
			t.Fatalf("n=%d: WriteDocument: %v", n, err)
		}
		if !bytes.Equal(want, got.Bytes()) {
			t.Fatalf("n=%d: streamed archive differs from the inline one", n)
		}
		if int64(len(encoded)) != streamedBytes(raw).EncodedLen() {
			t.Fatalf("n=%d: EncodedLen %d, marshalled length %d", n, streamedBytes(raw).EncodedLen(), len(encoded))
		}
	}
}

// Size decided the inline-versus-blob branch before writing began, so a source that
// yields a different count means the decision was made on stale facts.
func TestStreamedEntryRefusesASizeMismatch(t *testing.T) {
	v := streamedBytes([]byte("abcdef"))
	v.Size = 5
	var buf bytes.Buffer
	_, err := WriteDocument(&buf, nil, map[string]StreamedValue{"esr-/output": v})
	if err == nil || !strings.Contains(err.Error(), "declared 5") {
		t.Fatalf("expected a size mismatch error, got %v", err)
	}
}

func TestWriteDocumentRefusesAKeyThatIsBothStreamedAndInline(t *testing.T) {
	var buf bytes.Buffer
	_, err := WriteDocument(&buf,
		map[string]json.RawMessage{"esr-/output": json.RawMessage(`"x"`)},
		map[string]StreamedValue{"esr-/output": streamedBytes([]byte("x"))})
	if err == nil {
		t.Fatal("expected an error for a key written twice")
	}
}

// The range EntryRange reports must be the value itself, byte for byte, read straight out
// of the blob — which is the whole basis for streaming an entry with ranged GETs.
func TestEntryRangeAddressesTheValueBytes(t *testing.T) {
	doc := []byte(`{"n-/a":"short","n-/rows":[1,2,3],"n-/trailing/":{"x":1},"n-/big":"` +
		strings.Repeat("Q", 70_000) + `"}`)
	raw, _, err := Build(doc)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	r, err := NewReader(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	for _, key := range r.Names() {
		off, n, err := r.EntryRange(key)
		if err != nil {
			t.Fatalf("EntryRange(%q): %v", key, err)
		}
		want, err := r.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if !bytes.Equal(raw[off:off+n], want) {
			t.Fatalf("EntryRange(%q) does not address the value", key)
		}
	}
	if _, _, err := r.EntryRange("n-/absent"); err == nil {
		t.Fatal("expected an error for an absent key")
	}
}
