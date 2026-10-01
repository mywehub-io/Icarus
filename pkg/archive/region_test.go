package archive

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// EntryRegionEnd must land exactly on the first central-directory header, for every
// shape the writers produce, or a preloaded span would miss the tail of the last entry.
func TestEntryRegionEndIsTheStartOfTheDirectory(t *testing.T) {
	docs := map[string][]byte{
		"flat keys": []byte(`{"n-/a":1,"n-/b":"two","n-/rows":[{"x":1},{"x":2}]}`),
		"one key":   []byte(`{"n-/v":"x"}`),
	}
	for name, doc := range docs {
		raw, _, err := Build(doc)
		if err != nil {
			t.Fatalf("%s: Build: %v", name, err)
		}
		checkRegionEnd(t, name, raw)
	}

	opaque, _, err := BuildOpaque([]byte("MSH|^~\\&|not json"))
	if err != nil {
		t.Fatalf("BuildOpaque: %v", err)
	}
	checkRegionEnd(t, "opaque", opaque)
}

func checkRegionEnd(t *testing.T, name string, raw []byte) {
	t.Helper()
	end, ok := EntryRegionEnd(bytes.NewReader(raw), int64(len(raw)))
	if !ok {
		t.Fatalf("%s: EntryRegionEnd reported unknown", name)
	}
	if end <= 0 || end+4 > int64(len(raw)) {
		t.Fatalf("%s: end %d out of range for %d bytes", name, end, len(raw))
	}
	if sig := binary.LittleEndian.Uint32(raw[end:]); sig != 0x02014b50 {
		t.Fatalf("%s: bytes at %d are %#x, not a central-directory header", name, end, sig)
	}
}

func TestEntryRegionEndRejectsWhatItCannotRead(t *testing.T) {
	raw, _, err := Build([]byte(`{"n-/v":"x"}`))
	if err != nil {
		t.Fatal(err)
	}

	withComment := append(append([]byte(nil), raw...), 'c')
	binary.LittleEndian.PutUint16(withComment[len(raw)-2:], 1)

	cases := map[string][]byte{
		"not a zip":    []byte("plain bytes, nothing like an archive at all"),
		"too short":    []byte("PK"),
		"with comment": withComment,
	}
	for name, data := range cases {
		if _, ok := EntryRegionEnd(bytes.NewReader(data), int64(len(data))); ok {
			t.Fatalf("%s: EntryRegionEnd reported a region it cannot know", name)
		}
	}
}
