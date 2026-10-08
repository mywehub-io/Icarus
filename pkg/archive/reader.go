package archive

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Reader gives addressed access to an archive over anything implementing io.ReaderAt.
//
// Backed by storage.BlobReaderAt it reads through ranged GETs, so opening a Reader moves
// only the end-of-central-directory record, the central directory and the manifest —
// kilobytes at the tail of the blob, whatever the payload's size — and each value fetched
// afterwards moves only that value.
//
// Nothing here parses a value. The archive addresses whole keys and serves bytes; it does
// not know whether a value is a CSV, an HL7 message or an array of a hundred thousand
// rows, and it deliberately records no row or segment boundaries.
type Reader struct {
	zr       *zip.Reader
	manifest Manifest
	byName   map[string]*zip.File
	arrays   map[string]bool
	names    []string

	// entryOf maps a flat key to the entry name holding it. Identity for every ordinary
	// key; it differs only for the keys a ZIP name cannot express verbatim.
	entryOf map[string]string
}

// NewReader opens an archive. size must be the blob's exact length, which
// BlobReference.SizeBytes already carries, so no HEAD request is needed on this path.
func NewReader(r io.ReaderAt, size int64) (*Reader, error) {
	zr, err := zip.NewReader(r, size)
	if err != nil {
		return nil, fmt.Errorf("archive: open: %w", err)
	}

	a := &Reader{
		zr:      zr,
		byName:  make(map[string]*zip.File, len(zr.File)),
		arrays:  map[string]bool{},
		names:   make([]string, 0, len(zr.File)),
		entryOf: make(map[string]string, len(zr.File)),
	}
	for _, f := range zr.File {
		a.byName[f.Name] = f
	}

	// The manifest has to be read before the key set is built, because it is what says
	// which reserved entries are really payload keys under a generated name.
	if err := a.loadManifest(); err != nil {
		return nil, err
	}

	for _, f := range zr.File {
		key, ok := a.keyForEntry(f.Name)
		if !ok {
			continue // format-internal, not payload
		}
		a.entryOf[key] = f.Name
		a.names = append(a.names, key)
	}
	sort.Strings(a.names)

	return a, nil
}

// keyForEntry maps a stored entry name back to the flat key it holds, reporting false for
// entries that belong to the format rather than the payload.
func (a *Reader) keyForEntry(entry string) (string, bool) {
	if !strings.HasPrefix(entry, ReservedPrefix) {
		return entry, true
	}
	if key, ok := a.manifest.Renames[entry]; ok {
		return key, true
	}
	return "", false
}

func (a *Reader) loadManifest() error {
	f, ok := a.byName[ManifestName]
	if !ok {
		// Tolerated rather than fatal. An archive with no manifest is readable — every
		// entry is still addressable by name — it just cannot answer "is this value an
		// array", so the fetch planner has to treat those mappings conservatively. That
		// degrades efficiency, never correctness.
		a.manifest = Manifest{Version: FormatVersion}
		return nil
	}

	body, err := readFile(f)
	if err != nil {
		return fmt.Errorf("archive: read manifest: %w", err)
	}
	if err := json.Unmarshal(body, &a.manifest); err != nil {
		return fmt.Errorf("archive: parse manifest: %w", err)
	}
	if a.manifest.Version > FormatVersion {
		return fmt.Errorf("archive: manifest version %d is newer than supported version %d",
			a.manifest.Version, FormatVersion)
	}
	for _, name := range a.manifest.ArrayEntries {
		a.arrays[name] = true
	}
	return nil
}

// Names returns every payload entry name — that is, every flat key in the file — in
// sorted order, with format-internal entries excluded. Answered from the central
// directory already in memory, so it costs no fetch.
func (a *Reader) Names() []string {
	out := make([]string, len(a.names))
	copy(out, a.names)
	return out
}

// Has reports whether a key is present, without fetching its value.
func (a *Reader) Has(key string) bool {
	_, ok := a.entryOf[key]
	return ok
}

// Size returns a key's uncompressed length from the directory, without fetching it.
func (a *Reader) Size(key string) (int64, bool) {
	entry, ok := a.entryOf[key]
	if !ok {
		return 0, false
	}
	f, ok := a.byName[entry]
	if !ok {
		return 0, false
	}
	return int64(f.UncompressedSize64), true
}

// EntryRange returns where a key's value sits within the blob: its first byte's offset
// and its length. Answered from the central directory plus one local-header read.
//
// This is what lets a consumer stream one large value with ranged GETs of its own choosing
// rather than through Get, which holds the whole value in memory. It relies on the entry
// being STORED, so the range within the blob is the value byte for byte; any other method
// is refused rather than handed back as a range of compressed bytes.
func (a *Reader) EntryRange(key string) (offset, length int64, err error) {
	entry, ok := a.entryOf[key]
	if !ok {
		return 0, 0, fmt.Errorf("archive: entry %q not found", key)
	}
	f := a.byName[entry]
	if f.Method != zip.Store {
		return 0, 0, fmt.Errorf("archive: entry %q uses method %d, not STORED", key, f.Method)
	}
	offset, err = f.DataOffset()
	if err != nil {
		return 0, 0, fmt.Errorf("archive: locate entry %q: %w", key, err)
	}
	return offset, int64(f.UncompressedSize64), nil
}

// IsArray reports whether a key's value is a JSON array, from the manifest.
//
// This is the fact the "//" classification turns on and that an entry name cannot carry:
// such a mapping resolves from one key only when that key exists *and* holds an array,
// and otherwise degrades into a prefix scan needing every key of the node. A planner that
// cannot establish it must fetch conservatively.
func (a *Reader) IsArray(key string) bool { return a.arrays[key] }

// TotalPayloadBytes is the summed uncompressed size of every payload entry. The fetch
// planner compares it against a node's share to decide between many ranged reads and one
// whole-blob download.
func (a *Reader) TotalPayloadBytes() int64 {
	var total int64
	for _, key := range a.names {
		if n, ok := a.Size(key); ok {
			total += n
		}
	}
	return total
}

// Get returns one key's value bytes, verbatim as the writer stored them.
func (a *Reader) Get(key string) ([]byte, error) {
	entry, ok := a.entryOf[key]
	if !ok {
		return nil, fmt.Errorf("archive: entry %q not found", key)
	}
	return readFile(a.byName[entry])
}

// Flat materialises the named keys into the flat map the resolver's extraction code
// consumes. Names that are absent are skipped rather than erroring: a mapping naming a
// key the producer never wrote is routine, and turning it into a failure would break runs
// that succeed today.
func (a *Reader) Flat(keys []string) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(keys))
	for _, key := range keys {
		entry, ok := a.entryOf[key]
		if !ok {
			continue
		}
		body, err := readFile(a.byName[entry])
		if err != nil {
			return nil, fmt.Errorf("archive: read entry %q: %w", key, err)
		}
		var v interface{}
		if err := json.Unmarshal(body, &v); err != nil {
			return nil, fmt.Errorf("archive: entry %q is not valid JSON: %w", key, err)
		}
		out[key] = v
	}
	return out, nil
}

// FlatAll materialises every payload entry, which is what a whole-node or whole-file read
// resolves to.
func (a *Reader) FlatAll() (map[string]interface{}, error) { return a.Flat(a.names) }

// Document re-materialises the archive as the flat JSON document it was built from. Used
// by the differential harness to assert the container swap is lossless.
func (a *Reader) Document() ([]byte, error) {
	flat := make(map[string]json.RawMessage, len(a.names))
	for _, key := range a.names {
		body, err := readFile(a.byName[a.entryOf[key]])
		if err != nil {
			return nil, fmt.Errorf("archive: read entry %q: %w", key, err)
		}
		flat[key] = json.RawMessage(body)
	}
	return json.Marshal(flat)
}

func readFile(f *zip.File) ([]byte, error) {
	rc, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}
