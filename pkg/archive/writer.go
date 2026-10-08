package archive

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// Build converts a marshalled StandardUnitOutput document into an archive.
//
// Every top-level key becomes one STORED entry named after that key, holding the key's
// raw JSON value. The document's own bytes for each value are reused rather than
// re-marshalled, so no number reformatting, key reordering or escaping change can creep
// in between the two representations.
//
// Entries are written in sorted key order. Nothing in the format requires an order, but a
// deterministic one means the same payload produces the same bytes, which is what lets a
// differential harness assert byte-identity and makes a diff between two runs meaningful.
// Stats describes what a build produced. EntryCount is the measurement that decides
// whether the high-key-count payload shape is real in practice: that shape costs heap on
// open and makes the archive larger than the document it replaced, and the plan accepts
// both rather than designing around them, on the basis that this number will say whether
// a remedy is needed.
type Stats struct {
	EntryCount      int
	ArrayEntryCount int
}

// Build converts a marshalled flat JSON document into an addressable archive, failing if
// the payload is not a JSON object.
func Build(doc []byte) ([]byte, Stats, error) {
	flat, err := decodeFlat(doc)
	if err != nil {
		return nil, Stats{}, err
	}
	return buildDocument(flat)
}

func buildDocument(flat map[string]json.RawMessage) ([]byte, Stats, error) {
	var buf bytes.Buffer
	stats, err := WriteDocument(&buf, flat)
	if err != nil {
		return nil, stats, err
	}
	return buf.Bytes(), stats, nil
}

// WriteDocument writes a document archive to w, one STORED entry per flat key. flat holds
// values already in JSON form.
func WriteDocument(w io.Writer, flat map[string]json.RawMessage) (Stats, error) {
	var stats Stats

	names := make([]string, 0, len(flat))
	for name := range flat {
		names = append(names, name)
	}
	for _, name := range names {
		if strings.HasPrefix(name, ReservedPrefix) {
			// A payload key can never legitimately collide with the reserved prefix, so
			// this means the input was not a StandardUnitOutput document.
			return stats, fmt.Errorf("archive: payload key %q uses the reserved %q prefix", name, ReservedPrefix)
		}
	}
	sort.Strings(names)

	// Resolve stored names before writing anything, because the manifest records the
	// renames and is written first.
	stored := make([]string, len(names))
	manifest := Manifest{Version: FormatVersion}
	for i, name := range names {
		entry, renamed := storableName(name, i)
		stored[i] = entry
		if renamed {
			if manifest.Renames == nil {
				manifest.Renames = map[string]string{}
			}
			manifest.Renames[entry] = name
		}
		if raw, ok := flat[name]; ok && isJSONArray(raw) {
			// Array entries are listed by flat key, which is what the fetch planner asks
			// about; it never sees a stored name.
			manifest.ArrayEntries = append(manifest.ArrayEntries, name)
		}
	}

	zw := zip.NewWriter(w)

	// The manifest is written first so a reader that wants it does not have to reach past
	// a large payload entry to get there.
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return stats, fmt.Errorf("archive: marshal manifest: %w", err)
	}
	if err := writeEntry(zw, ManifestName, manifestBytes); err != nil {
		return stats, err
	}

	for i, name := range names {
		if err := writeEntry(zw, stored[i], flat[name]); err != nil {
			return stats, err
		}
	}

	if err := zw.Close(); err != nil {
		return stats, fmt.Errorf("archive: finalise: %w", err)
	}
	stats.EntryCount = len(names)
	stats.ArrayEntryCount = len(manifest.ArrayEntries)
	return stats, nil
}

// writeEntry appends one uncompressed entry. Method Store is not optional: it is what
// makes an entry's bytes a contiguous, seekable range of the blob.
func writeEntry(zw *zip.Writer, name string, body []byte) error {
	w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
	if err != nil {
		return fmt.Errorf("archive: create entry %q: %w", name, err)
	}
	if _, err := w.Write(body); err != nil {
		return fmt.Errorf("archive: write entry %q: %w", name, err)
	}
	return nil
}
