package archive

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
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

	// Opaque records that the payload was carried whole rather than addressed by key.
	// Worth logging: an unexpected run of opaque writes on the node-output path would
	// mean documents are failing the shape test and silently losing selective fetch.
	Opaque bool
}

// BuildPayload converts anything bound for blob storage into an archive.
//
// This is the entry point CreateResult uses, and it exists so that call site has no format
// branch left: every blob written through the resolver is an archive, whatever was handed
// to it. A StandardUnitOutput document becomes an addressable one entry per flat key; any
// other payload — an HL7 message from MLLP ingest, an arbitrary HTTP trigger body — becomes
// an opaque one carrying those bytes whole.
//
// It fails only on a genuine encoding fault, never on the payload's shape. Artemis writes
// the trigger blob *before* acknowledging the message, precisely so no MSA|AA is sent for a
// body that is not yet durable, so a shape this refused would cost a message rather than a
// field.
func BuildPayload(data []byte) ([]byte, Stats, error) {
	flat, err := decodeFlat(data)
	if err != nil || !isStandardUnitOutput(flat) {
		return BuildOpaque(data)
	}
	return buildDocument(flat)
}

// BuildOpaque wraps payload bytes in an archive that carries them whole.
//
// There is one entry and it is reserved, so the archive has an empty key space: Names
// returns nothing and no fetch planner will ever select from it. Reading it back is
// Reader.Raw, and the bytes returned are the bytes given, which is the property the trigger
// path depends on.
func BuildOpaque(payload []byte) ([]byte, Stats, error) {
	stats := Stats{Opaque: true}

	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)

	manifestBytes, err := json.Marshal(Manifest{Version: FormatVersion, Raw: true})
	if err != nil {
		return nil, stats, fmt.Errorf("archive: marshal manifest: %w", err)
	}
	if err := writeEntry(zw, ManifestName, manifestBytes); err != nil {
		return nil, stats, err
	}
	if err := writeEntry(zw, RawEntryName, payload); err != nil {
		return nil, stats, err
	}
	if err := zw.Close(); err != nil {
		return nil, stats, fmt.Errorf("archive: finalise: %w", err)
	}
	return buf.Bytes(), stats, nil
}

// Build converts a marshalled flat JSON document into an addressable archive, failing if
// the payload is not a JSON object.
//
// Callers that cannot guarantee the shape want BuildPayload instead.
func Build(doc []byte) ([]byte, Stats, error) {
	flat, err := decodeFlat(doc)
	if err != nil {
		return nil, Stats{}, err
	}
	return buildDocument(flat)
}

func buildDocument(flat map[string]json.RawMessage) ([]byte, Stats, error) {
	var buf bytes.Buffer
	stats, err := WriteDocument(&buf, flat, nil)
	if err != nil {
		return nil, stats, err
	}
	return buf.Bytes(), stats, nil
}

// StreamedValue is a byte value written into a document archive without ever being held
// in memory whole: a database file, a decoded upload, anything a node produced on disk.
//
// The entry it becomes is exactly what the in-memory path writes for the same bytes — a
// JSON string holding their standard base64 encoding — so a reader cannot tell which path
// wrote it. Base64's alphabet needs no JSON escaping, which is what makes encoding on the
// fly byte-identical to marshalling the encoded string.
type StreamedValue struct {
	// Size is the raw (unencoded) length. Open must yield exactly this many bytes; a
	// mismatch fails the write, because it means the source changed under the writer.
	Size int64
	Open func() (io.ReadCloser, error)
}

// EncodedLen is the length of the value's JSON form: base64 plus the two quotes.
func (v StreamedValue) EncodedLen() int64 {
	return int64(base64.StdEncoding.EncodedLen(int(v.Size))) + 2
}

// WriteDocument writes a document archive to w, one STORED entry per flat key.
//
// flat holds values already in JSON form; streamed holds byte values encoded as they are
// copied. The two key sets must not overlap. buildDocument is this function with nothing
// streamed, so there is one writer and no second implementation to keep in step with it.
func WriteDocument(w io.Writer, flat map[string]json.RawMessage, streamed map[string]StreamedValue) (Stats, error) {
	var stats Stats

	names := make([]string, 0, len(flat)+len(streamed))
	for name := range flat {
		names = append(names, name)
	}
	for name := range streamed {
		if _, dup := flat[name]; dup {
			return stats, fmt.Errorf("archive: key %q is both streamed and inline", name)
		}
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
		// A streamed value is always a string, never an array.
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
		if v, ok := streamed[name]; ok {
			if err := writeStreamedEntry(zw, stored[i], v); err != nil {
				return stats, err
			}
			continue
		}
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

// writeStreamedEntry appends one STORED entry holding v as a JSON base64 string, encoding
// while copying so the raw bytes are never held whole.
func writeStreamedEntry(zw *zip.Writer, name string, v StreamedValue) error {
	if v.Open == nil {
		return fmt.Errorf("archive: streamed entry %q has no source", name)
	}
	src, err := v.Open()
	if err != nil {
		return fmt.Errorf("archive: open streamed entry %q: %w", name, err)
	}
	defer src.Close()

	w, err := zw.CreateHeader(&zip.FileHeader{Name: name, Method: zip.Store})
	if err != nil {
		return fmt.Errorf("archive: create entry %q: %w", name, err)
	}
	if _, err := w.Write([]byte{'"'}); err != nil {
		return fmt.Errorf("archive: write entry %q: %w", name, err)
	}
	enc := base64.NewEncoder(base64.StdEncoding, w)
	n, err := io.Copy(enc, src)
	if err != nil {
		return fmt.Errorf("archive: write entry %q: %w", name, err)
	}
	if n != v.Size {
		return fmt.Errorf("archive: streamed entry %q yielded %d bytes, declared %d", name, n, v.Size)
	}
	// Close flushes the final partial quantum and its padding.
	if err := enc.Close(); err != nil {
		return fmt.Errorf("archive: write entry %q: %w", name, err)
	}
	if _, err := w.Write([]byte{'"'}); err != nil {
		return fmt.Errorf("archive: write entry %q: %w", name, err)
	}
	return nil
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
