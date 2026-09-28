// Package archive implements the directly addressable container that replaces the
// single JSON document in the Zeus to Elysium node-output handoff.
//
// The format is an ordinary ZIP with one STORED entry per StandardUnitOutput flat key.
// The entry name is the key verbatim — brackets, slashes and all — and the entry body is
// that key's JSON value byte for byte as it would have appeared in the document. Nothing
// is re-grouped, no index is stripped, no value is reinterpreted, which is what makes the
// container swap incapable of changing what the pipeline means: the key set and every
// value are identical, so any observed difference is a container bug rather than a
// disagreement about how data should have been reshaped.
//
// Entries are never compressed. An uncompressed entry occupies a contiguous byte range of
// the blob, so a range within the entry is a range within the blob and a consumer can
// fetch one field without moving the rest. Deflate would forfeit that, since a deflate
// stream cannot be seeked into.
package archive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

const (
	// FormatVersion is the manifest's schema version. Readers reject anything higher
	// rather than guessing at a layout they were not written for.
	FormatVersion = 1

	// ManifestName is the reserved entry holding what the ZIP central directory cannot
	// express. The "_wehub/" prefix cannot collide with a real flat key because every
	// key contains "-/" after its node id.
	ManifestName = "_wehub/manifest.json"

	// RawEntryName holds an opaque payload's bytes in an archive built by BuildOpaque.
	// Reserved like the manifest, so it is never mistaken for a payload key.
	RawEntryName = "_wehub/raw"

	// ReservedPrefix marks entries that belong to the format rather than the payload.
	ReservedPrefix = "_wehub/"

	// Extension and ContentType are what a result blob written in this format uses.
	Extension   = ".zip"
	ContentType = "application/zip"

	// MaxReferenceHops caps reference chains at read time. The writer flattens chains to
	// depth 1, so this exists only as defence against a writer bug and should never be
	// reached in practice.
	MaxReferenceHops = 4
)

// Manifest carries the small amount the central directory cannot express.
//
// It deliberately does not describe shape, grouping or record boundaries. A format that
// does not claim to understand a value's interior does not need them, and those belong to
// the deferred iteration work.
type Manifest struct {
	Version int `json:"version"`

	// Raw marks an archive whose payload is one opaque byte string under RawEntryName
	// rather than a set of addressable flat keys.
	//
	// It exists so that everything written to blob storage is an archive, including the
	// payloads that are not StandardUnitOutput documents and never could be: Artemis's
	// MLLP ingest offloads a raw HL7 message, and an HTTP trigger offloads whatever body
	// arrived. Those have no key space to address, so the archive carries them whole and
	// hands them back byte for byte.
	//
	// The flag lives in the manifest rather than being inferred from the entry set
	// because inference would be a guess: an empty document and an opaque payload of
	// zero bytes are indistinguishable by entries alone, and a reader that guesses wrong
	// returns a plausible, well-formed, wrong answer.
	Raw bool `json:"raw,omitempty"`

	// ArrayEntries names the entries whose value is a JSON array.
	//
	// It is a sparse list rather than a flag per entry on purpose: a per-entry flag would
	// reintroduce inside the archive exactly the per-key scaling problem the format is
	// meant to avoid. It stays small in both payload shapes for opposite reasons — a
	// few-key payload has few entries at all, and a high-key-count payload's indexed keys
	// hold scalars and objects, because the arrays were exploded into field[i] before
	// persistence.
	//
	// The read path needs this because a "//" mapping resolves from a single key only
	// when that key exists *and* holds an array; otherwise the very same mapping degrades
	// to a prefix scan over every key of the node. A name cannot carry that fact.
	ArrayEntries []string `json:"array_entries,omitempty"`

	// References maps an entry name to the value it points at, for entries that are a
	// pointer rather than a copy. Empty in this release: nothing emits references yet.
	References map[string]Reference `json:"references,omitempty"`

	// Renames maps a stored entry name to the flat key it actually represents.
	//
	// Entry names are flat keys verbatim, with exactly one exception that ZIP itself
	// imposes: a name ending in "/" denotes a directory, and Go's writer refuses to write
	// a body to one. A key can end in "/" when its final path segment is empty, and the
	// root key "<nodeId>-/" has that shape too — every persistence site strips that key
	// before writing, but a format that fails a unit outright if one ever survives is not
	// one worth shipping.
	//
	// Such keys are stored under a generated reserved name and recovered from here. The
	// generated name lives under ReservedPrefix, so it cannot collide with a payload key,
	// and this map stays empty for every ordinary payload.
	Renames map[string]string `json:"renames,omitempty"`
}

// storableName reports the entry name a flat key is written under, and whether that
// differs from the key itself.
func storableName(key string, index int) (string, bool) {
	if !strings.HasSuffix(key, "/") {
		return key, false
	}
	return fmt.Sprintf("%sk/%d", ReservedPrefix, index), true
}

// Reference locates a value living in another blob's archive.
//
// ETag, CRC32 and Length are carried so a follower can pin the target's identity and tell
// a deterministic re-execution apart from genuine content change.
type Reference struct {
	BlobURL string `json:"blob_url"`
	Entry   string `json:"entry"`
	ETag    string `json:"etag,omitempty"`
	CRC32   uint32 `json:"crc32"`
	Length  int64  `json:"length"`
}

// IsArchive reports whether data begins with the ZIP local file header signature.
//
// Format is detected from the bytes rather than from the blob's name or a flag on the
// message. During the rollout window both forms exist — the reader ships one deploy ahead
// of writing being switched on — and a run in flight can hold a document written by an
// earlier node beside an archive written by a later one. Sniffing removes any dependence
// on the two sides agreeing about which is which.
func IsArchive(data []byte) bool {
	return len(data) >= 4 && data[0] == 'P' && data[1] == 'K' && data[2] == 0x03 && data[3] == 0x04
}

// isJSONArray reports whether a raw JSON value is an array, by its first significant byte.
//
// This is exact for well-formed JSON and costs nothing: the alternative, unmarshalling
// every value to look at its Go type, would defeat the point of storing values verbatim.
func isJSONArray(raw []byte) bool {
	for _, b := range raw {
		switch b {
		case ' ', '\t', '\r', '\n':
			continue
		case '[':
			return true
		default:
			return false
		}
	}
	return false
}

// isStandardUnitOutput reports whether a decoded JSON object is a node-output document
// rather than some other object that merely happens to parse.
//
// The test is the flat-key shape itself: FlattenMap writes "<nodeId>-/<path>" for every
// key it emits ([flatten.go:35-68]), so a document's keys all carry "-/" after a non-empty
// node id, and the resolver already splits them on exactly that separator.
//
// The discrimination matters because the two forms round trip differently. A document is
// stored as one entry per key and re-materialised by marshalling those keys back into an
// object, which is lossless in JSON terms but not byte for byte: key order and whitespace
// are the writer's. That is correct for a node output, whose consumer is the field-mapping
// extractor and which was itself produced by a marshal. It is wrong for a trigger body,
// whose consumer is a plugin handed the bytes a caller actually sent — so an arbitrary
// JSON object is carried opaquely and comes back unchanged.
func isStandardUnitOutput(flat map[string]json.RawMessage) bool {
	if len(flat) == 0 {
		// Nothing to judge by. Treated as opaque so the bytes survive untouched, which is
		// the safer answer when the shape cannot be established.
		return false
	}
	for key := range flat {
		if i := strings.Index(key, "-/"); i <= 0 {
			return false
		}
	}
	return true
}

// decodeFlat parses a StandardUnitOutput document into its flat keys, keeping each value's
// bytes exactly as they appeared so they can be stored verbatim.
func decodeFlat(doc []byte) (map[string]json.RawMessage, error) {
	if len(bytes.TrimSpace(doc)) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	var flat map[string]json.RawMessage
	if err := json.Unmarshal(doc, &flat); err != nil {
		return nil, fmt.Errorf("archive: payload is not a flat JSON object: %w", err)
	}
	return flat, nil
}
