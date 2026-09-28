package resolver

import (
	"sort"
	"strings"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// wholeFileFetchRatio is the share of an archive's payload above which the planner stops
// issuing ranged reads and downloads the blob once instead.
//
// Above this point many small GETs cost more in round trips than one large GET costs in
// bytes, and the archive's own framing means "nearly all the keys" is close to "the whole
// file" anyway. Below it, selective fetch is a real saving.
const wholeFileFetchRatio = 0.6

// fetchPlan is the outcome of applying the completeness rule to one archive.
type fetchPlan struct {
	// keys is the exact entry set to fetch, sorted, when wholeFile is false.
	keys []string
	// wholeFile means fetch the blob in one request and read every entry from memory.
	wholeFile bool
}

// planArchiveFetch decides which entries of one archive a unit must read.
//
// This is the completeness rule, and it is the single correctness constraint the whole
// read path rests on. Getting it wrong does not raise an error: the array-handling code
// downstream sizes arrays from the highest index it can see, so an incomplete key set
// yields a silently shorter array.
//
// The rule is conservative per source node. Fetch the exact keys only when every mapping
// from that node is an exact-key lookup; fetch all of that node's keys as soon as one is
// not. A node's conservative verdict never drags another node in the same file up with it.
func planArchiveFetch(a *archive.Reader, containsNodes []string, mappings []message.FieldMapping) fetchPlan {
	inFile := make(map[string]bool, len(containsNodes))
	for _, n := range containsNodes {
		inFile[n] = true
	}

	// Group every entry name by the node that emitted it, so "all of this node's keys"
	// is answerable without rescanning.
	nodeKeys := make(map[string][]string)
	for _, name := range a.Names() {
		if node, ok := nodeIDFromFlatKey(name); ok {
			nodeKeys[node] = append(nodeKeys[node], name)
		}
	}

	selected := make(map[string]bool)
	wholeNode := make(map[string]bool)

	for _, m := range mappings {
		if m.IsEventTrigger || m.SourceNodeID == "" || !inFile[m.SourceNodeID] {
			continue
		}
		if wholeNode[m.SourceNodeID] {
			continue // already conservative; nothing a further mapping can add
		}

		key, exact := classifyMapping(a, m)
		if !exact {
			wholeNode[m.SourceNodeID] = true
			continue
		}
		if key != "" {
			selected[key] = true
		}
	}

	for node := range wholeNode {
		for _, k := range nodeKeys[node] {
			selected[k] = true
		}
	}

	keys := make([]string, 0, len(selected))
	for k := range selected {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// One large request beats many small ones once the selection approaches the whole
	// payload. Sizes come from the central directory, so this costs no extra fetch.
	total := a.TotalPayloadBytes()
	if total > 0 {
		var selectedBytes int64
		for _, k := range keys {
			if n, ok := a.Size(k); ok {
				selectedBytes += n
			}
		}
		if float64(selectedBytes) >= wholeFileFetchRatio*float64(total) {
			return fetchPlan{wholeFile: true}
		}
	}

	return fetchPlan{keys: keys}
}

// classifyMapping decides whether one mapping can be served by a single named entry.
//
// It returns (key, true) for an exact-key lookup, and ("", false) when the extraction this
// mapping will drive scans by prefix and therefore needs every key of the node.
//
// The classification is made against the archive, never against the path string alone.
// Two of the five routes into a prefix scan are invisible in the endpoint: Iterate is a
// flag on the mapping, and a key being absent is a property of the archive.
func classifyMapping(a *archive.Reader, m message.FieldMapping) (string, bool) {
	prefix := m.SourceNodeID + "-/"
	endpoint := m.SourceEndpoint

	// Iterate turns extraction into a prefix scan whatever the path looks like, and the
	// resulting array is sized from the highest first-index among the matched keys — so a
	// partial key set truncates it.
	if m.Iterate {
		return "", false
	}

	// The root handle is a first-class designer port meaning the whole node output. It is
	// served by reconstructing the node from every one of its leaf keys, never by a stored
	// root key: every persistence site strips that key before writing.
	if endpoint == "" || endpoint == "/" {
		return "", false
	}

	if strings.Contains(endpoint, "//") {
		parts := strings.SplitN(endpoint, "//", 2)
		if len(parts) != 2 {
			return "", false
		}
		collection := strings.Trim(parts[0], "/")
		field := strings.Trim(parts[1], "/")
		if collection == "" || field == "" {
			return "", false
		}
		key := prefix + collection
		// Presence alone is not the test. Collection traversal returns from this single
		// key only when it exists *and* holds an array; an entry holding an object falls
		// through into the same prefix scan.
		if a.Has(key) && a.IsArray(key) {
			return key, true
		}
		return "", false
	}

	key := prefix + strings.TrimPrefix(endpoint, "/")
	if a.Has(key) {
		return key, true
	}

	// The key is absent. A plugin-error mapping is *expected* to miss when the upstream
	// node completed without writing /error, and is answered with a synthesised default
	// before any structural extraction runs, so it needs nothing fetched.
	if m.SourceSectionId == runtime.SectionPluginError && pluginErrorSectionDefault(endpoint) != nil {
		return "", true
	}

	// Otherwise the miss is not the end of the attempt: extraction falls through to a
	// structural read over the node reconstructed from every key carrying its prefix. That
	// reconstruction is what would silently come up empty under a partial fetch, so an
	// absent key widens the fetch rather than narrowing it.
	return "", false
}

// nodeIDFromFlatKey splits "<nodeId>-/<path>" at the first "-/" separator.
func nodeIDFromFlatKey(key string) (string, bool) {
	i := strings.Index(key, "-/")
	if i <= 0 {
		return "", false
	}
	return key[:i], true
}
