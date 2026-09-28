package resolver

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/archive"
	"github.com/wehubfusion/Icarus/pkg/message"
)

// The inline path is the one the archive change must not touch, and it is the one most
// executions take: a result under maxInlineBytes never reaches storage at all, it rides in
// the NATS message and is read straight from the consumer graph.
//
// That makes it the largest blast radius in the change and, until this file, the least
// covered — nothing asserted that a small payload stays unarchived. The decision is made on
// the payload as given, before any format choice, so ZIP framing can never push something
// over the threshold, and the bytes a consumer receives are the bytes the producer wrote.

func TestCreateResultKeepsSmallPayloadsInlineAndVerbatim(t *testing.T) {
	doc := []byte(`{"up-/name":"david","up-/rows":[{"name":"a"},{"name":"b"}],"up-/meta/count":2}`)

	cases := []struct {
		name    string
		payload []byte
	}{
		// A StandardUnitOutput document: the shape that WOULD be archived above the
		// threshold. Below it, it must stay exactly as marshalled.
		{"node output document", doc},
		// A raw HL7 body, as MLLP ingest offloads. Small ones never reach blob at all.
		{"hl7", []byte("MSH|^~\\&|SENDER|FAC|RECV|FAC|20260928||ADT^A01|MSG0001|P|2.5\rPID|1||12345^^^FAC^MR||SMITH^JOHN\r")},
		// An HTTP trigger body, where byte-identity is what the caller is owed.
		{"http json body", []byte("{ \"zebra\" : 1,\n  \"apple\": 2 }")},
		{"csv", []byte("id,name\r\n1,david\r\n")},
		{"single byte", []byte("x")},
	}

	for _, tc := range cases {
		// A fake that records uploads, so "nothing was written" is asserted against what
		// reached storage rather than against the returned struct alone.
		fake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}}
		svc := NewService(fake, DefaultMaxInlineBytes)

		res, err := svc.CreateResult(context.Background(), tc.payload, ResultMeta{
			WorkflowID: "wf", RunID: "run", NodeID: "n", ExecutionID: "exec",
		})
		if err != nil {
			t.Fatalf("%s: CreateResult: %v", tc.name, err)
		}
		if res.BlobReference != nil {
			t.Fatalf("%s: a payload under the threshold went to blob (%s)", tc.name, res.BlobReference.URL)
		}
		if res.UsedBlob {
			t.Fatalf("%s: UsedBlob set for an inline result", tc.name)
		}
		if len(fake.uploads) != 0 {
			t.Fatalf("%s: %d blob(s) written for an inline result", tc.name, len(fake.uploads))
		}
		if !bytes.Equal(res.InlineData, tc.payload) {
			t.Fatalf("%s: inline data is not the payload verbatim\n got %q\nwant %q",
				tc.name, res.InlineData, tc.payload)
		}
		// The decisive one: an inline result must never be an archive. A consumer reads
		// InlineData directly from the dispatch message and does no unwrapping.
		if archive.IsArchive(res.InlineData) {
			t.Fatalf("%s: inline data was archived", tc.name)
		}
	}
}

// The threshold is a boundary, and boundaries are where off-by-one lives. Exactly at the
// limit is inline; one byte over is an archive. Asserting both sides together is what makes
// this meaningful — either alone passes under a shifted comparison.
func TestCreateResultThresholdBoundary(t *testing.T) {
	const limit = 2048

	// Build documents whose marshalled length lands exactly on and just over the limit by
	// padding one value, so both sides are the same shape and differ only in size.
	build := func(total int) []byte {
		t.Helper()
		for pad := 0; pad < total+64; pad++ {
			b, err := json.Marshal(map[string]string{"up-/pad": strings.Repeat("x", pad)})
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if len(b) == total {
				return b
			}
			if len(b) > total {
				break
			}
		}
		t.Fatalf("could not build a document of exactly %d bytes", total)
		return nil
	}

	atLimit := build(limit)
	overLimit := build(limit + 1)

	for _, tc := range []struct {
		name       string
		payload    []byte
		wantInline bool
	}{
		{"exactly at the threshold", atLimit, true},
		{"one byte over", overLimit, false},
	} {
		fake := &fakeBlobClient{bodies: map[string]string{}, uploads: map[string][]byte{}}
		svc := NewService(fake, limit)

		res, err := svc.CreateResult(context.Background(), tc.payload, ResultMeta{
			WorkflowID: "wf", RunID: "run", NodeID: "n", ExecutionID: "exec-" + tc.name,
		})
		if err != nil {
			t.Fatalf("%s: CreateResult: %v", tc.name, err)
		}

		gotInline := res.BlobReference == nil
		if gotInline != tc.wantInline {
			t.Fatalf("%s: payload of %d bytes against a %d limit went inline=%v, want %v",
				tc.name, len(tc.payload), limit, gotInline, tc.wantInline)
		}

		if tc.wantInline {
			if !bytes.Equal(res.InlineData, tc.payload) {
				t.Fatalf("%s: inline data is not verbatim", tc.name)
			}
			continue
		}
		// Over the threshold it becomes an archive, and the archive is larger than the
		// document — which is exactly why the comparison must happen on the payload as
		// given and never on the stored form. Doing it the other way round would let ZIP
		// framing push a payload over a threshold it was under.
		stored := fake.uploads[res.BlobReference.URL]
		if !archive.IsArchive(stored) {
			t.Fatalf("%s: payload over the threshold was not archived", tc.name)
		}
		if len(stored) <= len(tc.payload) {
			t.Fatalf("%s: archive (%d) is not larger than the document (%d); the premise of "+
				"deciding on the unstored payload no longer holds", tc.name, len(stored), len(tc.payload))
		}
	}
}

// An inline result is consumed straight from the consumer graph, with no storage round trip
// and no format handling. This pins that the whole archive read path stays out of the way:
// the fake would error on any call, so a single download attempt fails the test.
func TestInlineSourceResolvesWithoutTouchingStorage(t *testing.T) {
	doc := `{"up-/name":"david","up-/rows":[{"name":"a"},{"name":"b"}]}`

	// No bodies and no uploads: any storage call this path makes returns an error.
	fake := &fakeBlobClient{bodies: map[string]string{}}
	svc := NewService(fake, DefaultMaxInlineBytes)

	graph := NewConsumerGraph()
	graph.ResultLocations["up"] = &ResultLocation{
		NodeID:        "up",
		ExecutionID:   "exec1",
		StorageType:   "inline",
		HasInlineData: true,
		InlineData:    json.RawMessage(doc),
	}

	mappings := []message.FieldMapping{
		{SourceNodeID: "up", SourceEndpoint: "/name", DestinationEndpoints: []string{"/name"}},
		{SourceNodeID: "up", SourceEndpoint: "/rows//name", DestinationEndpoints: []string{"/names"}},
	}

	out, err := svc.ResolveMappedInputWithConsumerGraph(context.Background(), nil, nil,
		&FieldMappingParams{FieldMappings: mappings, ConsumerGraph: graph}, graph)
	if err != nil {
		t.Fatalf("resolve inline source: %v", err)
	}
	for _, want := range []string{"david", "a", "b"} {
		if !bytes.Contains(out, []byte(want)) {
			t.Fatalf("inline resolve lost %q: %s", want, out)
		}
	}
}
