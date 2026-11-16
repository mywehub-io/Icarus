package message

import (
	"testing"

	"github.com/nats-io/nats.go"
)

func TestIsReflection(t *testing.T) {
	const subject = "HTTP_REQUESTS_UAT.runnerB.env.exec1"
	unit, err := NewMessage().ToBytes()
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		subject string
		h       nats.Header
		data    []byte
		want    bool
	}{
		// Genuine messages.
		{name: "no headers", subject: subject, data: unit},
		{name: "Nats-Msg-Id", subject: subject, h: nats.Header{"Nats-Msg-Id": {"wf:run:node"}}, data: unit},
		{name: "KV put", subject: "$KV.EXECUTION_HEARTBEATS.a.b.c", h: nats.Header{"KV-Operation": {"PUT"}}, data: []byte(`{"state":"running"}`)},
		{name: "KV delete marker", subject: "$KV.EXECUTION_HEARTBEATS.a.b.c", h: nats.Header{"KV-Operation": {"DEL"}}},
		{name: "genuine direct get answer", subject: "$KV.EXECUTION_HEARTBEATS.a.b.c", h: nats.Header{
			"Nats-Stream":     {"KV_EXECUTION_HEARTBEATS"},
			"Nats-Subject":    {"$KV.EXECUTION_HEARTBEATS.a.b.c"},
			"Nats-Sequence":   {"7"},
			"Nats-Time-Stamp": {"2026-11-13T10:00:00Z"},
		}, data: []byte(`{"state":"running"}`)},
		{name: "JSON with a stream key among others", subject: subject, data: []byte(`{"stream":"X","seq":1,"payload":{}}`)},
		{name: "JSON with stream but no seq", subject: subject, data: []byte(`{"stream":"X"}`)},
		{name: "type that is not a JetStream API", subject: subject, data: []byte(`{"type":"workflow.unit"}`)},
		{name: "empty body", subject: subject},

		// Reflections.
		{name: "Nats-Subject differs", subject: subject, want: true, h: nats.Header{
			"Nats-Stream":  {"KV_RUNNER_A"},
			"Nats-Subject": {"$KV.RUNNER_A.mine"},
		}, data: unit},
		{name: "second Nats-Subject differs", subject: "$KV.EXECUTION_HEARTBEATS.a.b.c", want: true, h: nats.Header{
			"Nats-Stream":  {"KV_RUNNER_A", "KV_EXECUTION_HEARTBEATS"},
			"Nats-Subject": {"$KV.EXECUTION_HEARTBEATS.a.b.c", "$KV.RUNNER_A.mine"},
		}, data: []byte(`{}`)},
		{name: "Nats-Subject in another case", subject: subject, want: true, h: nats.Header{"nats-subject": {"other"}}},
		{name: "Status header", subject: subject, want: true, h: nats.Header{"Status": {"408"}, "Description": {"Empty Request"}}},
		{name: "Nats-Stream without Nats-Subject", subject: subject, want: true, h: nats.Header{"Nats-Stream": {"KV_RUNNER_A"}}, data: unit},
		{name: "JetStream API response", subject: subject, want: true, data: []byte(`{"type":"io.nats.jetstream.api.v1.stream_info_response","config":{}}`)},
		{name: "JetStream API response after whitespace", subject: subject, want: true, data: []byte("\n  {\"type\":\"io.nats.jetstream.api.v1.consumer_create_response\"}")},
		{name: "JetStream API response spaced", subject: subject, want: true, data: []byte(`{ "type" : "io.nats.jetstream.api.v1.stream_names_response" }`)},
		{name: "publish ack", subject: subject, want: true, data: []byte(`{"stream":"KV_RUNNER_A","seq":42}`)},
		{name: "publish ack with domain and duplicate", subject: subject, want: true, data: []byte(` {"stream":"X","domain":"hub","seq":3,"duplicate":true}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := IsReflection(tc.subject, tc.h, tc.data)
			if got != tc.want {
				t.Fatalf("IsReflection = %v (%q), want %v", got, reason, tc.want)
			}
			if got && reason == "" {
				t.Fatal("a reflection must say why")
			}
		})
	}
}
