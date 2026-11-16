package message

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
)

// jsAPIResponsePrefix starts every JetStream API response body.
const jsAPIResponsePrefix = `{"type":"io.nats.jetstream.api.`

// maxPubAckBody bounds the body IsReflection decodes to look for a publish ack. An ack is a few
// dozen bytes; a larger body is never one, and is not decoded.
const maxPubAckBody = 1024

// pubAckKeys are the keys a JetStream publish ack can carry.
var pubAckKeys = map[string]bool{"stream": true, "seq": true, "domain": true, "duplicate": true}

// IsReflection reports whether a message that arrived on subject, with headers h and body data, is
// an answer the NATS server published to a reply subject someone else chose, and returns why.
//
// NATS does not check the reply subject of a request, so a client allowed to send requests can
// make the server publish its answer to any subject, and a stream capturing that subject stores
// it. Such a message is one of:
//   - a direct get answer: the stored message's headers plus the server's Nats-Stream,
//     Nats-Subject, Nats-Sequence and Nats-Time-Stamp, where Nats-Subject names the subject that
//     was read, not subject; or an empty body with a Status header for a request the server
//     refused;
//   - a publish ack or a JetStream API response, with no headers and a JSON body.
//
// A genuine direct get answer read from subject carries Nats-Subject equal to subject, so it is
// not reported. Every Nats-Subject value is checked: a direct get of a stored reflection carries
// the stored one and the server's own. Header names are matched without regard to case.
func IsReflection(subject string, h nats.Header, data []byte) (bool, string) {
	var hasSubject, hasStream bool
	for k, vals := range h {
		switch {
		case strings.EqualFold(k, "Nats-Subject"):
			for _, v := range vals {
				hasSubject = true
				if v != subject {
					return true, fmt.Sprintf("Nats-Subject header %q differs from the subject", v)
				}
			}
		case strings.EqualFold(k, "Status"):
			return true, "Status header present"
		case strings.EqualFold(k, "Nats-Stream"):
			if len(vals) > 0 {
				hasStream = true
			}
		}
	}
	if hasStream && !hasSubject {
		return true, "Nats-Stream header without Nats-Subject"
	}

	body := bytes.TrimLeft(data, " \t\r\n")
	if bytes.HasPrefix(body, []byte(jsAPIResponsePrefix)) {
		return true, "body is a JetStream API response"
	}
	if len(body) == 0 || body[0] != '{' || len(body) > maxPubAckBody {
		return false, ""
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false, ""
	}
	if raw, ok := fields["type"]; ok {
		var typ string
		if json.Unmarshal(raw, &typ) == nil && strings.HasPrefix(typ, "io.nats.jetstream.api.") {
			return true, "body is a JetStream API response"
		}
	}
	_, hasStreamKey := fields["stream"]
	_, hasSeqKey := fields["seq"]
	if !hasStreamKey || !hasSeqKey {
		return false, ""
	}
	for k := range fields {
		if !pubAckKeys[k] {
			return false, ""
		}
	}
	return true, "body is a JetStream publish ack"
}
