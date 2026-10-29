package strings_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	stringsproc "github.com/wehubfusion/Icarus/pkg/embedded/processors/strings"
)

// A failed uri_decode does not quote the bad escape, which is a piece of the input. The input is
// synthetic.
func TestProcessURIDecodeErrorDoesNotEchoInput(t *testing.T) {
	node := createTestNode(t, "test-node-1")
	rawConfig, _ := json.Marshal(stringsproc.Config{Action: "uri_decode"})
	output := node.Process(createProcessInput(map[string]interface{}{"value": "SYNTH%Q9"}, rawConfig, 0))

	if output.Error == nil {
		t.Fatal("expected a uri_decode error")
	}
	if strings.Contains(output.Error.Error(), "%Q9") {
		t.Errorf("error %q quotes the input", output.Error)
	}
	if errors.Unwrap(output.Error) != nil {
		t.Errorf("error wraps %v, which quotes the input", errors.Unwrap(output.Error))
	}
}
