package json

import (
	stdjson "encoding/json"
	"strings"
	"testing"
)

// An out-of-range number is reported by its length, not its literal, which is input data. The
// literal is synthetic.
func TestScalarValue_OutOfRangeNumberDoesNotEchoLiteral(t *testing.T) {
	_, err := scalarValue(stdjson.Number("4321e999"))
	if err == nil {
		t.Fatal("expected an error for a number out of float64 range")
	}
	if strings.Contains(err.Error(), "4321") {
		t.Errorf("error %q echoes the number", err)
	}
}
