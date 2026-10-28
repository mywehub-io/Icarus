package tests

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/simplecondition"
)

// A numeric comparison on a field that is not a number fails the node. The error becomes log text
// and node output, so it gives the field's length, not its value. The value is synthetic.
func TestProcessNumericComparisonErrorDoesNotEchoValue(t *testing.T) {
	node := createConditionNode(t, "test-condition-node")
	cfg := simplecondition.Config{
		LogicOperator: simplecondition.LogicAnd,
		ManualInputs: []simplecondition.ManualInput{{
			Name:     "nhs_number",
			Type:     simplecondition.DataTypeNumber,
			Value:    "10",
			Operator: simplecondition.OpGreaterThan,
		}},
	}
	rawConfig, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	output := node.Process(createProcessInput(map[string]interface{}{"nhs_number": "SYNTH-999-000-1234"}, rawConfig))

	if output.Error == nil {
		t.Fatal("expected a comparison error")
	}
	msg := output.Error.Error()
	if strings.Contains(msg, "SYNTH-999-000-1234") || strings.Contains(msg, "1234") {
		t.Errorf("error %q echoes the input value", msg)
	}
	if !strings.Contains(msg, "cannot convert string of length 18 to number") {
		t.Errorf("error %q should give the value's length", msg)
	}
}
