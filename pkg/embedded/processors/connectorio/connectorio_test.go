package connectorio

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

const patientSchema = `{"type":"OBJECT","properties":{
	"nhs_number":{"type":"STRING","required":true},
	"name":{"type":"STRING"}}}`

func process(t *testing.T, factory func(runtime.EmbeddedNodeConfig) (runtime.EmbeddedNode, error), plugin string, schema string, data map[string]interface{}) runtime.ProcessOutput {
	t.Helper()
	node, err := factory(runtime.EmbeddedNodeConfig{NodeId: "n1", PluginType: plugin, Label: "Start"})
	if err != nil {
		t.Fatal(err)
	}
	cfg := map[string]interface{}{"schema_id": "s1"}
	if schema != "" {
		cfg["schema"] = json.RawMessage(schema)
	}
	raw, _ := json.Marshal(cfg)
	return node.Process(runtime.ProcessInput{NodeId: "n1", Label: "Start", PluginType: plugin, Data: data, RawConfig: raw, ItemIndex: -1})
}

func codeOf(err error) string {
	var coded *runtime.CodedError
	if errors.As(err, &coded) {
		return coded.Code
	}
	return ""
}

func TestValidInputPassesThroughUnchanged(t *testing.T) {
	in := map[string]interface{}{"nhs_number": "9000000009", "name": "Test Patient"}
	out := process(t, NewInputNode, PluginInput, patientSchema, in)
	if out.Error != nil {
		t.Fatalf("unexpected error: %v", out.Error)
	}
	if out.Data["nhs_number"] != "9000000009" || out.Data["name"] != "Test Patient" || len(out.Data) != 2 {
		t.Fatalf("output changed: %v", out.Data)
	}
}

func TestInputMismatchFailsWithInputCode(t *testing.T) {
	out := process(t, NewInputNode, PluginInput, patientSchema, map[string]interface{}{"name": "Test Patient"})
	if out.Error == nil {
		t.Fatal("expected a failure for a missing required field")
	}
	if got := codeOf(out.Error); got != CodeInputInvalid {
		t.Fatalf("code = %q, want %q (%v)", got, CodeInputInvalid, out.Error)
	}
}

func TestResultMismatchFailsWithResultCode(t *testing.T) {
	out := process(t, NewResultNode, PluginResult, patientSchema, map[string]interface{}{"nhs_number": 42})
	if got := codeOf(out.Error); got != CodeResultInvalid {
		t.Fatalf("code = %q, want %q (%v)", got, CodeResultInvalid, out.Error)
	}
}

func TestSchemaNotEnrichedIsUnavailable(t *testing.T) {
	out := process(t, NewResultNode, PluginResult, "", map[string]interface{}{"nhs_number": "1"})
	if got := codeOf(out.Error); got != CodeSchemaUnavailable {
		t.Fatalf("code = %q, want %q", got, CodeSchemaUnavailable)
	}
}

func TestWrongPluginTypeRefused(t *testing.T) {
	if _, err := NewInputNode(runtime.EmbeddedNodeConfig{PluginType: PluginResult}); err == nil {
		t.Fatal("expected the input factory to refuse another plugin type")
	}
}

const ordersCSV = `{"name":"orders","delimiter":",","columnHeaders":{"order_id":{"position":0,"type":"STRING","required":true},"amount":{"position":1,"type":"NUMBER","required":false}}}`

const admitHL7 = `{"segments":[{"name":"MSH","usage":"R","rpt":"1","fields":[{"position":"MSH-1","dataType":"ST","usage":"R"},{"position":"MSH-2","dataType":"ST","usage":"R"},{"position":"MSH-3","dataType":"ST","usage":"O"},{"position":"MSH-4","dataType":"ST","usage":"O"},{"position":"MSH-5","dataType":"ST","usage":"O"},{"position":"MSH-6","dataType":"ST","usage":"O"},{"position":"MSH-7","dataType":"ST","usage":"O"},{"position":"MSH-8","dataType":"ST","usage":"O"},{"position":"MSH-9","dataType":"ST","usage":"R"},{"position":"MSH-10","dataType":"ST","usage":"O"},{"position":"MSH-11","dataType":"ST","usage":"O"},{"position":"MSH-12","dataType":"ST","usage":"O"}]},{"name":"PID","usage":"R","rpt":"1","fields":[{"position":"PID-1","dataType":"ST","usage":"O"},{"position":"PID-2","dataType":"ST","usage":"O"},{"position":"PID-3","dataType":"ST","usage":"R"},{"position":"PID-4","dataType":"ST","usage":"O"},{"position":"PID-5","dataType":"ST","usage":"O"},{"position":"PID-6","dataType":"ST","usage":"O"},{"position":"PID-7","dataType":"ST","usage":"O"}]}]}`

// NFR-10: a CSV and an HL7 schema validate the same way as a JSON one.
func TestValidateEachFormat(t *testing.T) {
	if err := Validate([]byte(ordersCSV), map[string]interface{}{"data": []interface{}{
		map[string]interface{}{"order_id": "ORD-1", "amount": 3}}}, CodeInputInvalid); err != nil {
		t.Fatalf("valid CSV rows: %v", err)
	}
	if codeOf(Validate([]byte(ordersCSV), map[string]interface{}{"data": []interface{}{
		map[string]interface{}{"amount": 3}}}, CodeInputInvalid)) != CodeInputInvalid {
		t.Fatal("a CSV row missing a required column must be refused")
	}
	if codeOf(Validate([]byte(ordersCSV), map[string]interface{}{"data": "not rows"}, CodeInputInvalid)) != CodeInputInvalid {
		t.Fatal("CSV rows that are not a list must be refused")
	}
	msg := "MSH|^~\\&|SEND|FAC|RECV|FAC|20260101000000||ADT^A01|1|P|2.5\rPID|1||12345^^^HOSP^MR\r"
	if err := Validate([]byte(admitHL7), map[string]interface{}{"value": msg}, CodeInputInvalid); err != nil {
		t.Fatalf("valid HL7: %v", err)
	}
	if codeOf(Validate([]byte(admitHL7), map[string]interface{}{"value": "MSH|^~\\&|SEND|FAC|RECV|FAC|20260101000000||ADT^A01|1|P|2.5\r"}, CodeInputInvalid)) != CodeInputInvalid {
		t.Fatal("an HL7 message missing a required segment must be refused")
	}
}
