package tests

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/dop251/goja"
	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

func runScript(t *testing.T, script string, data map[string]interface{}) runtime.ProcessOutput {
	t.Helper()
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{NodeId: "js", PluginType: "plugin-js"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]interface{}{"script": script, "timeout": "5s"})
	return node.Process(runtime.ProcessInput{Ctx: context.Background(), Data: data, RawConfig: raw, ItemIndex: 0})
}

// A thrown message is bounded: a script that dumps the item into what it throws does not put the
// whole item in the node error. The type and goja's position suffix stay, as the wehub CLI parses
// it. The data is synthetic.
func TestProcess_ExceptionMessageIsBounded(t *testing.T) {
	tail := strings.Repeat("x", 300) + "SYNTHETIC-TAIL-8841"
	out := runScript(t, "var a = 1;\nthrow new TypeError('bad item ' + input.blob);", map[string]interface{}{"blob": tail})

	var execErr *jsrunner.ExecutionError
	if !errors.As(out.Error, &execErr) {
		t.Fatalf("expected an ExecutionError, got %v", out.Error)
	}
	msg := out.Error.Error()
	if strings.Contains(msg, "SYNTHETIC-TAIL-8841") {
		t.Errorf("error is not bounded: %q", msg)
	}
	if !strings.HasPrefix(execErr.Message, "TypeError: bad item ") {
		t.Errorf("message should keep the exception type, got %q", execErr.Message)
	}
	// The wrapper adds a first line, so the author's line 2 is line 3 of script.js, as before.
	if !regexp.MustCompile(`\.\.\. \(truncated\) at script\.js:3:\d+\(\d+\)$`).MatchString(execErr.Message) {
		t.Errorf("message should mark the cut and keep the position, got %q", execErr.Message)
	}
	if n := len([]rune(execErr.Message)); n > 200 {
		t.Errorf("message is %d runes", n)
	}
	// The full exception stays reachable in process, for a caller that needs it.
	var ex *goja.Exception
	if !errors.As(out.Error, &ex) || !strings.Contains(ex.Error(), "SYNTHETIC-TAIL-8841") {
		t.Errorf("the full exception should stay reachable as Err")
	}
}

// A short message is kept whole, in the format goja gives it.
func TestProcess_ShortExceptionMessageIsKept(t *testing.T) {
	out := runScript(t, "throw new Error('test error')", map[string]interface{}{})
	var execErr *jsrunner.ExecutionError
	if !errors.As(out.Error, &execErr) {
		t.Fatalf("expected an ExecutionError, got %v", out.Error)
	}
	var ex *goja.Exception
	if !errors.As(out.Error, &ex) || execErr.Message != ex.Error() {
		t.Errorf("message %q should equal goja's own %q", execErr.Message, ex.Error())
	}
}
