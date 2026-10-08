package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/wehubfusion/Icarus/pkg/embedded/processors/jsrunner"
	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
)

// One node shared by many workers: every item runs on its own VM from the one compiled program,
// with no data race (run with -race) and each item seeing only its own input.
func TestConcurrentItemsShareOneNode(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{NodeId: "js", PluginType: "plugin-js"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]interface{}{"script": "return { doubled: input.n * 2 };", "timeout": "5s"})
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out := node.Process(runtime.ProcessInput{Ctx: context.Background(), Data: map[string]interface{}{"n": i}, RawConfig: raw, ItemIndex: i})
			if out.Error != nil {
				errs <- out.Error
				return
			}
			if got := fmt.Sprint(out.Data["doubled"]); got != fmt.Sprint(i*2) {
				errs <- fmt.Errorf("item %d got %v", i, got)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// A setTimeout callback runs on the VM's goroutine after the synchronous script, before the result
// is taken (it used to run on the timer's goroutine while the VM was in use).
func TestTimerCallbackRunsAfterTheScript(t *testing.T) {
	node, err := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{NodeId: "js", PluginType: "plugin-js"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]interface{}{
		"script":            "var out = {order: ['script']}; setTimeout(function(){ out.order.push('timer'); }, 0); var t = Date.now(); while (Date.now() - t < 20) {} return out;",
		"timeout":           "5s",
		"security_level":    "permissive",
		"enabled_utilities": []string{"timers", "json"},
	})
	out := node.Process(runtime.ProcessInput{Ctx: context.Background(), Data: map[string]interface{}{}, RawConfig: raw})
	if out.Error != nil {
		t.Fatal(out.Error)
	}
	order, _ := json.Marshal(out.Data["order"])
	if string(order) != `["script","timer"]` {
		t.Fatalf("want the timer after the script, got %s", order)
	}
}

// A syntax error is reported the same way with the compiled program.
func TestSyntaxErrorIsReported(t *testing.T) {
	node, _ := jsrunner.NewJSRunnerNode(runtime.EmbeddedNodeConfig{NodeId: "js", PluginType: "plugin-js"})
	raw, _ := json.Marshal(map[string]interface{}{"script": "return {a: ;", "timeout": "5s"})
	for i := 0; i < 2; i++ { // the cached failure is returned on the second call too
		if out := node.Process(runtime.ProcessInput{Ctx: context.Background(), Data: map[string]interface{}{}, RawConfig: raw}); out.Error == nil {
			t.Fatalf("call %d: want a syntax error", i)
		}
	}
}
