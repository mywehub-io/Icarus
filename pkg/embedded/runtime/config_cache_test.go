package runtime

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

type cacheCfg struct{ N int }

// The configuration is parsed once however many items and workers read it, a different
// configuration is parsed afresh, and a configuration that does not parse keeps its error.
func TestConfigCacheParsesOnce(t *testing.T) {
	var parses atomic.Int32
	parse := func(raw json.RawMessage) (*cacheCfg, error) {
		parses.Add(1)
		return ParseJSON[cacheCfg](raw)
	}
	var c ConfigCache[cacheCfg]
	raw := json.RawMessage(`{"N":7}`)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cfg, err := c.Get(raw, parse)
			if err != nil || cfg.N != 7 {
				t.Errorf("cfg %+v err %v", cfg, err)
			}
		}()
	}
	wg.Wait()
	// Racing first readers may each parse; once one has stored it, none does.
	before := parses.Load()
	for i := 0; i < 1000; i++ {
		if _, err := c.Get(raw, parse); err != nil {
			t.Fatal(err)
		}
	}
	if parses.Load() != before {
		t.Fatalf("parsed again after the first parse: %d -> %d", before, parses.Load())
	}
	if before > 64 {
		t.Fatalf("parsed %d times for 64 first readers", before)
	}

	if cfg, _ := c.Get(json.RawMessage(`{"N":9}`), parse); cfg.N != 9 {
		t.Fatalf("a different configuration must be parsed afresh, got %+v", cfg)
	}

	bad := json.RawMessage(`{"N":`)
	_, err1 := c.Get(bad, parse)
	n := parses.Load()
	_, err2 := c.Get(bad, parse)
	if err1 == nil || err2 == nil || !errors.Is(err2, err1) && err1.Error() != err2.Error() {
		t.Fatalf("errors %v / %v", err1, err2)
	}
	if parses.Load() != n {
		t.Fatal("a configuration that does not parse must keep its error, not re-parse")
	}
}

type preparedStub struct {
	BaseNode
	prepared atomic.Value
}

func (p *preparedStub) Process(ProcessInput) ProcessOutput {
	return SuccessOutput(map[string]interface{}{})
}
func (p *preparedStub) Prepare(raw json.RawMessage) { p.prepared.Store(string(raw)) }

// The SubflowProcessor gives a node its normalised configuration when it builds it, with the action
// injected, so a node parses it once before its first item.
func TestSubflowProcessorPreparesEachNodeOnce(t *testing.T) {
	var stub *preparedStub
	factory := NewDefaultNodeFactory()
	factory.Register("plugin-prepared", func(cfg EmbeddedNodeConfig) (EmbeddedNode, error) {
		stub = &preparedStub{BaseNode: NewBaseNode(cfg)}
		return stub, nil
	})
	_, err := NewSubflowProcessor(SubflowConfig{
		ParentNodeId: "parent",
		Factory:      factory,
		NodeConfigs: []EmbeddedNodeConfig{{
			NodeId: "n1", Label: "n1", PluginType: "plugin-prepared", Action: "Do It", Embeddable: true,
			NodeConfig: NodeConfig{NodeId: "n1", Config: json.RawMessage(`{"label":"x"}`)},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := stub.prepared.Load().(string)
	if got == "" {
		t.Fatal("the node was never prepared")
	}
	var cfg map[string]interface{}
	if err := json.Unmarshal([]byte(got), &cfg); err != nil || cfg["label"] != "x" {
		t.Fatalf("prepared with %q (%v)", got, err)
	}
}
