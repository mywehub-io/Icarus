package schema

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

// Many items through one cached schema at once: each gets its own copy of an object default with
// its nested defaults filled, and the cached schema never changes (run with -race).
func TestCachedSchemaIsSharedSafely(t *testing.T) {
	def := []byte(`{"type":"OBJECT","properties":{
		"name":{"type":"STRING"},
		"meta":{"type":"OBJECT","default":{"source":"default"},"properties":{"source":{"type":"STRING"},"level":{"type":"NUMBER","default":1}}}}}`)
	e := NewEngine()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			in, _ := json.Marshal(map[string]interface{}{"name": fmt.Sprintf("n%d", i)})
			res, err := e.ProcessWithSchema(in, def, ProcessOptions{ApplyDefaults: true, StructureData: true})
			if err != nil {
				errs <- err
				return
			}
			var out map[string]interface{}
			_ = json.Unmarshal(res.Data, &out)
			meta, _ := out["meta"].(map[string]interface{})
			if out["name"] != fmt.Sprintf("n%d", i) || meta["source"] != "default" {
				errs <- fmt.Errorf("item %d: %s", i, res.Data)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	e.compiledMu.RLock()
	n := len(e.compiledJSON)
	e.compiledMu.RUnlock()
	if n != 1 {
		t.Fatalf("one definition must compile once, cache holds %d", n)
	}
}
