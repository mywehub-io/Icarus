package sentryscrub

import (
	"os"
	"strings"
	"testing"
)

// TestRequiresOnlySentry holds the module to sentry-go. Every Olympus service adds this module,
// so anything it requires lands in all of their module graphs, and requiring the root Icarus
// module would move the services' Icarus SDK pins.
func TestRequiresOnlySentry(t *testing.T) {
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("read go.mod: %v", err)
	}
	inRequireBlock := false
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		switch {
		case line == "require (":
			inRequireBlock = true
			continue
		case inRequireBlock && line == ")":
			inRequireBlock = false
			continue
		case strings.HasPrefix(line, "require "):
			line = strings.TrimPrefix(line, "require ")
		case !inRequireBlock:
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		path, indirect := fields[0], strings.Contains(line, "// indirect")
		if path == "github.com/wehubfusion/Icarus" {
			t.Fatalf("go.mod requires the root Icarus module")
		}
		if !indirect && path != "github.com/getsentry/sentry-go" {
			t.Errorf("go.mod requires %s directly; only github.com/getsentry/sentry-go is allowed", path)
		}
	}
}
