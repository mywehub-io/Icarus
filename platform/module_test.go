package platform

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	rootModule = "github.com/wehubfusion/Icarus"
	thisModule = rootModule + "/platform"
)

// allowedDirect lists the modules go.mod may require directly. Anything else needs a deliberate
// change here, because every producer inherits this module's graph.
var allowedDirect = map[string]bool{
	"github.com/google/uuid":                    true,
	"github.com/grpc-ecosystem/grpc-gateway/v2": true,
	"github.com/nats-io/nats.go":                true,
	"go.opentelemetry.io/otel":                  true,
	"go.opentelemetry.io/otel/metric":           true,
	"go.opentelemetry.io/otel/trace":            true,
	"go.uber.org/zap":                           true,
	"google.golang.org/genproto/googleapis/api": true,
	"google.golang.org/genproto/googleapis/rpc": true,
	"google.golang.org/grpc":                    true,
	"google.golang.org/protobuf":                true,
	"github.com/nats-io/nats-server/v2":         true, // tests only
	"github.com/stretchr/testify":               true, // tests only
}

// testOnly are modules no package outside tests may depend on.
var testOnly = []string{"github.com/nats-io/nats-server/v2", "github.com/stretchr/testify"}

// testSupport is the one non-test package allowed to import the test-only modules.
const testSupport = thisModule + "/internal/testnats"

func goCmd(t *testing.T, args ...string) string {
	t.Helper()
	gobin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go command not found")
	}
	cmd := exec.Command(gobin, args...)
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=readonly")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

type requirement struct {
	path     string
	indirect bool
}

func readRequirements(t *testing.T) []requirement {
	t.Helper()
	f, err := os.Open("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []requirement
	inBlock := false
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		switch {
		case line == "require (":
			inBlock = true
			continue
		case inBlock && line == ")":
			inBlock = false
			continue
		case strings.HasPrefix(line, "require "):
			line = strings.TrimPrefix(line, "require ")
		case !inBlock:
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		out = append(out, requirement{path: fields[0], indirect: strings.Contains(line, "// indirect")})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// The module must never require the root Icarus module, even in tests: producers adopt it
// without moving their Icarus SDK pin, and customer runners never see it.
func TestNeverRequiresRootModule(t *testing.T) {
	for _, r := range readRequirements(t) {
		if r.path == rootModule {
			t.Fatalf("go.mod requires %s", rootModule)
		}
	}
	for _, line := range strings.Split(goCmd(t, "list", "-m", "all"), "\n") {
		if f := strings.Fields(line); len(f) > 0 && f[0] == rootModule {
			t.Fatalf("the module graph includes %s: %s", rootModule, line)
		}
	}
}

func TestDirectRequirementsAreAllowed(t *testing.T) {
	for _, r := range readRequirements(t) {
		if !r.indirect && !allowedDirect[r.path] {
			t.Errorf("go.mod requires %s directly, which is not on the allowed list", r.path)
		}
	}
}

// Test-only modules stay out of every package a service imports.
func TestTestOnlyModulesStayInTests(t *testing.T) {
	var pkgs []string
	for _, p := range strings.Fields(goCmd(t, "list", "./...")) {
		if p != testSupport {
			pkgs = append(pkgs, p)
		}
	}
	args := append([]string{"list", "-deps", "-f", "{{if .Module}}{{.Module.Path}}{{end}}"}, pkgs...)
	for _, mod := range strings.Fields(goCmd(t, args...)) {
		for _, banned := range testOnly {
			if mod == banned {
				t.Errorf("a non-test package depends on %s", banned)
			}
		}
	}
}

// The library reads no environment variables: every setting is an option the caller passes.
func TestNoEnvironmentReads(t *testing.T) {
	banned := []string{"os.Getenv", "os.LookupEnv", "os.Environ", "syscall.Getenv", "os.ExpandEnv"}
	err := filepath.WalkDir(".", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, s := range banned {
			if strings.Contains(string(b), s) {
				t.Errorf("%s calls %s", path, s)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
