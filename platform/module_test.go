package platform

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	rootModule = "github.com/wehubfusion/Icarus"
	thisTree   = rootModule + "/platform"
)

// allowedModules lists the modules the platform packages may import from. Anything else needs a
// deliberate change here, because every producer links what these packages import. The rest of
// the Icarus SDK is not on the list: the platform packages keep their own copies (jobs/contract.go)
// rather than import it.
var allowedModules = map[string]bool{
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
const testSupport = thisTree + "/internal/testnats"

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

// Every import of a platform package, tests included, is the standard library, another platform
// package, or a package of an allowed module.
func TestImportsOnlyAllowedModules(t *testing.T) {
	imports := map[string]bool{}
	for _, line := range strings.Split(goCmd(t, "list", "-f",
		"{{join .Imports \"\\n\"}}\n{{join .TestImports \"\\n\"}}\n{{join .XTestImports \"\\n\"}}", "./..."), "\n") {
		if p := strings.TrimSpace(line); p != "" && p != thisTree && !strings.HasPrefix(p, thisTree+"/") {
			imports[p] = true
		}
	}
	if len(imports) == 0 {
		t.Fatal("go list found no imports")
	}
	var paths []string
	for p := range imports {
		paths = append(paths, p)
	}
	args := append([]string{"list", "-f", "{{.ImportPath}} {{.Standard}} {{if .Module}}{{.Module.Path}}{{end}}"}, paths...)
	for _, line := range strings.Split(strings.TrimSpace(goCmd(t, args...)), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 || f[1] == "true" {
			continue
		}
		if len(f) < 3 || !allowedModules[f[2]] {
			t.Errorf("a platform package imports %s, which is not in an allowed module", f[0])
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
