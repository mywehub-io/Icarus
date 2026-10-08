package jsrunner

import (
	"fmt"

	"github.com/dop251/goja"
)

// Sandbox manages security restrictions for JavaScript execution
type Sandbox struct {
	securityLevel string
	maxStackDepth int
}

// NewSandbox creates a new sandbox with the given configuration
func NewSandbox(config *Config) *Sandbox {
	return &Sandbox{
		securityLevel: config.SecurityLevel,
		maxStackDepth: config.MaxStackDepth,
	}
}

// Apply applies sandbox restrictions to a VM runtime
func (s *Sandbox) Apply(vm *goja.Runtime) error {
	// Remove dangerous globals based on security level
	if err := s.removeDangerousGlobals(vm); err != nil {
		return fmt.Errorf("failed to remove dangerous globals: %w", err)
	}

	// Freeze built-in objects to prevent tampering
	if err := s.freezeBuiltins(vm); err != nil {
		return fmt.Errorf("failed to freeze built-ins: %w", err)
	}

	return nil
}

// removeDangerousGlobals removes or restricts dangerous global objects
func (s *Sandbox) removeDangerousGlobals(vm *goja.Runtime) error {
	dangerousGlobals := []string{
		"require",        // Node.js require
		"module",         // Node.js module
		"exports",        // Node.js exports
		"process",        // Node.js process
		"global",         // Node.js global
		"__dirname",      // Node.js __dirname
		"__filename",     // Node.js __filename
		"Buffer",         // Node.js Buffer
		"setImmediate",   // Node.js setImmediate
		"clearImmediate", // Node.js clearImmediate
	}

	for _, name := range dangerousGlobals {
		if err := vm.Set(name, goja.Undefined()); err != nil {
			return fmt.Errorf("failed to remove %s: %w", name, err)
		}
	}

	// In strict mode, also remove eval
	if s.securityLevel == SecurityLevelStrict {
		if err := s.restrictEval(vm); err != nil {
			return err
		}
	}

	return nil
}

// restrictEval restricts or removes eval functionality
func (s *Sandbox) restrictEval(vm *goja.Runtime) error {
	// Replace eval with a restricted version that throws an error
	restrictedEval := func(call goja.FunctionCall) goja.Value {
		panic(vm.NewGoError(fmt.Errorf("eval is not allowed in strict security mode")))
	}

	return vm.Set("eval", restrictedEval)
}

// freezeBuiltins freezes built-in objects to prevent modification
func (s *Sandbox) freezeBuiltins(vm *goja.Runtime) error {
	// List of built-in objects to freeze
	builtins := []string{
		"Object",
		"Array",
		"Function",
		"String",
		"Number",
		"Boolean",
		"Date",
		"RegExp",
		"Error",
		"Math",
	}

	// Only freeze in standard and strict modes
	if s.securityLevel == SecurityLevelPermissive {
		return nil
	}

	val, err := vm.RunProgram(freezeProgram)
	if err != nil {
		return fmt.Errorf("failed to create freeze function: %w", err)
	}

	freezeFn, ok := goja.AssertFunction(val)
	if !ok {
		return fmt.Errorf("freeze function is not a function")
	}

	for _, name := range builtins {
		obj := vm.Get(name)
		if obj != nil && obj != goja.Undefined() {
			if _, err := freezeFn(goja.Undefined(), obj); err != nil {
				// Non-fatal, log and continue
				continue
			}
		}
	}

	return nil
}

// InjectSecurityAPI injects security-related APIs into the VM
func (s *Sandbox) InjectSecurityAPI(vm *goja.Runtime) error {
	securityObj := vm.NewObject()

	// Add security level information
	securityObj.Set("level", s.securityLevel)

	vm.Set("__security__", securityObj)
	return nil
}

// CreateSecureContext creates a secure execution context
func CreateSecureContext(vm *goja.Runtime, config *Config) error {
	sandbox := NewSandbox(config)

	if err := sandbox.Apply(vm); err != nil {
		return err
	}

	if err := sandbox.InjectSecurityAPI(vm); err != nil {
		return err
	}

	return nil
}

// freezeProgram builds the function the sandbox freezes builtins with. Compiled once per process
// and run on each VM (D20).
var freezeProgram = goja.MustCompile("freeze.js", `
		(function() {
			function freezeObject(obj) {
				if (obj && typeof obj === 'object') {
					Object.freeze(obj);
					if (obj.prototype) {
						Object.freeze(obj.prototype);
					}
				}
			}
			return freezeObject;
		})()
	`, false)
