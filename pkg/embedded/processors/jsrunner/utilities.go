package jsrunner

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dop251/goja"
)

// Utility defines the interface for JavaScript utilities
type Utility interface {
	// Name returns the unique name of the utility
	Name() string

	// Register registers the utility in the VM runtime
	Register(vm *goja.Runtime) error

	// AllowedSecurityLevels returns the security levels that allow this utility
	AllowedSecurityLevels() []string

	// Cleanup is called when the VM is being cleaned up
	Cleanup(vm *goja.Runtime) error
}

// UtilityRegistry manages available JavaScript utilities
type UtilityRegistry struct {
	utilities map[string]Utility
	mu        sync.RWMutex
}

// NewUtilityRegistry creates a new utility registry with built-in utilities
func NewUtilityRegistry() *UtilityRegistry {
	registry := &UtilityRegistry{
		utilities: make(map[string]Utility),
	}

	// Register built-in utilities
	registry.Register(&ConsoleUtility{maxLogs: 1000, maxErrors: 1000})
	registry.Register(&JSONUtility{})
	registry.Register(&EncodingUtility{})
	registry.Register(&TimersUtility{timers: make(map[int]*time.Timer)})

	return registry
}

// Register adds a utility to the registry
func (r *UtilityRegistry) Register(utility Utility) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.utilities[utility.Name()] = utility
}

// RegisterEnabled registers all enabled utilities in the VM
func (r *UtilityRegistry) RegisterEnabled(vm *goja.Runtime, config *Config) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, utilityName := range config.EnabledUtilities {
		utility, ok := r.utilities[utilityName]
		if !ok {
			continue // Skip unknown utilities
		}

		// Check if utility is allowed at this security level
		if !r.isAllowedAtLevel(utility, config.SecurityLevel) {
			continue
		}

		if err := utility.Register(vm); err != nil {
			return fmt.Errorf("failed to register utility %s: %w", utilityName, err)
		}
	}

	return nil
}

// CleanupEnabled calls cleanup on all enabled utilities
func (r *UtilityRegistry) CleanupEnabled(vm *goja.Runtime, config *Config) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, utilityName := range config.EnabledUtilities {
		utility, ok := r.utilities[utilityName]
		if !ok {
			continue
		}

		if err := utility.Cleanup(vm); err != nil {
			return fmt.Errorf("failed to cleanup utility %s: %w", utilityName, err)
		}
	}

	return nil
}

// RunDueTimers runs the callbacks of fired timers on the caller's goroutine (see TimersUtility).
func (r *UtilityRegistry) RunDueTimers() {
	r.mu.RLock()
	t, _ := r.utilities["timers"].(*TimersUtility)
	r.mu.RUnlock()
	if t != nil {
		t.RunDue()
	}
}

// isAllowedAtLevel checks if a utility is allowed at the given security level
func (r *UtilityRegistry) isAllowedAtLevel(utility Utility, securityLevel string) bool {
	allowedLevels := utility.AllowedSecurityLevels()
	for _, level := range allowedLevels {
		if level == securityLevel {
			return true
		}
	}
	return false
}

// ConsoleUtility provides console.log, console.error, etc.
type ConsoleUtility struct {
	logs      []string
	errors    []string
	mu        sync.Mutex
	maxLogs   int
	maxErrors int
}

func (u *ConsoleUtility) Name() string { return "console" }

func (u *ConsoleUtility) AllowedSecurityLevels() []string {
	return []string{SecurityLevelStandard, SecurityLevelPermissive}
}

func (u *ConsoleUtility) Register(vm *goja.Runtime) error {
	console := vm.NewObject()

	// console.log
	console.Set("log", func(call goja.FunctionCall) goja.Value {
		args := make([]interface{}, len(call.Arguments))
		for i, arg := range call.Arguments {
			args[i] = arg.Export()
		}
		u.mu.Lock()
		u.logs = append(u.logs, fmt.Sprint(args...))
		if len(u.logs) > u.maxLogs {
			u.logs = u.logs[len(u.logs)-u.maxLogs:]
		}
		u.mu.Unlock()
		return goja.Undefined()
	})

	// console.error
	console.Set("error", func(call goja.FunctionCall) goja.Value {
		args := make([]interface{}, len(call.Arguments))
		for i, arg := range call.Arguments {
			args[i] = arg.Export()
		}
		u.mu.Lock()
		u.errors = append(u.errors, fmt.Sprint(args...))
		if len(u.errors) > u.maxErrors {
			u.errors = u.errors[len(u.errors)-u.maxErrors:]
		}
		u.mu.Unlock()
		return goja.Undefined()
	})

	// console.warn (alias to error)
	console.Set("warn", console.Get("error"))

	// console.info (alias to log)
	console.Set("info", console.Get("log"))

	vm.Set("console", console)
	return nil
}

func (u *ConsoleUtility) Cleanup(vm *goja.Runtime) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.logs = nil
	u.errors = nil
	return nil
}

// JSONUtility provides JSON.parse and JSON.stringify
type JSONUtility struct{}

func (u *JSONUtility) Name() string { return "json" }

func (u *JSONUtility) AllowedSecurityLevels() []string {
	return []string{SecurityLevelStrict, SecurityLevelStandard, SecurityLevelPermissive}
}

func (u *JSONUtility) Register(vm *goja.Runtime) error {
	jsonObj := vm.NewObject()

	// JSON.parse
	jsonObj.Set("parse", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			panic(vm.NewTypeError("JSON.parse requires an argument"))
		}

		str := call.Argument(0).String()
		var result interface{}
		if err := json.Unmarshal([]byte(str), &result); err != nil {
			panic(vm.NewGoError(fmt.Errorf("JSON.parse error: %w", err)))
		}

		return vm.ToValue(result)
	})

	// JSON.stringify
	jsonObj.Set("stringify", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			panic(vm.NewTypeError("JSON.stringify requires an argument"))
		}

		val := call.Argument(0).Export()
		bytes, err := json.Marshal(val)
		if err != nil {
			panic(vm.NewGoError(fmt.Errorf("JSON.stringify error: %w", err)))
		}

		return vm.ToValue(string(bytes))
	})

	vm.Set("JSON", jsonObj)
	return nil
}

func (u *JSONUtility) Cleanup(vm *goja.Runtime) error {
	return nil
}

// EncodingUtility provides btoa, atob for base64 encoding
type EncodingUtility struct{}

func (u *EncodingUtility) Name() string { return "encoding" }

func (u *EncodingUtility) AllowedSecurityLevels() []string {
	return []string{SecurityLevelStandard, SecurityLevelPermissive}
}

func (u *EncodingUtility) Register(vm *goja.Runtime) error {
	// btoa - encode to base64
	vm.Set("btoa", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			panic(vm.NewTypeError("btoa requires an argument"))
		}

		str := call.Argument(0).String()
		encoded := base64.StdEncoding.EncodeToString([]byte(str))
		return vm.ToValue(encoded)
	})

	// atob - decode from base64
	vm.Set("atob", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			panic(vm.NewTypeError("atob requires an argument"))
		}

		str := call.Argument(0).String()
		decoded, err := base64.StdEncoding.DecodeString(str)
		if err != nil {
			panic(vm.NewGoError(fmt.Errorf("atob error: %w", err)))
		}

		return vm.ToValue(string(decoded))
	})

	return nil
}

func (u *EncodingUtility) Cleanup(vm *goja.Runtime) error {
	return nil
}

// TimersUtility provides setTimeout, clearTimeout, setInterval, clearInterval.
//
// A goja runtime is not safe for concurrent use, so a timer never calls into the VM from its own
// goroutine. When it fires it queues the callback, and RunDue runs queued callbacks on the VM's
// goroutine once the script's synchronous part has finished, as a JavaScript event loop would.
// A timer still pending when the script's result is taken never runs.
type TimersUtility struct {
	timers map[int]*time.Timer
	nextID int
	mu     sync.Mutex
	vmRef  *goja.Runtime
	due    []goja.Callable
}

// RunDue runs the callbacks of timers that have fired, in firing order, on the caller's goroutine,
// which must be the one running the VM. Callbacks that fire while it runs are run too, up to a
// bound, so a callback that schedules another cannot loop forever.
func (u *TimersUtility) RunDue() {
	const maxCallbacks = 1000
	for ran := 0; ran < maxCallbacks; ran++ {
		u.mu.Lock()
		if len(u.due) == 0 {
			u.mu.Unlock()
			return
		}
		cb := u.due[0]
		u.due = u.due[1:]
		u.mu.Unlock()
		_, _ = cb(goja.Undefined())
	}
}

func (u *TimersUtility) Name() string { return "timers" }

func (u *TimersUtility) AllowedSecurityLevels() []string {
	return []string{SecurityLevelPermissive}
}

func (u *TimersUtility) Register(vm *goja.Runtime) error {
	u.mu.Lock()
	u.vmRef = vm
	u.mu.Unlock()

	// setTimeout
	vm.Set("setTimeout", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) < 2 {
			panic(vm.NewTypeError("setTimeout requires at least 2 arguments"))
		}

		callback, ok := goja.AssertFunction(call.Argument(0))
		if !ok {
			panic(vm.NewTypeError("first argument must be a function"))
		}

		delay := call.Argument(1).ToInteger()

		u.mu.Lock()
		id := u.nextID
		u.nextID++
		currentVM := u.vmRef

		timer := time.AfterFunc(time.Duration(delay)*time.Millisecond, func() {
			u.mu.Lock()
			defer u.mu.Unlock()
			delete(u.timers, id)
			if u.vmRef == currentVM {
				u.due = append(u.due, callback)
			}
		})

		u.timers[id] = timer
		u.mu.Unlock()

		return vm.ToValue(id)
	})

	// clearTimeout
	vm.Set("clearTimeout", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			return goja.Undefined()
		}

		id := int(call.Argument(0).ToInteger())

		u.mu.Lock()
		if timer, ok := u.timers[id]; ok {
			timer.Stop()
			delete(u.timers, id)
		}
		u.mu.Unlock()

		return goja.Undefined()
	})

	// setInterval - simplified (same as setTimeout)
	vm.Set("setInterval", vm.Get("setTimeout"))

	// clearInterval - same as clearTimeout
	vm.Set("clearInterval", vm.Get("clearTimeout"))

	return nil
}

func (u *TimersUtility) Cleanup(vm *goja.Runtime) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	// Stop and clear all timers
	for _, timer := range u.timers {
		timer.Stop()
	}
	u.timers = make(map[int]*time.Timer)
	u.nextID = 0
	u.vmRef = nil
	u.due = nil

	return nil
}
