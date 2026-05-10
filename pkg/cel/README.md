# pkg/cel

Compile-once / evaluate-many CEL expression engine for custom validation rules.

Used internally by the HL7 schema processor and available as a standalone building block
for services that need rule evaluation.

## Core types

```go
type InputRule struct {
    ID, Name     string // machine ID and human label
    When, Assert string // CEL expressions; When is optional guard, Assert must return bool
    Message      string // violation message template
    ErrorPath    string // JSON path for the violation
    Severity     string // "ERROR" | "WARNING" | "INFO"
}

type CompiledRule struct {
    Rule      InputRule
    WhenAst   *cel.Ast
    AssertAst *cel.Ast
}
```

## `Engine`

```go
func NewEngine(opts ...cel.EnvOption) (*Engine, error)
```

Creates a CEL environment from standard `cel-go` options (variable declarations, function
bindings, standard library). (`pkg/cel/engine.go:19`)

### `Compile`

```go
func (e *Engine) Compile(rules []InputRule) ([]CompiledRule, error)
```

Type-checks all `When` and `Assert` expressions. `Assert` must return `bool`. Returns the
first compilation error encountered. Call once per schema load; reuse `CompiledRule` slices
across evaluations.

## `ScopeIterator`

```go
type ScopeIterator interface {
    IterationCount(rule InputRule) int
    EnvOptionsAt(rule InputRule, index int) []cel.EnvOption
    ErrorPath(rule InputRule, index int) string
}
```

Supplies per-instance bindings for rules that iterate over repeated structures (e.g. HL7
segments). The implementor infers from the rule's expression text how many instances exist
and binds the relevant runtime functions for each.

Returning `0` from `IterationCount` skips the rule entirely (e.g. the target segment is absent).

## Result types

```go
type Violation struct {
    RuleID, RuleName, Path, Message, Severity string
}

type EvalError struct {
    RuleID, RuleName, Expr string // Expr is "bind", "when", or "assert"
    Err          error
    Path         string
    RuleSeverity string
}
```

`EvalError` carries the rule severity so callers can bucket runtime errors (failed to evaluate
a guard) consistently with static violations.

## See also

- `pkg/schema/hl7/` — uses this engine for HL7 custom validation rules
