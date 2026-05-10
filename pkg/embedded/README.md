# pkg/embedded

Embedded node processing system for Elysium execution units. Orchestrates concurrent
execution of a group of nodes (parent + embedded children) within a single JetStream
message processing cycle.

## Package alias

The package is named `embeddedv2` in Go source. Import it as:

```go
import embeddedv2 "github.com/wehubfusion/Icarus/pkg/embedded"
```

## Core types (re-exported from `runtime`)

| Type | Description |
|---|---|
| `EmbeddedNode` | Interface every embedded node processor must implement: `Process(ProcessInput) ProcessOutput`, `NodeId() string`, `PluginType() string` |
| `EmbeddedNodeFactory` | Registry of `NodeCreator` functions; creates nodes from `EmbeddedNodeConfig` |
| `NodeCreator` | `func(EmbeddedNodeConfig) (EmbeddedNode, error)` — factory function for a plugin type |
| `ProcessInput` | All data a node needs: resolved inputs, field mappings, workflow context, blob client |
| `ProcessOutput` | Node result: output data, error, metadata |
| `ExecutionUnit` | Group of nodes in a single execution (parent + embedded children) |
| `EmbeddedNodeConfig` | `NodeID`, `PluginType`, `Configuration` (raw JSON), and context IDs |
| `StandardUnitOutput` | Flattened result of a complete execution unit |

## `Processor` (internal `runtime.Processor`)

The runtime processor drives the execution unit:

1. Resolves inputs using the provided resolver.
2. Creates each node via the factory.
3. Dispatches nodes through a worker pool.
4. Collects outputs and reports back.

Configure via `ProcessorConfig` and `WorkerPoolConfig`:

```go
cfg := runtime.ProcessorConfig{
    WorkerPool: runtime.WorkerPoolConfig{
        WorkerCount: 4,
        QueueSize:   16,
    },
}
```

## Lifecycle observer

`runtime.ArgusLifecycleEmitter` emits Argus `node.started` and `node.ended` events for
each embedded node. Inject it via `ProcessorConfig.LifecycleEmitter` to enable observation
without coupling the embedded runtime to Argus directly.
(`pkg/embedded/runtime/argus_lifecycle_emitter.go`)

## Error types

`NodeFailureError` wraps a node processing error with the node ID, plugin type, and whether
the failure is permanent (no retry) or transient. (`pkg/embedded/runtime/node_failure_error.go`)

## See also

- `pkg/runner/` — runner that receives the JetStream message and calls the embedded processor
- `pkg/resolver/` — field mapping and blob input resolution used by `ProcessInput`
