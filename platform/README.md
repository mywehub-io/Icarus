# Icarus platform module

`github.com/wehubfusion/Icarus/platform` is the shared library WeHub services use to:

- publish audit events to a NATS JetStream stream, one event per API call; and
- create Iris jobs with their resource lease, and publish the jobs' updates.

It is a nested module, versioned on its own. It is separate from the root Icarus module (the
runner SDK), so a service can adopt it without moving its Icarus SDK pin, and customer runners
never see it in their module graph.

## Packages

| Package | What it does |
| --- | --- |
| `audit` | `Emitter`: builds, limits and publishes audit events, with a bounded retry queue, producer heartbeats and OTel metrics; `Publish` returns the stream's answer for a caller with its own outbox. `Collector`: the request-scoped record handlers fill with `RecordDecision`, `SetJobID`, `SetResource`, `SetAttr` and `SetReason`. |
| `audit/auditpb` | Go code for the audit event proto (`proto/audit/v1/audit.proto`). |
| `audit/grpcaudit` | `UnaryServerInterceptor`, the hook at the gRPC API layer, plus `EntryMarker` and `HeaderMatcher` for the gRPC-Gateway and `Classify`. |
| `audit/grpcaudit/grpcaudittest` | `AssertAllClassified`, for a test in each service that fails when an RPC is unclassified. |
| `audit/httpaudit` | The same rules as `net/http` middleware, for a service that does not serve gRPC. |
| `jobs` | `Coordinator`: creates a job and its lease in Iris (`Begin`), then publishes running, progress and terminal updates (`Op`), with stateless `PublishStatus` and `Renew` for Temporal activities. |
| `jobs/iriswire` | Go code for a copy of the Iris messages the client needs (`proto/iriswire/v1/iriswire.proto`), under a separate proto package so it never collides with a service's own generated Iris code. |

## What gets audited

The interceptor builds at most one event per call, after the handler returns:

- A call refused with `PermissionDenied` or `Unauthenticated` (HTTP 401 or 403) is always
  recorded, at any hop, as a `DENIAL`. It goes to the organisation's chain only when the caller
  named a valid organisation id and at least one authorisation check granted something;
  otherwise it goes to the platform chain with the claimed id in `attrs`, at most once a minute
  per claimed organisation, source address and method.
- Any other failed call follows the same rule, so a forged `X-Client-Id` on a call that fails
  before any check can neither land in nor flood that organisation's chain. A call that succeeded
  is recorded against the organisation it named.
- A write at an entry is recorded as a `WRITE`, waiting for the stream's acknowledgement.
- A sensitive read at an entry is recorded as a `SENSITIVE_READ`, asynchronously.
- Anything else, including a write one service makes on another, is not recorded.

A method is a read when its `google.api.http` binding is GET, and a write for any other binding
or none, unless the service's override table says otherwise. A call is an entry when it carries
this process's gateway marker (`EntryMarker`) or when the last `x-forwarded-client-cert` element
names one of the configured entry principals.

Put the interceptor first in the chain so it sees the authentication interceptor's refusals, and
put `grpcaudit.IdentityRecorder` straight after the authentication interceptor: its callback runs
on the context authentication built, so it can use the service's own auth getters, and it records
the caller in the request's collector. `Config.Identity` is only the fallback for calls where
nothing was recorded (authentication refused them), so it may read only the incoming metadata. An
HTTP service records the caller from its own auth middleware with `audit.SetIdentity`. The gateway must
reach the gRPC server through a client connection, not `Register...HandlerServer`, or no
interceptor runs for REST calls.

## Rules

Tests in this module enforce each of these.

- **Never require the root module.** `go.mod` must not require `github.com/wehubfusion/Icarus`,
  not even for tests. The job contract constants are copied into `jobs`, with golden tests.
- **No environment variables.** The library reads none: every setting (stream and subject names,
  timeouts, queue sizes, entry principals) is an option the service passes from its own config.
- **A short dependency list.** `go.mod` may require directly only the modules listed in
  `module_test.go`; `nats-server` and `testify` stay in tests.
- **Nothing sensitive.** The module is public through the Go module proxy, so it holds no
  secrets, hostnames or environment values. Services pass their own service token through
  `jobs.TokenSource`.
- **No stream ownership.** Neither the audit emitter nor the jobs coordinator creates or updates
  a stream. A missing stream is a publish failure, counted and logged.

## Generated code

The protos repo is the source of truth for both proto files here:

- `proto/audit/v1/audit.proto` is a verbatim copy of `services/audit/v1/audit.proto`.
- `proto/iriswire/v1/iriswire.proto` copies the Iris messages from `services/iris/iris.proto` and
  `services/iris/events.proto` with identical field names, numbers and types. A test in `jobs`
  checks every field against a table copied from those files.

To regenerate after changing either file (needs `protoc` and `protoc-gen-go` on `PATH`):

```sh
make proto
```

Commit the generated code with the proto change.

## Development

```sh
go test ./...
go vet ./...
make fmt   # fails if gofmt would change anything
```

Tests run an embedded NATS server with JetStream and an in-memory gRPC server; they need no
external services.

## Releases

Tag a release as `platform/vX.Y.Z` on the commit to release, for example `platform/v0.1.0`. The
`platform/` prefix is what Go uses to find a nested module's versions. Record each release in
`CHANGELOG.md`.
