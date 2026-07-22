# pkg/jobs

Shared helpers for unified async job identifiers and NATS envelope constants.

Producers mint `JOB-<uuid>` locally via `NewJobID()` and publish updates on JetStream
(`jobs.update`). Iris projects status into Postgres (lazy create on seq=1 bootstrap).

This package does **not** own job state, idempotency storage, or projection logic.

```go
import "github.com/wehubfusion/Icarus/pkg/jobs"

jobID := jobs.NewJobID()
msgID := jobs.MessageID(jobID, 1)
```

Wire shapes are defined in `protos/services/iris/events.proto`.
