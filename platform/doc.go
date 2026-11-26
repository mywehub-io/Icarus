// Package platform is the root of the WeHub platform packages: the shared library WeHub services
// use to publish audit events and to create and update Iris jobs.
//
// The packages:
//
//   - audit: the emitter that publishes audit events to a JetStream stream, and the
//     request-scoped collector that handlers fill in.
//   - audit/grpcaudit: the gRPC server interceptor that records one event per call, plus the
//     gRPC-Gateway helpers it needs.
//   - audit/httpaudit: the same rules as net/http middleware.
//   - jobs: the Iris client that creates a job with its lease and publishes its updates.
//
// Two rules hold for every platform package. It imports nothing else from Icarus, even in tests,
// so a service links only what it uses. And it reads no environment variables: every setting is
// an option the service passes in from its own configuration. Tests in this package enforce both.
package platform
