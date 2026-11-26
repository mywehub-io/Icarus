# Changelog

All notable changes to the sentryscrub module (`github.com/wehubfusion/Icarus/sentryscrub`) are
documented here. Releases are tagged `sentryscrub/vX.Y.Z`.

## [Unreleased]

### First version

- `ClientOptions(dsn, environment)`: the options a service passes to `sentry.Init`. Stack traces
  on every event, breadcrumbs and default PII off, transactions dropped, and `Scrub` as
  `BeforeSend`.
- `Scrub`: rebuilds an event's message from the internal error code and metadata (or replaces it
  with `Withheld`), removes exception text, and removes the request, user, breadcrumbs and extra
  data. Tags, contexts, the fingerprint and stack traces pass through.
- `InternalErrorMessage(metaData, code)`: the one message shape `Scrub` keeps, for services that
  report with `CaptureMessage`.
