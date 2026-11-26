# Icarus sentryscrub module

`github.com/wehubfusion/Icarus/sentryscrub` keeps message content out of Sentry. Every WeHub
service that reports to Sentry starts it with this module's options:

```go
import "github.com/wehubfusion/Icarus/sentryscrub"

if err := sentry.Init(sentryscrub.ClientOptions(cfg.Sentry.DSN, environment)); err != nil {
	// ...
}
```

It is a nested module, released on its own `sentryscrub/vX.Y.Z` tags. It requires sentry-go and
nothing else, so a service adopts it without moving its Icarus SDK pin.

## Why

Workflow messages (HL7, FHIR, CSV and the rest) carry patient data in real use. Any text a service
builds at run time can hold a piece of one: an error's text quotes the input it failed on, and a
script throws the value it rejected. Before this module, `NewInternalError` sent its message and
its cause's full text to Sentry, and many callers put the cause's text in the message too.

## What reaches Sentry

`Scrub`, the `BeforeSend` hook that `ClientOptions` installs, runs on every event:

| Field | What happens |
| --- | --- |
| Message | Kept only in the internal error shape, rebuilt from the code and the metadata: `MetaData: <metadata>` (when it matches the metadata pattern) and `Code: <code>`. The `Message:` and `Error:` lines that `NewInternalError` wrote in every Icarus version are dropped. Any other message becomes `[message withheld]`. |
| Exceptions | Each keeps its Go type and stack trace. Its text is removed, as are the mechanism's description and data. |
| Request, user, breadcrumbs, extra | Removed. |
| Tags, contexts, fingerprint, level, stack traces | Unchanged. |

`ClientOptions` also attaches a stack trace to every event, because a scrubbed event has no text
to say where it came from. It turns breadcrumbs and default PII off, and drops transactions:
Sentry tracing is not used (traces go to OpenTelemetry), and spans name queries and URLs.

Metadata may hold letters, digits, spaces and `_ . : = , / @ + -`, up to 200 characters. No
quotes, braces, pipes, carets, backslashes or line breaks, so neither HL7 nor JSON passes. A code
is an identifier of up to 64 characters.

## Rules for callers

- Put codes, identifiers and numbers in tags and contexts, never text taken from a message or an
  error. `Scrub` passes them through.
- A service that reports with `CaptureMessage` builds its message with `InternalErrorMessage`.
  Anything else arrives as `[message withheld]`.
- The full error stays in the service's own logs. Find it from the event's time, stack trace,
  code and tags.
