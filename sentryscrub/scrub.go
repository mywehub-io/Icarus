// Package sentryscrub keeps message content out of Sentry.
//
// Workflow messages (HL7, FHIR, CSV and the rest) carry patient data in real use, and any text a
// service builds at run time can hold a piece of one: an error's text quotes the input it failed
// on, and a script throws the value it rejected. So no such text leaves a service for Sentry.
// Scrub, the BeforeSend hook that ClientOptions installs, keeps only what code writes:
//
//   - A message survives only in the shape the Icarus errors package uses for an internal error,
//     rebuilt from its code and metadata. The "Message:" and "Error:" lines that NewInternalError
//     wrote before this module existed are dropped, whatever Icarus version a service pins. Any
//     other message becomes Withheld.
//   - Every exception keeps its Go type and stack trace and loses its text.
//   - The request, the user, breadcrumbs and extra data are removed.
//
// Tags, contexts, the fingerprint, the level and stack traces pass through unchanged. Code that
// sets tags or contexts must use codes, identifiers and numbers only, never text taken from a
// message or an error.
//
// The module requires sentry-go and nothing else, never the root Icarus module, so a service
// adopts it without moving its Icarus SDK pin.
package sentryscrub

import (
	"regexp"
	"strings"

	"github.com/getsentry/sentry-go"
)

// Withheld replaces an event message that is not in the internal error shape.
const Withheld = "[message withheld]"

const (
	metaDataPrefix = "MetaData: "
	codePrefix     = "Code: "
)

var (
	// codePattern is an error code: an identifier such as SCHEMA_FETCH_ERROR. Empty is allowed,
	// because some callers put their code in the metadata and pass an empty code.
	codePattern = regexp.MustCompile(`^[A-Za-z0-9_.:-]{0,64}$`)

	// metaDataPattern is what metadata may hold: identifiers, key=value pairs, names and paths.
	// No quotes, braces, pipes, carets, backslashes or line breaks, so neither HL7 nor JSON
	// passes. Callers put codes and ids here; this is the check behind that rule.
	metaDataPattern = regexp.MustCompile(`^[A-Za-z0-9_.:=,/@+ -]{1,200}$`)

	// legacyBody is the rest of a message NewInternalError wrote before this module:
	// "Message: <text>\nCode: <code>\nError: <cause>". The text and the cause are free text; only
	// the code is kept. The earliest code line that an error line follows is the one
	// NewInternalError wrote, and the code pattern bounds anything else it could match.
	legacyBody = regexp.MustCompile(`(?s)^Message: .*?\n` + codePrefix + `([A-Za-z0-9_.:-]{0,64})\nError: `)
)

// ClientOptions returns the options a WeHub service passes to sentry.Init. Every event carries a
// stack trace, because a scrubbed event has no text to say where it came from. Breadcrumbs,
// default PII and transactions are off, and Scrub runs on every event before it is sent.
//
// Sentry tracing is not used (traces go to OpenTelemetry). A transaction's spans name queries and
// URLs, so BeforeSendTransaction drops them.
func ClientOptions(dsn, environment string) sentry.ClientOptions {
	return sentry.ClientOptions{
		Dsn:                   dsn,
		Environment:           environment,
		AttachStacktrace:      true,
		SendDefaultPII:        false,
		MaxBreadcrumbs:        -1,
		BeforeSend:            Scrub,
		BeforeSendTransaction: dropTransaction,
	}
}

// InternalErrorMessage is the message an internal error is reported with: its metadata, when that
// matches the metadata pattern, and its code. Scrub keeps a message in this shape and nothing
// else, so a service that reports with CaptureMessage builds its message here.
func InternalErrorMessage(metaData, code string) string {
	if !codePattern.MatchString(code) {
		return Withheld
	}
	if metaDataPattern.MatchString(metaData) {
		return metaDataPrefix + metaData + "\n" + codePrefix + code
	}
	return codePrefix + code
}

// Scrub is a sentry BeforeSend hook. It returns the event with every field that can hold message
// content removed or rebuilt, as the package documentation describes. It never drops an event.
func Scrub(event *sentry.Event, _ *sentry.EventHint) *sentry.Event {
	if event == nil {
		return nil
	}
	event.Message = scrubMessage(event.Message)
	for i := range event.Exception {
		event.Exception[i].Value = ""
		if m := event.Exception[i].Mechanism; m != nil {
			m.Description = ""
			m.Data = nil
		}
	}
	event.Request = nil
	event.User = sentry.User{}
	event.Breadcrumbs = nil
	event.Extra = nil
	return event
}

func dropTransaction(*sentry.Event, *sentry.EventHint) *sentry.Event {
	return nil
}

// scrubMessage rebuilds an internal error message from its code and metadata. It accepts the
// shape InternalErrorMessage writes and the one NewInternalError wrote before this module.
func scrubMessage(message string) string {
	if message == "" {
		return ""
	}
	metaData, rest := "", message
	if strings.HasPrefix(rest, metaDataPrefix) {
		line, after, ok := strings.Cut(rest, "\n")
		if !ok {
			return Withheld
		}
		metaData, rest = strings.TrimPrefix(line, metaDataPrefix), after
	}
	if code, ok := strings.CutPrefix(rest, codePrefix); ok && !strings.Contains(code, "\n") {
		return InternalErrorMessage(metaData, code)
	}
	if m := legacyBody.FindStringSubmatch(rest); m != nil {
		return InternalErrorMessage(metaData, m[1])
	}
	return Withheld
}
