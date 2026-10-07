package fileref

import (
	"mime"
	"path"
	"strings"
)

// Content types of the native formats in decisions D2.
const (
	ContentTypeOctetStream = "application/octet-stream"
	ContentTypeJSON        = "application/json"
	ContentTypeNDJSON      = "application/x-ndjson"
	ContentTypeCSV         = "text/csv; charset=utf-8"
	ContentTypeText        = "text/plain; charset=utf-8"
	ContentTypeXLSX        = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	ContentTypeDuckDB      = "application/vnd.duckdb"
	ContentTypeHL7         = "x-application/hl7-v2+er7"
	ContentTypeESR         = "text/plain"
	ContentTypeFHIRJSON    = "application/fhir+json"
	ContentTypeFHIRXML     = "application/fhir+xml"
	ContentTypeXML         = "application/xml"
	ContentTypeZIP         = "application/zip"
)

// The one table every writer and Athena's signer use. Extension → content type; the first
// extension listed for a content type is the one ExtensionFor returns.
var extTable = []struct {
	ext         string
	contentType string
}{
	{"json", ContentTypeJSON},
	{"ndjson", ContentTypeNDJSON},
	{"csv", ContentTypeCSV},
	{"txt", ContentTypeText},
	{"xlsx", ContentTypeXLSX},
	{"duckdb", ContentTypeDuckDB},
	{"hl7", ContentTypeHL7},
	{"dat", ContentTypeESR},
	{"xml", ContentTypeXML},
	{"zip", ContentTypeZIP},
	{"pdf", "application/pdf"},
	{"html", "text/html; charset=utf-8"},
	{"bin", ContentTypeOctetStream},
}

// Extra content types that map to an extension but are never produced by a writer.
var typeAliases = map[string]string{
	"text/csv":                          "csv",
	"application/csv":                   "csv",
	"text/plain":                        "txt",
	"text/json":                         "json",
	"application/fhir+json":             "json",
	"application/fhir+xml":              "xml",
	"text/xml":                          "xml",
	"application/ndjson":                "ndjson",
	"application/jsonl":                 "ndjson",
	"application/jsonlines":             "ndjson",
	"application/hl7-v2":                "hl7",
	"x-application/hl7-v2+er7":          "hl7",
	"application/hl7-v2+er7":            "hl7",
	"application/x-hl7":                 "hl7",
	"application/vnd.ms-excel":          "xls",
	"application/x-www-form-urlencoded": "txt",
}

// BaseContentType strips parameters and lower-cases: "Text/CSV; charset=utf-8" → "text/csv".
func BaseContentType(ct string) string {
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = ct[:i]
	}
	return strings.ToLower(strings.TrimSpace(ct))
}

// ExtensionFor returns the extension, without the dot, for a content type; "bin" when unknown.
func ExtensionFor(contentType string) string {
	base := BaseContentType(contentType)
	if base == "" {
		return "bin"
	}
	for _, e := range extTable {
		if BaseContentType(e.contentType) == base {
			return e.ext
		}
	}
	if ext, ok := typeAliases[base]; ok {
		return ext
	}
	if strings.HasSuffix(base, "+json") {
		return "json"
	}
	if strings.HasSuffix(base, "+xml") {
		return "xml"
	}
	if exts, err := mime.ExtensionsByType(base); err == nil && len(exts) > 0 {
		return strings.TrimPrefix(exts[0], ".")
	}
	return "bin"
}

// ContentTypeFor returns the content type for an extension (with or without the dot) or a file
// name; application/octet-stream when unknown.
func ContentTypeFor(extOrName string) string {
	ext := extOrName
	if strings.Contains(ext, ".") {
		ext = path.Ext(ext)
	}
	ext = strings.ToLower(strings.TrimPrefix(ext, "."))
	if ext == "" {
		return ContentTypeOctetStream
	}
	for _, e := range extTable {
		if e.ext == ext {
			return e.contentType
		}
	}
	if ct := mime.TypeByExtension("." + ext); ct != "" {
		return ct
	}
	return ContentTypeOctetStream
}
