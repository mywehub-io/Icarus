package jsonops

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/wehubfusion/Icarus/pkg/embedded/runtime"
	"github.com/wehubfusion/Icarus/pkg/fileref"
	schemajson "github.com/wehubfusion/Icarus/pkg/schema/json"
)

// byteDefaults gives a BYTE property that takes its default a file instead of the base64 string the
// schema stores (raw payloads Q4): the saved default stays base64 in Morpheus, and when it is
// applied at run time the node decodes it and writes it as a file, so the property carries a file
// reference like every other byte value. Without a file store the string is kept.
type byteDefaults struct {
	input runtime.ProcessInput

	mu  sync.Mutex
	seq int
	err error
}

func newByteDefaults(input runtime.ProcessInput) *byteDefaults { return &byteDefaults{input: input} }

// hook is the function to give the schema processor, nil when there is nowhere to write a file.
func (b *byteDefaults) hook() schemajson.ByteDefaultFunc {
	if b.input.Files == nil {
		return nil
	}
	return b.write
}

func (b *byteDefaults) write(property, value string) (interface{}, error) {
	raw, err := base64.StdEncoding.DecodeString(value)
	if err != nil {
		if raw, err = base64.URLEncoding.DecodeString(value); err != nil {
			// Not base64: leave the value for the validator to report.
			return value, nil
		}
	}
	b.mu.Lock()
	b.seq++
	port := fmt.Sprintf("default-%s-%d", fileref.SanitizePart(property, "byte"), b.seq)
	b.mu.Unlock()
	ref, werr := b.input.WriteOutputFile(port, fileref.ContentTypeOctetStream, bytes.NewReader(raw))
	if werr != nil {
		b.mu.Lock()
		if b.err == nil {
			b.err = werr
		}
		b.mu.Unlock()
		return value, werr
	}
	return ref, nil
}

// failure is the first error writing a default's file, nil when none failed.
func (b *byteDefaults) failure() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}
