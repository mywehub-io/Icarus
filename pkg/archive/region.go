package archive

import (
	"encoding/binary"
	"io"
)

// eocdLen is the size of a ZIP end-of-central-directory record with no comment.
const eocdLen = 22

// EntryRegionEnd returns the offset where an archive's entries end and its central
// directory begins, read from the end-of-central-directory record in the last 22 bytes.
//
// Every byte an entry read needs, local headers, values and data descriptors, lies in
// [0, EntryRegionEnd), so a caller about to read most entries can fetch that span once.
//
// It reports false when the record is not where a comment-free, non-ZIP64 archive puts
// it. The writers in this package never set a comment, and an archive needs ZIP64 only
// past 65,535 entries or 4 GiB. A caller treats false as "unknown" and fetches the whole
// blob, which is always correct.
func EntryRegionEnd(r io.ReaderAt, size int64) (int64, bool) {
	if size < eocdLen {
		return 0, false
	}
	var rec [eocdLen]byte
	if _, err := r.ReadAt(rec[:], size-eocdLen); err != nil && err != io.EOF {
		return 0, false
	}
	if binary.LittleEndian.Uint32(rec[0:4]) != 0x06054b50 {
		return 0, false
	}
	if binary.LittleEndian.Uint16(rec[20:22]) != 0 {
		return 0, false // a comment: this is not the record a comment-free archive ends with
	}
	cdSize := int64(binary.LittleEndian.Uint32(rec[12:16]))
	cdOffset := int64(binary.LittleEndian.Uint32(rec[16:20]))
	if cdOffset == 0xFFFFFFFF || cdSize == 0xFFFFFFFF {
		return 0, false // ZIP64: the real values live in another record
	}
	if cdOffset+cdSize+eocdLen != size {
		return 0, false
	}
	return cdOffset, true
}
