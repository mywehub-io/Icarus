package fileref

import "encoding/json"

// A {files:[...]} list (D2) is how a multipart HTTP trigger, and later any node taking several
// files, carries them: each item is a reference plus the form key it arrived under,
//
//	{"$file": {"path": "...", "size": 12, ...}, "key": "upload"}
//
// The item is not itself a reference (Parse accepts the single-key form only), so a list item is
// never mistaken for a file to open by a reader that expects one: read it with ParseFilesItem.

// FilesItemKey is the JSON key holding a list item's form key.
const FilesItemKey = "key"

// ContentTypeFilesManifest marks a file holding a {files:[...]} value with its form fields: the
// trigger value of a multipart request, written by Artemis next to the parts it lists.
const ContentTypeFilesManifest = "application/vnd.wehub.files+json"

// FilesItem returns one list item: the reference's value form plus its form key.
func FilesItem(ref FileRef, key string) map[string]interface{} {
	item := Value(ref)
	if key != "" {
		item[FilesItemKey] = key
	}
	return item
}

// ParseFilesItem reads one list item. It applies Parse's rules to the reference part and does not
// say the file may be opened: see InRun.
func ParseFilesItem(v interface{}) (FileRef, string, bool) {
	m, ok := v.(map[string]interface{})
	if !ok {
		if raw, isRaw := v.(json.RawMessage); isRaw {
			var decoded interface{}
			if err := json.Unmarshal(raw, &decoded); err != nil {
				return FileRef{}, "", false
			}
			return ParseFilesItem(decoded)
		}
		return FileRef{}, "", false
	}
	key := ""
	if k, present := m[FilesItemKey]; present {
		s, isString := k.(string)
		if !isString {
			return FileRef{}, "", false
		}
		key = s
	}
	for k := range m {
		if k != Key && k != FilesItemKey {
			return FileRef{}, "", false
		}
	}
	ref, ok := Parse(map[string]interface{}{Key: m[Key]})
	return ref, key, ok
}
