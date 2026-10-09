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

// FilesKey is the key holding a files list: {"files": [item, ...]}.
const FilesKey = "files"

// FilesList returns a {files:[...]} value holding refs, each keyed by its file name.
func FilesList(refs []FileRef) map[string]interface{} {
	items := make([]interface{}, len(refs))
	for i, r := range refs {
		items[i] = FilesItem(r, r.FileName)
	}
	return map[string]interface{}{FilesKey: items}
}

// ParseFilesList reads a {files:[...]} value: at least one item, every item a list item
// (ParseFilesItem). Other keys beside "files" (form fields) are ignored. It does not say the files
// may be opened: see InRun.
func ParseFilesList(v interface{}) (refs []FileRef, keys []string, ok bool) {
	if raw, isRaw := v.(json.RawMessage); isRaw {
		var decoded interface{}
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, nil, false
		}
		v = decoded
	}
	m, isMap := v.(map[string]interface{})
	if !isMap {
		return nil, nil, false
	}
	items, isList := m[FilesKey].([]interface{})
	if !isList || len(items) == 0 {
		return nil, nil, false
	}
	for _, item := range items {
		ref, key, ok := ParseFilesItem(item)
		if !ok {
			return nil, nil, false
		}
		refs = append(refs, ref)
		keys = append(keys, key)
	}
	return refs, keys, true
}
