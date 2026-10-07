// Package filestore opens and writes the files byte values live in, as streams.
//
// A Store is bound to one run. It opens a fileref.FileRef only when its path is under that run's
// prefix (decisions D11), reads it with ranged GETs so no file is held whole, and writes a new file
// with a streamed upload whose content type is the caller's, never the ".json" default.
package filestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/wehubfusion/Icarus/pkg/fileref"
	"github.com/wehubfusion/Icarus/pkg/storage"
)

// ErrNotInRun is returned when a reference's path is not under the store's run prefix.
var ErrNotInRun = errors.New("file reference is not under the current run")

// Backend is the part of the blob client a Store needs. storage.AzureBlobClient implements it.
type Backend interface {
	UploadStream(ctx context.Context, blobPath string, body io.Reader, contentType string, metadata map[string]string) (string, error)
	DownloadRange(ctx context.Context, blobURL string, offset, count int64) ([]byte, error)
	URLFor(blobPath string) string
	DeleteBlob(ctx context.Context, blobPath string) error
}

// Store opens and creates the files of one run.
type Store interface {
	// Open streams the file. The caller must Close the reader.
	Open(ctx context.Context, ref fileref.FileRef) (io.ReadCloser, error)
	// OpenToDisk copies the file to a new temp file under dir and returns its path. The caller
	// removes it. For formats that need random access (decisions D6).
	OpenToDisk(ctx context.Context, ref fileref.FileRef, dir string) (string, error)
	// Create starts a streamed upload to path, which must be under the run's prefix.
	Create(ctx context.Context, path, contentType, fileName string) (Writer, error)
	// Run is the workflow and run this store is bound to.
	Run() (workflowID, runID string)
}

// Writer is a file being written. Close commits it and returns its reference with the true
// size; Abort discards it. Exactly one of them must be called.
type Writer interface {
	io.Writer
	Close() (fileref.FileRef, error)
	Abort() error
}

// ChunkBytes is the ranged GET size Open uses.
var ChunkBytes = storage.DefaultRangeChunkBytes

type store struct {
	backend    Backend
	workflowID string
	runID      string
}

// New returns a Store for one run.
func New(backend Backend, workflowID, runID string) (Store, error) {
	if backend == nil {
		return nil, fmt.Errorf("filestore: backend is required")
	}
	if workflowID == "" || runID == "" {
		return nil, fmt.Errorf("filestore: workflow and run ids are required")
	}
	return &store{backend: backend, workflowID: workflowID, runID: runID}, nil
}

func (s *store) Run() (string, string) { return s.workflowID, s.runID }

func (s *store) check(path string) error {
	if !fileref.InRun(fileref.FileRef{Path: path}, s.workflowID, s.runID) {
		return fmt.Errorf("%w: %q", ErrNotInRun, path)
	}
	return nil
}

func (s *store) Open(ctx context.Context, ref fileref.FileRef) (io.ReadCloser, error) {
	if err := s.check(ref.Path); err != nil {
		return nil, err
	}
	if ref.Size == 0 {
		return io.NopCloser(eofReader{}), nil
	}
	return storage.NewRangeReader(ctx, s.backend, s.backend.URLFor(ref.Path), 0, ref.Size, ChunkBytes)
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }

func (s *store) OpenToDisk(ctx context.Context, ref fileref.FileRef, dir string) (string, error) {
	rc, err := s.Open(ctx, ref)
	if err != nil {
		return "", err
	}
	defer rc.Close()

	ext := fileref.ExtensionFor(ref.ContentType)
	f, err := os.CreateTemp(dir, "wehub-file-*."+ext)
	if err != nil {
		return "", fmt.Errorf("filestore: create temp file: %w", err)
	}
	n, copyErr := io.Copy(f, rc)
	closeErr := f.Close()
	if copyErr == nil && closeErr != nil {
		copyErr = closeErr
	}
	if copyErr == nil && n != ref.Size {
		copyErr = fmt.Errorf("filestore: %s is %d bytes, reference says %d", ref.Path, n, ref.Size)
	}
	if copyErr != nil {
		_ = os.Remove(f.Name())
		return "", fmt.Errorf("filestore: copy to disk: %w", copyErr)
	}
	return f.Name(), nil
}

func (s *store) Create(ctx context.Context, path, contentType, fileName string) (Writer, error) {
	if err := s.check(path); err != nil {
		return nil, err
	}
	if contentType == "" {
		contentType = fileref.ContentTypeFor(path)
	}
	ctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	w := &writer{
		backend:     s.backend,
		path:        path,
		contentType: contentType,
		fileName:    fileName,
		pw:          pw,
		cancel:      cancel,
		done:        make(chan struct{}),
		ctx:         ctx,
	}
	go func() {
		defer close(w.done)
		_, err := s.backend.UploadStream(ctx, path, pr, contentType, map[string]string{
			"workflow_id": s.workflowID,
			"run_id":      s.runID,
		})
		// Unblock a writer still writing when the upload failed.
		_ = pr.CloseWithError(errOrClosed(err))
		w.uploadErr = err
	}()
	return w, nil
}

func errOrClosed(err error) error {
	if err == nil {
		return io.ErrClosedPipe
	}
	return err
}

type writer struct {
	backend     Backend
	path        string
	contentType string
	fileName    string
	pw          *io.PipeWriter
	cancel      context.CancelFunc
	done        chan struct{}
	ctx         context.Context

	mu        sync.Mutex
	n         int64
	finished  bool
	uploadErr error
}

func (w *writer) Write(p []byte) (int, error) {
	n, err := w.pw.Write(p)
	w.mu.Lock()
	w.n += int64(n)
	w.mu.Unlock()
	return n, err
}

func (w *writer) Close() (fileref.FileRef, error) {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return fileref.FileRef{}, fmt.Errorf("filestore: writer already closed")
	}
	w.finished = true
	w.mu.Unlock()

	_ = w.pw.Close()
	<-w.done
	w.cancel()
	if w.uploadErr != nil {
		return fileref.FileRef{}, fmt.Errorf("filestore: upload %s: %w", w.path, w.uploadErr)
	}
	return fileref.FileRef{
		Path:        w.path,
		Size:        w.n,
		ContentType: w.contentType,
		FileName:    w.fileName,
	}, nil
}

// Abort stops the upload. A streamed upload commits its block list only at the end, so an
// aborted one leaves no blob; the delete covers a backend that committed anyway.
func (w *writer) Abort() error {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return nil
	}
	w.finished = true
	w.mu.Unlock()

	w.cancel()
	_ = w.pw.CloseWithError(context.Canceled)
	<-w.done
	return w.backend.DeleteBlob(context.WithoutCancel(w.ctx), w.path)
}

type ctxKey struct{}

// WithStore returns ctx carrying store, for code that runs below an API it cannot change (the
// embedded runtime receives the parent unit's context).
func WithStore(ctx context.Context, store Store) context.Context {
	return context.WithValue(ctx, ctxKey{}, store)
}

// FromContext returns the store WithStore put on ctx, or nil.
func FromContext(ctx context.Context) Store {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(ctxKey{}).(Store)
	return s
}
