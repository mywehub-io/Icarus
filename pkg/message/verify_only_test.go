package message

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

// verifyJS is a JSContext that has, or lacks, one stream and records every create and update.
type verifyJS struct {
	JSContext
	exists  bool
	creates int
	updates int
}

type verifyStream struct{ jetstream.Stream }

func (verifyStream) CachedInfo() *jetstream.StreamInfo {
	// No size cap and the wrong discard policy, so a service that is not verify-only updates it.
	return &jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: "S"}}
}

func (j *verifyJS) Stream(context.Context, string) (jetstream.Stream, error) {
	if !j.exists {
		return nil, jetstream.ErrStreamNotFound
	}
	return verifyStream{}, nil
}

func (j *verifyJS) CreateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	j.creates++
	j.exists = true
	return verifyStream{}, nil
}

func (j *verifyJS) UpdateStream(_ context.Context, cfg jetstream.StreamConfig) (jetstream.Stream, error) {
	j.updates++
	return verifyStream{}, nil
}

func newVerifyService(js *verifyJS, verifyOnly bool) *MessageService {
	s := &MessageService{js: js, logger: zap.NewNop(), resultStream: "RESULTS_UAT", resultSubject: "result_uat"}
	s.SetVerifyOnly(verifyOnly)
	return s
}

func TestVerifyOnly_MissingStreamIsNotCreated(t *testing.T) {
	ctx := context.Background()
	for _, check := range []struct {
		name string
		run  func(*MessageService) error
	}{
		{"EnsureStream", func(s *MessageService) error { return s.EnsureStream(ctx, "HTTP_REQUESTS_UAT") }},
		{"result stream", func(s *MessageService) error { return s.ensureResultStream(ctx) }},
	} {
		t.Run(check.name, func(t *testing.T) {
			js := &verifyJS{}
			err := check.run(newVerifyService(js, true))
			if !errors.Is(err, ErrVerifyOnlyStreamMissing) || !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("err = %v, want ErrVerifyOnlyStreamMissing wrapping ErrStreamNotFound", err)
			}
			if js.creates != 0 {
				t.Fatalf("created %d streams", js.creates)
			}
		})
	}
}

func TestVerifyOnly_ExistingStreamIsNotUpdated(t *testing.T) {
	ctx := context.Background()
	js := &verifyJS{exists: true}
	s := newVerifyService(js, true)
	if err := s.EnsureStream(ctx, "HTTP_REQUESTS_UAT"); err != nil {
		t.Fatal(err)
	}
	if err := s.ensureResultStream(ctx); err != nil {
		t.Fatal(err)
	}
	if js.creates != 0 || js.updates != 0 {
		t.Fatalf("creates %d updates %d, want none", js.creates, js.updates)
	}
}

// Off, the default, keeps today's behaviour: create a missing stream, cap an existing one.
func TestVerifyOnly_OffCreatesAndCaps(t *testing.T) {
	ctx := context.Background()
	js := &verifyJS{}
	s := newVerifyService(js, false)
	if err := s.EnsureStream(ctx, "HTTP_REQUESTS_UAT"); err != nil {
		t.Fatal(err)
	}
	if js.creates != 1 {
		t.Fatalf("creates = %d, want 1", js.creates)
	}
	if err := s.EnsureStream(ctx, "HTTP_REQUESTS_UAT"); err != nil {
		t.Fatal(err)
	}
	if js.updates != 1 {
		t.Fatalf("updates = %d, want 1 (size cap)", js.updates)
	}
}
