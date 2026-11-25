// Package testnats runs an embedded NATS server with JetStream for the module's tests. Only
// tests import it; a module rule test keeps the server out of every other package.
package testnats

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Server is an embedded NATS server with JetStream, and a client connection to it.
type Server struct {
	Server *server.Server
	Conn   *nats.Conn
	JS     jetstream.JetStream
}

// Start runs a server on a random port with JetStream stored in a temporary directory. Both are
// shut down when the test ends.
func Start(t testing.TB) *Server {
	t.Helper()
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      server.RANDOM_PORT,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		t.Fatalf("testnats: new server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("testnats: server not ready")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		s.Shutdown()
		t.Fatalf("testnats: connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		s.Shutdown()
		t.Fatalf("testnats: jetstream: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		s.Shutdown()
		s.WaitForShutdown()
	})
	return &Server{Server: s, Conn: nc, JS: js}
}

// Connect opens another client connection, closed when the test ends.
func (s *Server) Connect(t testing.TB) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.Server.ClientURL())
	if err != nil {
		t.Fatalf("testnats: connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// CreateStream creates a stream, as the stream's owning service would.
func (s *Server) CreateStream(t testing.TB, name string, subjects ...string) jetstream.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := s.JS.CreateStream(ctx, jetstream.StreamConfig{
		Name:       name,
		Subjects:   subjects,
		Storage:    jetstream.MemoryStorage,
		Duplicates: time.Minute,
	})
	if err != nil {
		t.Fatalf("testnats: create stream %s: %v", name, err)
	}
	return st
}

// Messages reads every message in a stream, in stream order.
func (s *Server) Messages(t testing.TB, stream string) []jetstream.RawStreamMsg {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := s.JS.Stream(ctx, stream)
	if err != nil {
		t.Fatalf("testnats: stream %s: %v", stream, err)
	}
	info, err := st.Info(ctx)
	if err != nil {
		t.Fatalf("testnats: stream info %s: %v", stream, err)
	}
	var out []jetstream.RawStreamMsg
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := st.GetMsg(ctx, seq)
		if err != nil {
			continue
		}
		out = append(out, *m)
	}
	return out
}

// WaitFor polls cond every 10ms until it is true or timeout passes, and fails the test then.
func WaitFor(t testing.TB, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}
