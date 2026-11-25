package grpcaudit

import (
	"context"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	_ "google.golang.org/protobuf/types/known/emptypb"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
	"github.com/wehubfusion/Icarus/platform/internal/testnats"
)

const testService = "grpcaudit.test.v1.Things"

func full(m string) string { return "/" + testService + "/" + m }

var registerOnce sync.Once

// registerTestService registers a service whose methods carry google.api.http bindings, built
// with protodesc so the test needs no generated code.
func registerTestService(t *testing.T) {
	t.Helper()
	registerOnce.Do(func() {
		withRule := func(r *annotations.HttpRule) *descriptorpb.MethodOptions {
			o := &descriptorpb.MethodOptions{}
			proto.SetExtension(o, annotations.E_Http, r)
			return o
		}
		// A binding left in unknown fields, as when options are decoded before the extension
		// is registered.
		raw := &descriptorpb.MethodOptions{}
		rule, err := proto.Marshal(&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v1/raw"}})
		if err != nil {
			panic(err)
		}
		b := protowire.AppendTag(nil, annotations.E_Http.TypeDescriptor().Number(), protowire.BytesType)
		b = protowire.AppendBytes(b, rule)
		raw.ProtoReflect().SetUnknown(b)

		method := func(name string, opts *descriptorpb.MethodOptions) *descriptorpb.MethodDescriptorProto {
			return &descriptorpb.MethodDescriptorProto{
				Name:       proto.String(name),
				InputType:  proto.String(".google.protobuf.Empty"),
				OutputType: proto.String(".google.protobuf.Empty"),
				Options:    opts,
			}
		}
		fdp := &descriptorpb.FileDescriptorProto{
			Name:       proto.String("grpcaudit_test/things.proto"),
			Package:    proto.String("grpcaudit.test.v1"),
			Syntax:     proto.String("proto3"),
			Dependency: []string{"google/protobuf/empty.proto"},
			Service: []*descriptorpb.ServiceDescriptorProto{{
				Name: proto.String("Things"),
				Method: []*descriptorpb.MethodDescriptorProto{
					method("GetThing", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v1/things/{id}"}})),
					method("CreateThing", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: "/v1/things"}, Body: "*"})),
					method("UpdateThing", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Patch{Patch: "/v1/things/{id}"}, Body: "*"})),
					method("ReplaceThing", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Put{Put: "/v1/things/{id}"}, Body: "*"})),
					method("DeleteThing", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Delete{Delete: "/v1/things/{id}"}})),
					method("CustomThing", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Custom{Custom: &annotations.CustomHttpPattern{Kind: "HEAD", Path: "/v1/things"}}})),
					method("BatchListThings", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Post{Post: "/v1/things:batchList"}, Body: "*"})),
					method("GetThingSecret", withRule(&annotations.HttpRule{Pattern: &annotations.HttpRule_Get{Get: "/v1/things/{id}/secret"}})),
					method("Unbound", nil),
					method("RawGet", raw),
				},
			}},
		}
		fd, err := protodesc.NewFile(fdp, protoregistry.GlobalFiles)
		if err != nil {
			panic(err)
		}
		if err := protoregistry.GlobalFiles.RegisterFile(fd); err != nil {
			panic(err)
		}
	})
}

func TestClassify(t *testing.T) {
	registerTestService(t)
	cfg := Config{Overrides: map[string]Class{
		full("BatchListThings"): ClassRead,
		full("GetThingSecret"):  ClassSensitiveRead,
		full("DeleteThing"):     ClassAuto, // ClassAuto in the table defers to the binding
		"/nope.v1.Gone/Method":  ClassIgnore,
	}}
	cases := map[string]Class{
		full("GetThing"):        ClassRead,
		full("CreateThing"):     ClassWrite,
		full("UpdateThing"):     ClassWrite,
		full("ReplaceThing"):    ClassWrite,
		full("DeleteThing"):     ClassWrite,
		full("CustomThing"):     ClassWrite,
		full("Unbound"):         ClassWrite,
		full("RawGet"):          ClassRead,
		full("BatchListThings"): ClassRead,
		full("GetThingSecret"):  ClassSensitiveRead,
		"/nope.v1.Gone/Method":  ClassIgnore,
		full("Missing"):         ClassAuto,
		"/unknown.Svc/Method":   ClassAuto,
		"garbage":               ClassAuto,
	}
	for m, want := range cases {
		assert.Equal(t, want, Classify(cfg, m), m)
	}
}

func TestHeaderMatcher(t *testing.T) {
	var deferred []string
	next := func(k string) (string, bool) {
		deferred = append(deferred, k)
		return runtime.DefaultHeaderMatcher(k)
	}
	m := HeaderMatcher(next)
	for _, k := range []string{
		"Grpc-Metadata-X-Forwarded-Client-Cert", "grpc-metadata-x-forwarded-client-cert",
		"Grpc-Metadata-Authorization", "GRPC-METADATA-AUTHORIZATION",
		"Grpc-Metadata-X-Wehub-Audit-Entry", "x-wehub-audit-entry", "X-Forwarded-Client-Cert",
	} {
		_, ok := m(k)
		assert.False(t, ok, k)
	}
	for _, k := range []string{"X-Request-Id", "Idempotency-Key", "X-Envoy-External-Address", "X-Forwarded-For", "x-request-id"} {
		got, ok := m(k)
		assert.True(t, ok, k)
		assert.Equal(t, k, got)
	}
	assert.Empty(t, deferred)
	got, ok := m("Authorization")
	assert.True(t, ok)
	assert.Equal(t, "grpcgateway-Authorization", got)
	_, ok = m("X-Something-Else")
	assert.False(t, ok)
	assert.Equal(t, []string{"Authorization", "X-Something-Else"}, deferred)

	got, ok = HeaderMatcher(nil)("Grpc-Metadata-Foo")
	assert.True(t, ok, "nil next defers to the default matcher")
	assert.Equal(t, "Foo", got)
}

func TestEntryMarkerThroughGateway(t *testing.T) {
	mux := runtime.NewServeMux(EntryMarker(), runtime.WithIncomingHeaderMatcher(HeaderMatcher(nil)))
	req := httptest.NewRequest("POST", "/v1/things", nil)
	req.Header.Set("Grpc-Metadata-X-Forwarded-Client-Cert", "URI=spiffe://cluster.local/ns/x/sa/gateway")
	req.Header.Set("Grpc-Metadata-Authorization", "Bearer forged")
	req.Header.Set("Grpc-Metadata-X-Wehub-Audit-Entry", "forged")
	req.Header.Set("Authorization", "Bearer real")
	req.Header.Set("X-Request-Id", "req-1")
	req.Header.Set("Idempotency-Key", "idem-1")
	req.Header.Set("X-Envoy-External-Address", "203.0.113.7")

	ctx, err := runtime.AnnotateContext(context.Background(), mux, req, full("CreateThing"))
	require.NoError(t, err)
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	assert.Equal(t, []string{entryMarker}, md.Get(EntryMetadataKey), "only the gateway's own marker is sent")
	assert.Empty(t, md.Get("x-forwarded-client-cert"))
	assert.Equal(t, []string{"Bearer real"}, md.Get("authorization"))
	assert.Equal(t, []string{"req-1"}, md.Get("x-request-id"))
	assert.Equal(t, []string{"idem-1"}, md.Get("idempotency-key"))
	assert.Equal(t, []string{"203.0.113.7"}, md.Get("x-envoy-external-address"))

	assert.True(t, isEntry(md, nil))
	assert.False(t, isEntry(metadata.Pairs(EntryMetadataKey, "forged"), nil))
}

// harness runs the interceptor against an embedded NATS stream.
type harness struct {
	t    *testing.T
	srv  *testnats.Server
	em   *audit.Emitter
	mu   sync.Mutex
	now  time.Time
	icpt grpc.UnaryServerInterceptor
}

const (
	orgID     = "0b5b1b7e-6a43-4c0e-9a51-2f4f6b8e0d11"
	gatewayID = "spiffe://cluster.local/ns/istio-ingress/sa/gateway"
)

func newHarness(t *testing.T, identity Identity) *harness {
	t.Helper()
	registerTestService(t)
	h := &harness{t: t, srv: testnats.Start(t), now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	h.srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em, err := audit.NewEmitter(h.srv.Conn, audit.Options{
		Service:        "things",
		PublishTimeout: time.Second,
		Now:            h.clock,
	})
	require.NoError(t, err)
	em.Start(context.Background())
	t.Cleanup(func() { _ = em.Close(context.Background()) })
	h.em = em
	h.icpt = UnaryServerInterceptor(Config{
		Emitter:         em,
		Service:         "things",
		Identity:        func(context.Context) Identity { return identity },
		EntryPrincipals: []string{gatewayID},
		Overrides: map[string]Class{
			full("BatchListThings"): ClassRead,
			full("GetThingSecret"):  ClassSensitiveRead,
			full("Unbound"):         ClassIgnore,
		},
	})
	return h
}

func (h *harness) clock() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *harness) advance(d time.Duration) {
	h.mu.Lock()
	h.now = h.now.Add(d)
	h.mu.Unlock()
}

func (h *harness) call(ctx context.Context, method string, handler func(ctx context.Context) error) error {
	_, err := h.icpt(ctx, nil, &grpc.UnaryServerInfo{FullMethod: full(method)}, func(ctx context.Context, _ any) (any, error) {
		return nil, handler(ctx)
	})
	return err
}

// events returns the stream's events other than the producer's own.
func (h *harness) events() []*auditpb.Event {
	var out []*auditpb.Event
	for _, m := range h.srv.Messages(h.t, "AUDIT") {
		ev := &auditpb.Event{}
		require.NoError(h.t, proto.Unmarshal(m.Data, ev))
		if ev.GetCategory() != auditpb.Category_CATEGORY_SYSTEM {
			out = append(out, ev)
		}
	}
	return out
}

func entryCtx(pairs ...string) context.Context {
	md := metadata.Pairs(append([]string{EntryMetadataKey, entryMarker}, pairs...)...)
	return metadata.NewIncomingContext(context.Background(), md)
}

func xfccCtx(uri string, pairs ...string) context.Context {
	md := metadata.Pairs(append([]string{"x-forwarded-client-cert", "By=spiffe://cluster.local/ns/a/sa/b;URI=" + uri}, pairs...)...)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestWriteAtEntryIsRecorded(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "user-1", ActorType: auditpb.ActorType_ACTOR_TYPE_USER, ClientID: orgID})
	ctx := entryCtx(
		"idempotency-key", "idem-1", "x-request-id", "req-1",
		"x-envoy-external-address", "203.0.113.7", "x-forwarded-for", "198.51.100.1",
		"grpcgateway-user-agent", "wehub-cli/1.2", "user-agent", "grpc-go/1.76.0",
	)
	err := h.call(ctx, "CreateThing", func(ctx context.Context) error {
		audit.RecordDecision(ctx, "thing", "create", "t-1", true)
		audit.SetJobID(ctx, "JOB-1")
		audit.SetAttr(ctx, "kind", "blue")
		return nil
	})
	require.NoError(t, err)

	events := h.events()
	require.Len(t, events, 1)
	ev := events[0]
	org := uuid.MustParse(orgID)
	assert.Equal(t, "things.CreateThing", ev.GetAction())
	assert.Equal(t, auditpb.Category_CATEGORY_WRITE, ev.GetCategory())
	assert.Equal(t, auditpb.Outcome_OUTCOME_OK, ev.GetOutcome())
	assert.Zero(t, ev.GetCode())
	assert.Equal(t, org[:], ev.GetClientId())
	assert.Equal(t, "user-1", ev.GetActor().GetId())
	assert.Equal(t, auditpb.ActorType_ACTOR_TYPE_USER, ev.GetActor().GetType())
	assert.Equal(t, "idem-1", ev.GetRequestId())
	assert.Equal(t, []byte{203, 0, 113, 7}, ev.GetSourceIp())
	assert.Equal(t, "wehub-cli/1.2", ev.GetUserAgent())
	assert.Equal(t, "JOB-1", ev.GetJobId())
	assert.Equal(t, "thing", ev.GetResource().GetType())
	assert.Equal(t, "t-1", ev.GetResource().GetId())
	assert.True(t, ev.GetDecision().GetGranted())
	assert.Equal(t, map[string]string{"kind": "blue"}, ev.GetAttrs())
}

func TestEntryByPrincipalAndFallbacks(t *testing.T) {
	h := newHarness(t, Identity{})
	ctx := xfccCtx(gatewayID, "x-request-id", "req-2", "x-forwarded-for", "10.0.0.1, 198.51.100.9", "user-agent", "curl/8")
	err := h.call(ctx, "DeleteThing", func(ctx context.Context) error {
		audit.RecordDecision(ctx, "thing", "delete", "t-9", true)
		audit.SetResource(ctx, "thing", "t-override")
		audit.SetReason(ctx, "IN_USE")
		return status.Error(codes.FailedPrecondition, "in use")
	})
	require.Error(t, err)

	events := h.events()
	require.Len(t, events, 1)
	ev := events[0]
	assert.Equal(t, auditpb.Outcome_OUTCOME_REFUSED, ev.GetOutcome())
	assert.Equal(t, uint32(codes.FailedPrecondition), ev.GetCode())
	assert.Equal(t, "req-2", ev.GetRequestId())
	assert.Equal(t, []byte{198, 51, 100, 9}, ev.GetSourceIp(), "the last x-forwarded-for element")
	assert.Equal(t, "curl/8", ev.GetUserAgent())
	assert.Equal(t, "t-override", ev.GetResource().GetId(), "SetResource wins over the decision")
	assert.Equal(t, "IN_USE", ev.GetReason())
	assert.Empty(t, ev.GetClientId())
	assert.Equal(t, auditpb.ActorType_ACTOR_TYPE_UNSPECIFIED, ev.GetActor().GetType())
}

func TestPeerAddressIsTheLastFallback(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "svc", ActorType: auditpb.ActorType_ACTOR_TYPE_SERVICE_ACCOUNT, ClientID: orgID})
	ctx := peer.NewContext(entryCtx(), &peer.Peer{Addr: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}})
	require.NoError(t, h.call(ctx, "UpdateThing", func(context.Context) error { return nil }))
	events := h.events()
	require.Len(t, events, 1)
	assert.Equal(t, net.ParseIP("2001:db8::1").To16(), net.IP(events[0].GetSourceIp()))
}

func TestWhatIsNotRecorded(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "user-1", ClientID: orgID})
	noop := func(context.Context) error { return nil }

	// A write that did not come from an entry: another service's fan-out.
	require.NoError(t, h.call(xfccCtx("spiffe://cluster.local/ns/olympus/sa/zeus"), "CreateThing", noop))
	require.NoError(t, h.call(context.Background(), "CreateThing", noop))
	// Plain reads, even at an entry, including a POST read from the override table.
	require.NoError(t, h.call(entryCtx(), "GetThing", noop))
	require.NoError(t, h.call(entryCtx(), "BatchListThings", noop))
	// An ignored method, even when refused.
	require.Error(t, h.call(entryCtx(), "Unbound", func(context.Context) error {
		return status.Error(codes.PermissionDenied, "no")
	}))
	// A forged marker is not an entry.
	forged := metadata.NewIncomingContext(context.Background(), metadata.Pairs(EntryMetadataKey, "guess"))
	require.NoError(t, h.call(forged, "CreateThing", noop))

	assert.Empty(t, h.events())
}

func TestSensitiveReadAtEntryIsRecordedAsync(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "user-1", ClientID: orgID})
	require.NoError(t, h.call(entryCtx(), "GetThingSecret", func(context.Context) error { return nil }))
	testnats.WaitFor(t, 5*time.Second, "the sensitive read", func() bool { return len(h.events()) == 1 })
	assert.Equal(t, auditpb.Category_CATEGORY_SENSITIVE_READ, h.events()[0].GetCategory())
}

func TestFailedWriteOutcome(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "user-1", ClientID: orgID})
	require.Error(t, h.call(entryCtx(), "CreateThing", func(context.Context) error {
		return status.Error(codes.Internal, "boom")
	}))
	events := h.events()
	require.Len(t, events, 1)
	assert.Equal(t, auditpb.Outcome_OUTCOME_FAILED, events[0].GetOutcome())
	assert.Equal(t, uint32(codes.Internal), events[0].GetCode())
}

// A write that fails before any authorisation check granted anything may carry a forged
// X-Client-Id, so it goes to the platform chain, rate limited; with a grant it is the
// organisation's own failure and goes to its chain.
func TestFailedWriteWithoutGrantGoesToPlatformChain(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "user-1", ActorType: auditpb.ActorType_ACTOR_TYPE_USER, ClientID: orgID})
	fail := func(context.Context) error { return status.Error(codes.InvalidArgument, "bad") }
	require.Error(t, h.call(entryCtx(), "CreateThing", fail))
	require.Error(t, h.call(entryCtx(), "CreateThing", fail)) // suppressed within the minute
	require.Error(t, h.call(entryCtx(), "CreateThing", func(ctx context.Context) error {
		audit.RecordDecision(ctx, "thing", "create", "", true)
		return status.Error(codes.InvalidArgument, "bad")
	}))

	events := h.events()
	require.Len(t, events, 2)
	assert.Empty(t, events[0].GetClientId())
	assert.Equal(t, orgID, events[0].GetAttrs()["claimed_client_id"])
	assert.Equal(t, auditpb.Category_CATEGORY_WRITE, events[0].GetCategory())
	org := uuid.MustParse(orgID)
	assert.Equal(t, org[:], events[1].GetClientId())
	assert.NotContains(t, events[1].GetAttrs(), "claimed_client_id")
}

// IdentityRecorder, placed after the service's authentication, wins over Config.Identity, which
// only sees the incoming metadata.
func TestIdentityRecorderTakesPrecedence(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "from-metadata", ClientID: orgID})
	type authKey struct{}
	recorder := IdentityRecorder(func(ctx context.Context) Identity {
		id, _ := ctx.Value(authKey{}).(string)
		return Identity{ActorID: id, ActorType: auditpb.ActorType_ACTOR_TYPE_USER, ClientID: orgID}
	})
	err := h.call(entryCtx(), "CreateThing", func(ctx context.Context) error {
		ctx = context.WithValue(ctx, authKey{}, "from-auth") // what the auth interceptor would add
		_, err := recorder(ctx, nil, &grpc.UnaryServerInfo{FullMethod: full("CreateThing")},
			func(context.Context, any) (any, error) { return nil, nil })
		return err
	})
	require.NoError(t, err)
	events := h.events()
	require.Len(t, events, 1)
	assert.Equal(t, "from-auth", events[0].GetActor().GetId())
	assert.Equal(t, auditpb.ActorType_ACTOR_TYPE_USER, events[0].GetActor().GetType())

	// Without a recorded identity the metadata fallback is used.
	require.NoError(t, h.call(entryCtx(), "UpdateThing", func(context.Context) error { return nil }))
	events = h.events()
	require.Len(t, events, 2)
	assert.Equal(t, "from-metadata", events[1].GetActor().GetId())
}

func TestDenialWithGrantGoesToOrganisationChain(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "user-1", ActorType: auditpb.ActorType_ACTOR_TYPE_USER, ClientID: orgID})
	// Not an entry: denials are recorded at any hop.
	err := h.call(context.Background(), "GetThing", func(ctx context.Context) error {
		audit.RecordDecision(ctx, "project", "read", "p-1", true)
		audit.RecordDecision(ctx, "thing", "read", "t-1", false)
		return status.Error(codes.PermissionDenied, "no")
	})
	require.Error(t, err)
	events := h.events()
	require.Len(t, events, 1)
	ev := events[0]
	org := uuid.MustParse(orgID)
	assert.Equal(t, auditpb.Category_CATEGORY_DENIAL, ev.GetCategory())
	assert.Equal(t, auditpb.Outcome_OUTCOME_DENIED, ev.GetOutcome())
	assert.Equal(t, uint32(codes.PermissionDenied), ev.GetCode())
	assert.Equal(t, org[:], ev.GetClientId())
	assert.False(t, ev.GetDecision().GetGranted())
	assert.Equal(t, "t-1", ev.GetResource().GetId())
	assert.NotContains(t, ev.GetAttrs(), "claimed_client_id")
}

func TestUntrustedDenialGoesToPlatformChainRateLimited(t *testing.T) {
	h := newHarness(t, Identity{ClientID: orgID})
	deny := func(context.Context) error { return status.Error(codes.Unauthenticated, "no token") }
	from := func(ip string) context.Context {
		return metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-envoy-external-address", ip))
	}

	require.Error(t, h.call(from("203.0.113.1"), "CreateThing", deny))
	require.Error(t, h.call(from("203.0.113.1"), "CreateThing", deny)) // suppressed
	require.Error(t, h.call(from("203.0.113.1"), "CreateThing", deny)) // suppressed
	require.Error(t, h.call(from("203.0.113.2"), "CreateThing", deny)) // another source
	require.Error(t, h.call(from("203.0.113.1"), "GetThing", deny))    // another method
	events := h.events()
	require.Len(t, events, 3)
	for _, ev := range events {
		assert.Empty(t, ev.GetClientId(), "an untrusted organisation id never reaches its chain")
		assert.Equal(t, orgID, ev.GetAttrs()["claimed_client_id"])
		assert.Equal(t, auditpb.Category_CATEGORY_DENIAL, ev.GetCategory())
		assert.NotContains(t, ev.GetAttrs(), "suppressed")
	}

	h.advance(61 * time.Second)
	require.Error(t, h.call(from("203.0.113.1"), "CreateThing", deny))
	events = h.events()
	require.Len(t, events, 4)
	assert.Equal(t, "2", events[3].GetAttrs()["suppressed"])
}

func TestInvalidClientIDGoesToPlatformChain(t *testing.T) {
	h := newHarness(t, Identity{ActorID: "user-1", ClientID: "not-a-uuid"})
	require.NoError(t, h.call(entryCtx(), "CreateThing", func(context.Context) error { return nil }))
	events := h.events()
	require.Len(t, events, 1)
	assert.Empty(t, events[0].GetClientId())
	assert.Equal(t, "not-a-uuid", events[0].GetAttrs()["claimed_client_id"])
}

func TestOutcomeFor(t *testing.T) {
	cases := map[codes.Code]auditpb.Outcome{
		codes.OK:                 auditpb.Outcome_OUTCOME_OK,
		codes.PermissionDenied:   auditpb.Outcome_OUTCOME_DENIED,
		codes.Unauthenticated:    auditpb.Outcome_OUTCOME_DENIED,
		codes.FailedPrecondition: auditpb.Outcome_OUTCOME_REFUSED,
		codes.AlreadyExists:      auditpb.Outcome_OUTCOME_REFUSED,
		codes.Aborted:            auditpb.Outcome_OUTCOME_REFUSED,
		codes.NotFound:           auditpb.Outcome_OUTCOME_FAILED,
		codes.Unavailable:        auditpb.Outcome_OUTCOME_FAILED,
	}
	for c, want := range cases {
		assert.Equal(t, want, outcomeFor(c), c.String())
	}
}

func TestNilEmitterStillRunsHandler(t *testing.T) {
	icpt := UnaryServerInterceptor(Config{Service: "things"})
	called := false
	_, err := icpt(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: full("CreateThing")},
		func(ctx context.Context, _ any) (any, error) {
			called = true
			require.NotNil(t, audit.CollectorFrom(ctx))
			return nil, nil
		})
	require.NoError(t, err)
	assert.True(t, called)
}
