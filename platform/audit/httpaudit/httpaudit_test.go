package httpaudit_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
	"github.com/wehubfusion/Icarus/platform/audit/grpcaudit"
	"github.com/wehubfusion/Icarus/platform/audit/httpaudit"
	"github.com/wehubfusion/Icarus/platform/internal/testnats"
)

const (
	orgID     = "0b5b1b7e-6a43-4c0e-9a51-2f4f6b8e0d11"
	gatewayID = "spiffe://cluster.local/ns/istio-ingress/sa/gateway"
	gwXFCC    = "By=spiffe://cluster.local/ns/proxeny/sa/epistates;URI=" + gatewayID
)

type harness struct {
	t   *testing.T
	srv *testnats.Server
	h   http.Handler
}

func newHarness(t *testing.T, mutate func(*httpaudit.Config)) *harness {
	t.Helper()
	srv := testnats.Start(t)
	srv.CreateStream(t, "AUDIT", "audit.v1.>")
	em, err := audit.NewEmitter(srv.Conn, audit.Options{Service: "epistates", PublishTimeout: time.Second})
	require.NoError(t, err)
	em.Start(context.Background())
	t.Cleanup(func() { _ = em.Close(context.Background()) })

	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/runners/{id}", func(w http.ResponseWriter, r *http.Request) {
		audit.RecordDecision(r.Context(), "runner", "update", r.PathValue("id"), true)
		w.WriteHeader(statusFrom(r))
	})
	mux.HandleFunc("GET /v1/runners/{id}", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("POST /internal/v1/runners", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	mux.HandleFunc("POST /v1/refuse", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	})
	mux.HandleFunc("POST /v1/stream", func(w http.ResponseWriter, r *http.Request) {
		_, ok := w.(http.Flusher)
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.(http.Flusher).Flush()
	})

	cfg := httpaudit.Config{
		Emitter:         em,
		Service:         "epistates",
		EntryPrincipals: []string{gatewayID},
		Identity: func(*http.Request) grpcaudit.Identity {
			return grpcaudit.Identity{ActorID: "user-1", ActorType: auditpb.ActorType_ACTOR_TYPE_USER, ClientID: orgID}
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}
	return &harness{t: t, srv: srv, h: httpaudit.Middleware(cfg)(mux)}
}

func statusFrom(r *http.Request) int {
	switch r.URL.Query().Get("status") {
	case "401":
		return http.StatusUnauthorized
	case "403":
		return http.StatusForbidden
	case "409":
		return http.StatusConflict
	case "500":
		return http.StatusInternalServerError
	case "302":
		return http.StatusFound
	}
	return http.StatusOK
}

func (h *harness) do(method, target string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.RemoteAddr = "192.0.2.10:5555"
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.h.ServeHTTP(rec, req)
	return rec
}

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

func TestWriteAtEntryIsRecorded(t *testing.T) {
	h := newHarness(t, nil)
	h.do("POST", "/v1/runners/r-1", map[string]string{
		"X-Forwarded-Client-Cert":  gwXFCC,
		"Idempotency-Key":          "idem-1",
		"X-Envoy-External-Address": "203.0.113.5",
		"User-Agent":               "wehub-cli/1.0",
	})
	events := h.events()
	require.Len(t, events, 1)
	ev := events[0]
	org := uuid.MustParse(orgID)
	assert.Equal(t, "epistates.POST /v1/runners/{id}", ev.GetAction())
	assert.Equal(t, auditpb.Category_CATEGORY_WRITE, ev.GetCategory())
	assert.Equal(t, auditpb.Outcome_OUTCOME_OK, ev.GetOutcome())
	assert.Equal(t, org[:], ev.GetClientId())
	assert.Equal(t, "idem-1", ev.GetRequestId())
	assert.Equal(t, []byte{203, 0, 113, 5}, ev.GetSourceIp())
	assert.Equal(t, "wehub-cli/1.0", ev.GetUserAgent())
	assert.Equal(t, "r-1", ev.GetResource().GetId())
}

func TestDefaultEntryRule(t *testing.T) {
	h := newHarness(t, nil)
	// Not from the gateway principal.
	h.do("POST", "/v1/runners/r-1", map[string]string{"X-Forwarded-Client-Cert": "URI=spiffe://cluster.local/ns/olympus/sa/zeus"})
	h.do("POST", "/v1/runners/r-1", nil)
	// From the gateway, but under /internal/.
	h.do("POST", "/internal/v1/runners", map[string]string{"X-Forwarded-Client-Cert": gwXFCC})
	// A read at an entry.
	h.do("GET", "/v1/runners/r-1", map[string]string{"X-Forwarded-Client-Cert": gwXFCC})
	assert.Empty(t, h.events())
}

func TestStatusMapping(t *testing.T) {
	cases := []struct {
		status  string
		outcome auditpb.Outcome
		code    codes.Code
	}{
		{"302", auditpb.Outcome_OUTCOME_OK, codes.OK},
		{"401", auditpb.Outcome_OUTCOME_DENIED, codes.Unauthenticated},
		{"403", auditpb.Outcome_OUTCOME_DENIED, codes.PermissionDenied},
		{"409", auditpb.Outcome_OUTCOME_REFUSED, codes.Aborted},
		{"500", auditpb.Outcome_OUTCOME_FAILED, codes.Internal},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			h := newHarness(t, nil)
			h.do("POST", "/v1/runners/r-1?status="+c.status, map[string]string{"X-Forwarded-Client-Cert": gwXFCC})
			events := h.events()
			require.Len(t, events, 1)
			assert.Equal(t, c.outcome, events[0].GetOutcome())
			assert.Equal(t, uint32(c.code), events[0].GetCode())
		})
	}
}

func TestDenialAtAnyHop(t *testing.T) {
	// The handler records a grant before refusing, so the organisation chain is trusted.
	h := newHarness(t, nil)
	h.do("POST", "/internal/v1/runners", nil)
	h.do("POST", "/v1/runners/r-2?status=403", nil)
	events := h.events()
	require.Len(t, events, 1)
	assert.Equal(t, auditpb.Category_CATEGORY_DENIAL, events[0].GetCategory())
	assert.NotEmpty(t, events[0].GetClientId())
}

func TestUntrustedDenialIsRateLimited(t *testing.T) {
	h := newHarness(t, nil)
	// /v1/refuse answers 401 without any authorisation check granting anything.
	for i := 0; i < 3; i++ {
		h.do("POST", "/v1/refuse", nil)
	}
	events := h.events()
	require.Len(t, events, 1, "one event per claimed organisation, source and route a minute")
	ev := events[0]
	assert.Empty(t, ev.GetClientId())
	assert.Equal(t, orgID, ev.GetAttrs()["claimed_client_id"])
	assert.Equal(t, auditpb.Outcome_OUTCOME_DENIED, ev.GetOutcome())
	assert.Equal(t, []byte{192, 0, 2, 10}, ev.GetSourceIp(), "the peer address when no header names one")
}

func TestNilEmitterStillServes(t *testing.T) {
	refuse := httpaudit.Middleware(httpaudit.Config{Service: "epistates"})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	rec := httptest.NewRecorder()
	refuse.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/x", nil))
	assert.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestFlusherIsKept(t *testing.T) {
	h := newHarness(t, nil)
	rec := h.do("POST", "/v1/stream", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, rec.Flushed)
}

func TestCustomRouteAndIgnore(t *testing.T) {
	h := newHarness(t, func(c *httpaudit.Config) {
		c.Route = func(*http.Request) string { return "/v1/runners/:id" }
		c.IsEntry = func(*http.Request) bool { return true }
		c.Classify = func(method, route string) grpcaudit.Class {
			if method == http.MethodGet {
				return grpcaudit.ClassSensitiveRead
			}
			return grpcaudit.ClassIgnore
		}
	})
	h.do("POST", "/v1/runners/r-1?status=403", nil)
	h.do("GET", "/v1/runners/r-1", nil)
	testnats.WaitFor(t, 5*time.Second, "the sensitive read", func() bool { return len(h.events()) == 1 })
	ev := h.events()[0]
	assert.Equal(t, "epistates.GET /v1/runners/:id", ev.GetAction())
	assert.Equal(t, auditpb.Category_CATEGORY_SENSITIVE_READ, ev.GetCategory())
}
