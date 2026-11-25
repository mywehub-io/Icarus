// Package grpcaudit records audit events at the gRPC API layer: one event per call, built by a
// unary server interceptor after the handler returns.
//
// What is recorded:
//   - every call refused with PermissionDenied or Unauthenticated, at any hop (category DENIAL);
//   - a write at an entry, that is a call that came from outside the platform (category WRITE);
//   - a sensitive read at an entry (category SENSITIVE_READ, published asynchronously).
//
// Plain reads, and writes one service makes on another, are not recorded, so a fan-out is
// counted once.
//
// A method is classified from its google.api.http binding (GET is a read; any other verb, or no
// binding, is a write) unless Config.Overrides names it. A call is an entry when it carries this
// process's gateway marker (see EntryMarker) or when the last x-forwarded-client-cert element
// names one of Config.EntryPrincipals.
//
// Put the interceptor first in the chain, so it sees the refusals of the authentication
// interceptor. Identity then runs on the context the interceptor received, so it must derive the
// caller from the incoming metadata, not from values an inner interceptor adds.
package grpcaudit

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/wehubfusion/Icarus/platform/audit"
	"github.com/wehubfusion/Icarus/platform/audit/auditpb"
	"github.com/wehubfusion/Icarus/platform/audit/internal/hook"
)

// EntryMetadataKey is the metadata key EntryMarker sets on every call from the gateway.
const EntryMetadataKey = "x-wehub-audit-entry"

// Metadata keys the interceptor reads.
const (
	mdXFCC           = "x-forwarded-client-cert"
	mdIdempotencyKey = "idempotency-key"
	mdRequestID      = "x-request-id"
	mdEnvoyExternal  = "x-envoy-external-address"
	mdForwardedFor   = "x-forwarded-for"
	mdGatewayUA      = "grpcgateway-user-agent"
	mdUserAgent      = "user-agent"
)

// Identity is the caller of one request.
type Identity struct {
	// ActorID is the Entra object id of the person or service account. Empty means unknown,
	// recorded as ACTOR_TYPE_UNSPECIFIED.
	ActorID   string
	ActorType auditpb.ActorType
	// ClientID is the organisation id (a UUID string) the caller named. May be empty.
	ClientID string
}

// IdentityFunc returns the caller of the request in ctx.
type IdentityFunc func(ctx context.Context) Identity

// Class says how a method is treated.
type Class int

const (
	// ClassAuto classifies the method from its google.api.http binding. As a result of
	// Classify it means the method could not be found and has no override; the interceptor
	// treats such a method as a write.
	ClassAuto Class = iota
	// ClassWrite is recorded at an entry.
	ClassWrite
	// ClassRead is not recorded unless refused.
	ClassRead
	// ClassSensitiveRead is recorded at an entry, asynchronously.
	ClassSensitiveRead
	// ClassIgnore is never recorded, not even when refused.
	ClassIgnore
)

func (c Class) String() string {
	switch c {
	case ClassAuto:
		return "auto"
	case ClassWrite:
		return "write"
	case ClassRead:
		return "read"
	case ClassSensitiveRead:
		return "sensitive_read"
	case ClassIgnore:
		return "ignore"
	}
	return "unknown"
}

// Config configures the interceptor. Every value comes from the service's code or configuration.
type Config struct {
	// Emitter publishes the events. Nil records nothing.
	Emitter *audit.Emitter
	// Service names the service in each action: <Service>.<Method>.
	Service string
	// Identity returns the caller. Nil means every caller is unknown.
	Identity IdentityFunc
	// EntryPrincipals are the SPIFFE ids, such as the ingress gateway's, whose calls are entries.
	EntryPrincipals []string
	// Overrides maps a full method name (/pkg.Service/Method) to its class, ahead of the
	// google.api.http binding.
	Overrides map[string]Class
	// Logger receives hook failures. Default: no logging.
	Logger *zap.Logger
}

// entryMarker is this process's gateway marker, made once.
var entryMarker = newMarker()

func newMarker() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic("grpcaudit: no randomness for the entry marker: " + err.Error())
	}
	return hex.EncodeToString(b)
}

// EntryMarker returns a ServeMux option that adds this process's marker to the metadata of every
// call the gRPC-Gateway makes, so the interceptor in the same process treats it as an entry. The
// marker is random per process, so a caller cannot forge it; HeaderMatcher drops a client-sent
// copy as well.
func EntryMarker() runtime.ServeMuxOption {
	return runtime.WithMetadata(func(context.Context, *http.Request) metadata.MD {
		return metadata.Pairs(EntryMetadataKey, entryMarker)
	})
}

// HeaderMatcher wraps a gateway header matcher. It never forwards a client-sent
// Grpc-Metadata-X-Forwarded-Client-Cert, Grpc-Metadata-Authorization or
// Grpc-Metadata-X-Wehub-Audit-Entry, nor the same headers without the prefix, since mesh identity
// and the entry marker must never come from a client. It forwards X-Request-Id, Idempotency-Key,
// X-Envoy-External-Address and X-Forwarded-For as they are, and defers everything else to next
// (runtime.DefaultHeaderMatcher when next is nil).
func HeaderMatcher(next runtime.HeaderMatcherFunc) runtime.HeaderMatcherFunc {
	if next == nil {
		next = runtime.DefaultHeaderMatcher
	}
	return func(key string) (string, bool) {
		switch {
		case equalsAny(key,
			runtime.MetadataHeaderPrefix+"X-Forwarded-Client-Cert",
			runtime.MetadataHeaderPrefix+"Authorization",
			runtime.MetadataHeaderPrefix+"X-Wehub-Audit-Entry",
			"X-Forwarded-Client-Cert",
			"X-Wehub-Audit-Entry"):
			return "", false
		case equalsAny(key, "X-Request-Id", "Idempotency-Key", "X-Envoy-External-Address", "X-Forwarded-For"):
			return key, true
		}
		return next(key)
	}
}

func equalsAny(key string, names ...string) bool {
	for _, n := range names {
		if strings.EqualFold(key, n) {
			return true
		}
	}
	return false
}

// Classify returns the class of fullMethod (/pkg.Service/Method): its override if there is one,
// else ClassRead for a GET binding and ClassWrite for any other binding or none. It returns
// ClassAuto when the method has no override and its descriptor is not in
// protoregistry.GlobalFiles.
func Classify(cfg Config, fullMethod string) Class {
	if c, ok := cfg.Overrides[fullMethod]; ok && c != ClassAuto {
		return c
	}
	md := findMethod(fullMethod)
	if md == nil {
		return ClassAuto
	}
	rule := httpRule(md)
	if rule == nil {
		return ClassWrite
	}
	if _, ok := rule.GetPattern().(*annotations.HttpRule_Get); ok {
		return ClassRead
	}
	return ClassWrite
}

func findMethod(fullMethod string) protoreflect.MethodDescriptor {
	name := strings.TrimPrefix(fullMethod, "/")
	i := strings.LastIndex(name, "/")
	if i <= 0 || i == len(name)-1 {
		return nil
	}
	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name[:i]))
	if err != nil {
		return nil
	}
	sd, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return nil
	}
	return sd.Methods().ByName(protoreflect.Name(name[i+1:]))
}

// httpRule returns the method's google.api.http binding, or nil. It also reads a binding left in
// the options' unknown fields, which happens when the options were decoded before the extension
// was registered.
func httpRule(md protoreflect.MethodDescriptor) *annotations.HttpRule {
	opts, ok := md.Options().(*descriptorpb.MethodOptions)
	if !ok || opts == nil {
		return nil
	}
	if proto.HasExtension(opts, annotations.E_Http) {
		if r, ok := proto.GetExtension(opts, annotations.E_Http).(*annotations.HttpRule); ok && r != nil {
			return r
		}
	}
	num := annotations.E_Http.TypeDescriptor().Number()
	b := opts.ProtoReflect().GetUnknown()
	for len(b) > 0 {
		n, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return nil
		}
		b = b[l:]
		if n == num && typ == protowire.BytesType {
			v, m := protowire.ConsumeBytes(b)
			if m < 0 {
				return nil
			}
			r := &annotations.HttpRule{}
			if proto.Unmarshal(v, r) == nil {
				return r
			}
			return nil
		}
		m := protowire.ConsumeFieldValue(n, typ, b)
		if m < 0 {
			return nil
		}
		b = b[m:]
	}
	return nil
}

// UnaryServerInterceptor returns the audit interceptor. See the package documentation for what
// it records.
func UnaryServerInterceptor(cfg Config) grpc.UnaryServerInterceptor {
	rec := hook.NewRecorder(cfg.Emitter, cfg.Logger)
	principals := hook.NewPrincipalSet(cfg.EntryPrincipals)
	var classes sync.Map
	classOf := func(m string) Class {
		if c, ok := classes.Load(m); ok {
			return c.(Class)
		}
		c := Classify(cfg, m)
		classes.Store(m, c)
		return c
	}

	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		class := classOf(info.FullMethod)
		if class == ClassIgnore {
			return handler(ctx, req)
		}
		ctx, col := audit.WithCollector(ctx)
		resp, err := handler(ctx, req)
		record(ctx, cfg, rec, principals, class, info.FullMethod, col, err)
		return resp, err
	}
}

func record(ctx context.Context, cfg Config, rec *hook.Recorder, principals hook.PrincipalSet,
	class Class, fullMethod string, col *audit.Collector, err error) {
	code := status.Code(err)
	md, _ := metadata.FromIncomingContext(ctx)
	denied := code == codes.PermissionDenied || code == codes.Unauthenticated
	kind := kindOf(class)
	entry := isEntry(md, principals)
	if !hook.Recorded(kind, entry, denied) {
		return
	}

	var id Identity
	if cfg.Identity != nil {
		id = safeIdentity(ctx, cfg)
	}

	var peerAddr string
	if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
		peerAddr = p.Addr.String()
	}

	rec.Record(ctx, hook.Call{
		Kind:      kind,
		Entry:     entry,
		Denied:    denied,
		Outcome:   outcomeFor(code),
		Code:      uint32(code),
		Action:    cfg.Service + "." + methodName(fullMethod),
		LimitKey:  fullMethod,
		ActorID:   id.ActorID,
		ActorType: id.ActorType,
		ClientID:  id.ClientID,
		RequestID: firstOf(md, mdIdempotencyKey, mdRequestID),
		UserAgent: firstOf(md, mdGatewayUA, mdUserAgent),
		SourceIP:  hook.SourceIP(firstOf(md, mdEnvoyExternal), md.Get(mdForwardedFor), peerAddr),
		Snapshot:  col.Snapshot(),
	})
}

// safeIdentity calls cfg.Identity, treating a panic as an unknown caller.
func safeIdentity(ctx context.Context, cfg Config) (id Identity) {
	defer func() {
		if p := recover(); p != nil {
			if cfg.Logger != nil {
				cfg.Logger.Error("audit identity callback panicked", zap.Any("panic", p))
			}
			id = Identity{}
		}
	}()
	return cfg.Identity(ctx)
}

func kindOf(c Class) hook.Kind {
	switch c {
	case ClassRead:
		return hook.KindRead
	case ClassSensitiveRead:
		return hook.KindSensitiveRead
	}
	return hook.KindWrite
}

// outcomeFor maps a gRPC status code to an event outcome: OK; DENIED for PermissionDenied and
// Unauthenticated; REFUSED for FailedPrecondition, AlreadyExists and Aborted; FAILED otherwise.
func outcomeFor(code codes.Code) auditpb.Outcome {
	switch code {
	case codes.OK:
		return auditpb.Outcome_OUTCOME_OK
	case codes.PermissionDenied, codes.Unauthenticated:
		return auditpb.Outcome_OUTCOME_DENIED
	case codes.FailedPrecondition, codes.AlreadyExists, codes.Aborted:
		return auditpb.Outcome_OUTCOME_REFUSED
	}
	return auditpb.Outcome_OUTCOME_FAILED
}

// isEntry reports whether the call carries this process's gateway marker or came straight from
// an entry principal.
func isEntry(md metadata.MD, principals hook.PrincipalSet) bool {
	for _, v := range md.Get(EntryMetadataKey) {
		if subtle.ConstantTimeCompare([]byte(v), []byte(entryMarker)) == 1 {
			return true
		}
	}
	return principals.Contains(hook.LastXFCCURI(md.Get(mdXFCC)))
}

func methodName(fullMethod string) string {
	if i := strings.LastIndex(fullMethod, "/"); i >= 0 {
		return fullMethod[i+1:]
	}
	return fullMethod
}

func firstOf(md metadata.MD, keys ...string) string {
	for _, k := range keys {
		for _, v := range md.Get(k) {
			if v != "" {
				return v
			}
		}
	}
	return ""
}
