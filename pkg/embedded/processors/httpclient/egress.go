package httpclient

import (
	"fmt"
	"net/netip"
	"sync/atomic"
	"syscall"
)

// The HTTP Client node takes its URL from a connection or from the run's data, so whoever can
// author or trigger a workflow chooses where the host connects. The dial refuses the addresses that
// reach the host itself or its platform rather than a system the workflow is meant to call:
// loopback (the pod, its sidecars, the services beside a host-run process), link-local (cloud
// instance metadata at 169.254.169.254), unspecified (0.0.0.0 dials the local host) and multicast.
// The check runs on the resolved address of every connection, so a hostname that resolves to one of
// them, or a redirect to one, is refused too.
//
// Private ranges stay reachable: calling a system inside the customer's own network is what the
// node is for. With HTTP_PROXY or HTTPS_PROXY set, the dial is to the proxy, and the proxy decides.

// EgressError is a connection the egress policy refused.
type EgressError struct {
	Addr   netip.AddrPort
	Reason string
}

func (e *EgressError) Error() string {
	return fmt.Sprintf("blocked by SSRF egress policy: %s is a %s address", e.Addr.Addr(), e.Reason)
}

// Transient reports false (message.TransientClassifier): the refusal arrives as a failed dial,
// which the runner otherwise retries as a dependency outage, but the same URL is refused every
// time.
func (e *EgressError) Transient() bool { return false }

var loopbackPorts atomic.Pointer[map[uint16]bool]

// AllowLoopbackPorts re-permits loopback destinations on these ports, and nothing else. It exists
// for a host-run local estate, whose test doubles listen on 127.0.0.1; a deployed Elysium leaves it
// empty. Call it once, during startup. An empty list refuses every loopback port.
func AllowLoopbackPorts(ports []uint16) {
	m := make(map[uint16]bool, len(ports))
	for _, p := range ports {
		m[p] = true
	}
	loopbackPorts.Store(&m)
}

// refusal is why ap may not be dialed, or "" when it may.
func refusal(ap netip.AddrPort) string {
	a := ap.Addr().Unmap()
	switch {
	case a.IsLoopback():
		if m := loopbackPorts.Load(); m != nil && (*m)[ap.Port()] {
			return ""
		}
		return "loopback"
	case a.IsUnspecified():
		return "unspecified"
	case a.IsLinkLocalUnicast(), a.IsLinkLocalMulticast():
		return "link-local"
	case a.IsMulticast():
		return "multicast"
	}
	return ""
}

// guardDial is the dialer's Control hook: it runs after resolution, on the address about to be
// connected to.
func guardDial(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		// Control always receives a literal ip:port; anything else is refused rather than dialed.
		return fmt.Errorf("blocked by SSRF egress policy: cannot parse %q: %w", address, err)
	}
	if reason := refusal(ap); reason != "" {
		return &EgressError{Addr: ap, Reason: reason}
	}
	return nil
}
