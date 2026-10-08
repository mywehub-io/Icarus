package httpclient

import (
	"net/netip"
	"testing"
)

func TestRefusal(t *testing.T) {
	AllowLoopbackPorts([]uint16{8999})
	defer AllowLoopbackPorts(nil)
	for addr, want := range map[string]string{
		"127.0.0.1:19094":         "loopback",
		"127.0.0.1:8999":          "",
		"127.1.2.3:80":            "loopback",
		"[::1]:80":                "loopback",
		"[::ffff:127.0.0.1]:80":   "loopback",
		"[::ffff:127.0.0.1]:8999": "",
		"0.0.0.0:80":              "unspecified",
		"[::]:80":                 "unspecified",
		"169.254.169.254:80":      "link-local",
		"[fe80::1]:80":            "link-local",
		"224.0.0.1:80":            "link-local",
		"239.1.1.1:80":            "multicast",
		"10.0.4.17:443":           "",
		"192.168.1.10:443":        "",
		"172.16.0.1:443":          "",
		"20.90.1.1:443":           "",
	} {
		if got := refusal(netip.MustParseAddrPort(addr)); got != want {
			t.Errorf("%s: got %q, want %q", addr, got, want)
		}
	}
}
