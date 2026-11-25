package hook

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSourceIPPrecedence(t *testing.T) {
	assert.Equal(t, []byte{203, 0, 113, 7},
		SourceIP("203.0.113.7", []string{"198.51.100.1"}, "192.0.2.1:443"))
	assert.Equal(t, []byte{198, 51, 100, 2},
		SourceIP("", []string{"10.0.0.1", "198.51.100.1, 198.51.100.2"}, "192.0.2.1:443"),
		"the last element of the last x-forwarded-for value")
	assert.Equal(t, []byte{192, 0, 2, 1}, SourceIP("", nil, "192.0.2.1:443"))
	assert.Equal(t, []byte{192, 0, 2, 1}, SourceIP("not-an-ip", []string{"junk"}, "192.0.2.1:443"))
	assert.Nil(t, SourceIP("", nil, "bufconn"))
}

func TestParseIP(t *testing.T) {
	assert.Len(t, ParseIP("2001:db8::1"), 16)
	assert.Len(t, ParseIP("[2001:db8::1]:8080"), 16)
	assert.Equal(t, []byte{192, 0, 2, 1}, ParseIP("::ffff:192.0.2.1"), "a mapped address is stored as IPv4")
	assert.Equal(t, []byte{192, 0, 2, 1}, ParseIP(" 192.0.2.1 "))
	assert.Len(t, ParseIP("fe80::1%eth0"), 16)
	assert.Nil(t, ParseIP(""))
	assert.Equal(t, "192.0.2.1", IPString([]byte{192, 0, 2, 1}))
	assert.Equal(t, "", IPString([]byte{1, 2, 3}))
}

func TestLastXFCCURIAndPrincipals(t *testing.T) {
	v := []string{
		`By=spiffe://a;URI=spiffe://cluster.local/ns/x/sa/first`,
		`By=spiffe://b;Hash=abc;URI=spiffe://cluster.local/ns/x/sa/old,By=spiffe://c;URI="spiffe://cluster.local/ns/istio/sa/gateway"`,
	}
	assert.Equal(t, "spiffe://cluster.local/ns/istio/sa/gateway", LastXFCCURI(v))
	assert.Equal(t, "", LastXFCCURI(nil))
	assert.Equal(t, "", LastXFCCURI([]string{"By=x;Hash=y"}))

	set := NewPrincipalSet([]string{"cluster.local/ns/istio/sa/gateway", " ", ""})
	assert.True(t, set.Contains("spiffe://cluster.local/ns/istio/sa/gateway"))
	assert.False(t, set.Contains("spiffe://cluster.local/ns/x/sa/other"))
	assert.False(t, set.Contains(""))
	assert.False(t, PrincipalSet(nil).Contains("spiffe://x"))
}

func TestMergeAttrsKeepsExtras(t *testing.T) {
	base := map[string]string{"a": "1", "b": "2", "c": "3", "d": "4", "e": "5", "f": "6"}
	out, cut := MergeAttrs(base, map[string]string{"claimed_client_id": "x", "suppressed": "3"})
	assert.True(t, cut)
	assert.Len(t, out, 6)
	assert.Equal(t, "x", out["claimed_client_id"])
	assert.Equal(t, "3", out["suppressed"])
	assert.Contains(t, out, "a")
	assert.NotContains(t, out, "f")

	out, cut = MergeAttrs(nil, nil)
	assert.Nil(t, out)
	assert.False(t, cut)
}

func TestLimiterWindowAndSweep(t *testing.T) {
	now := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	l := NewLimiter(time.Minute, func() time.Time { return now })

	ok, n := l.Allow("k")
	assert.True(t, ok)
	assert.Zero(t, n)
	for i := 0; i < 3; i++ {
		ok, _ = l.Allow("k")
		assert.False(t, ok)
	}
	ok, _ = l.Allow("other")
	assert.True(t, ok, "keys are limited separately")

	now = now.Add(time.Minute)
	ok, n = l.Allow("k")
	assert.True(t, ok)
	assert.Equal(t, uint64(3), n, "the next allowed event carries the suppressed count")

	now = now.Add(2 * time.Minute)
	l.Allow("fresh")
	assert.NotContains(t, l.entries, "other", "an idle key with nothing suppressed is forgotten")
}
