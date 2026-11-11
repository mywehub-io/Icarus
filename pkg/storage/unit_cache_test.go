package storage

import (
	"context"
	"testing"
)

func TestUnitCacheKeepsSmallBlobsForOneContext(t *testing.T) {
	ctx := WithUnitCache(context.Background())
	if WithUnitCache(ctx) != ctx {
		t.Fatal("WithUnitCache replaced an existing cache")
	}
	c := UnitCacheFrom(ctx)
	c.Put("small", []byte("abc"))
	c.Put("large", make([]byte, MaxUnitCachedBlobBytes+1))
	if b, ok := c.Get("small"); !ok || string(b) != "abc" {
		t.Fatalf("small blob: %q, %v", b, ok)
	}
	if _, ok := c.Get("large"); ok {
		t.Fatal("a blob above the bound was kept")
	}
	if UnitCacheFrom(context.Background()) != nil {
		t.Fatal("a context without a cache returned one")
	}
	var none *UnitCache
	none.Put("x", []byte("y"))
	if _, ok := none.Get("x"); ok {
		t.Fatal("a nil cache returned a hit")
	}
}
