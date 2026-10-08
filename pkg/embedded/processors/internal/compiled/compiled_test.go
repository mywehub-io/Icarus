package compiled

import (
	"fmt"
	"sync"
	"testing"
)

func TestRegexpIsCachedAndShared(t *testing.T) {
	a, err := Regexp(`^a+$`)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Regexp(`^a+$`)
	if a != b || !a.MatchString("aaa") {
		t.Fatal("the same pattern must give the same compiled value")
	}
	if _, err := Regexp(`(`); err == nil {
		t.Fatal("a bad pattern must fail")
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			re, err := Regexp(fmt.Sprintf("x%d", i%5))
			if err != nil || !re.MatchString(fmt.Sprintf("x%d", i%5)) {
				t.Error("concurrent compile failed")
			}
		}(i)
	}
	wg.Wait()
}

func TestCacheIsBounded(t *testing.T) {
	for i := 0; i < maxEntries+10; i++ {
		if _, err := Regexp(fmt.Sprintf("p%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	regexps.mu.RLock()
	n := len(regexps.m)
	regexps.mu.RUnlock()
	if n > maxEntries {
		t.Fatalf("cache holds %d, above %d", n, maxEntries)
	}
}

func TestLocation(t *testing.T) {
	l, err := Location("Europe/London")
	if err != nil || l.String() != "Europe/London" {
		t.Fatal(l, err)
	}
	if _, err := Location("Nowhere/Nope"); err == nil {
		t.Fatal("an unknown zone must fail")
	}
}
