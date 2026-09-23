package kv

import "testing"

func TestDurabilityDefaultAndValidation(t *testing.T) {
	if Durability("").Effective() != Replicated {
		t.Fatal("missing durability weakened")
	}
	for _, d := range []Durability{"", Local, Replicated} {
		if !d.Valid() {
			t.Fatal(d)
		}
	}
	if Durability("unknown").Valid() {
		t.Fatal("unknown durability accepted")
	}
}
