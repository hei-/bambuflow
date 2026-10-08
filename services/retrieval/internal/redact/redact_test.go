package redact

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSharedCases(t *testing.T) {
	raw, err := os.ReadFile("../../../../tools/test-upstream/pii_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []struct{ In, Out string } `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Cases {
		got := Mask(c.In)
		if got != c.Out {
			t.Errorf("Mask(%q) = %q，应为 %q", c.In, got, c.Out)
		}
		if Mask(got) != got {
			t.Errorf("掩码应幂等: %q", got)
		}
	}
}
