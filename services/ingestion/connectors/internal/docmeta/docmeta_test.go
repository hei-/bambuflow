package docmeta

import "testing"

const base = "id: loan\ntitle: 个人消费贷款管理办法\nowner: alice\nclassification: internal\nacl: [group:credit-l2]\nupdated_at: 2025-03-01T00:00:00Z\n"

func TestSourceFieldsArePassedThrough(t *testing.T) {
	m, err := Decode([]byte(base + "doc_type: 管理办法\ndept: 信贷管理部\ntopics: [个人贷款]\neffective_date: 2025-04-01\nstatus: effective\n"))
	if err != nil {
		t.Fatal(err)
	}
	if m.Source == nil || m.Source.DocType != "管理办法" || m.Source.EffectiveDate != "2025-04-01" || len(m.Source.Topics) != 1 {
		t.Fatalf("源端字段: %+v", m.Source)
	}
	m, err = Decode([]byte(base))
	if err != nil || m.Source != nil {
		t.Fatalf("没有源端字段时应为 nil: %+v %v", m.Source, err)
	}
}

func TestSourceFieldsAreValidated(t *testing.T) {
	for _, extra := range []string{"effective_date: 2025/04/01\n", "status: 作废\n", "doc_typ: 制度\n"} {
		if _, err := Decode([]byte(base + extra)); err == nil {
			t.Errorf("%q 应当校验失败", extra)
		}
	}
}

func TestSourceFingerprintChangesWithFields(t *testing.T) {
	a, _ := Decode([]byte(base + "status: effective\n"))
	b, _ := Decode([]byte(base + "status: repealed\n"))
	c, _ := Decode([]byte(base))
	if a.SourceFingerprint == "" || a.SourceFingerprint == b.SourceFingerprint || c.SourceFingerprint != "" {
		t.Fatalf("源端字段摘要: %q %q %q", a.SourceFingerprint, b.SourceFingerprint, c.SourceFingerprint)
	}
}
