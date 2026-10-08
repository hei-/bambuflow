package pgmigrate

import (
	"testing"
	"testing/fstest"
)

func TestStepsOrdered(t *testing.T) {
	fsys := fstest.MapFS{
		"sql/0002_b.sql": {Data: []byte("SELECT 2")},
		"sql/0001_a.sql": {Data: []byte("SELECT 1")},
		"sql/0010_c.sql": {Data: []byte("SELECT 10")},
	}
	steps, err := Steps(fsys, "sql")
	if err != nil {
		t.Fatal(err)
	}
	if steps[0].name != "0001_a.sql" || steps[2].name != "0010_c.sql" {
		t.Fatalf("应按序号排序: %+v", steps)
	}
}

func TestStepsRejectsDuplicateOrBadNames(t *testing.T) {
	for name, fsys := range map[string]fstest.MapFS{
		"序号重复": {"sql/0001_a.sql": {}, "sql/0001_b.sql": {}},
		"没有序号": {"sql/init.sql": {}},
	} {
		if _, err := Steps(fsys, "sql"); err == nil {
			t.Errorf("%s 应报错", name)
		}
	}
}
