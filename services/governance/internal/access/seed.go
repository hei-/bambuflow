package access

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
	"go.yaml.in/yaml/v3"
)

// SeedSource 是种子文件里的一个数据源及其授权的租户（开发与 CI 用，tools/test-upstream/sources.yaml）。
type SeedSource struct {
	SourceID              string   `yaml:"source_id"`
	Type                  string   `yaml:"type"`
	OwnerDept             string   `yaml:"owner_dept"`
	DefaultClassification int      `yaml:"default_classification"`
	Description           string   `yaml:"description"`
	Grants                []string `yaml:"grants"`
}

// LoadSeed 读种子文件。
func LoadSeed(path string) ([]SeedSource, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var f struct {
		Sources []SeedSource `yaml:"sources"`
	}
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("种子文件格式不对: %w", err)
	}
	for _, s := range f.Sources {
		if !sourceIDPattern.MatchString(s.SourceID) || s.OwnerDept == "" {
			return nil, fmt.Errorf("数据源 %q 不合法", s.SourceID)
		}
		for _, t := range s.Grants {
			if !tenantPattern.MatchString(t) {
				return nil, fmt.Errorf("租户 %q 不合法", t)
			}
		}
	}
	return f.Sources, nil
}

// Seed 写入种子：数据源按已审批写入，已存在的数据源与授权不动（不覆盖经管理接口做过的审批、撤销与取消授权）。
func (s *PG) Seed(ctx context.Context, sources []SeedSource) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		for _, src := range sources {
			tag, err := tx.Exec(ctx, `
				INSERT INTO governance.sources (source_id, type, owner_dept, default_classification, description, status, created_by,
					approved_by, approved_at, approval_basis, security_approved_by, security_approved_at, security_approval_basis)
				SELECT $1, $2, $3, $4, $5, 'active', 'seed', 'seed', now(), '开发与 CI 的种子数据', x.sec_by, x.sec_at, x.sec_basis
				FROM (SELECT CASE WHEN $4 = 4 THEN 'seed-infosec' ELSE '' END AS sec_by,
					CASE WHEN $4 = 4 THEN now() END AS sec_at,
					CASE WHEN $4 = 4 THEN '开发与 CI 的种子数据' ELSE '' END AS sec_basis) x
				ON CONFLICT (source_id) DO NOTHING`, src.SourceID, src.Type, src.OwnerDept, src.DefaultClassification, src.Description)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				continue
			}
			for _, t := range src.Grants {
				if _, err := tx.Exec(ctx, `
					INSERT INTO governance.source_grants (source_id, tenant, granted_by) VALUES ($1, $2, 'seed')
					ON CONFLICT DO NOTHING`, src.SourceID, t); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
