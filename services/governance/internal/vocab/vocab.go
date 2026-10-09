// Package vocab 管理受控词表（governance 详设 6.1）并生成下发给各服务的快照。
package vocab

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.yaml.in/yaml/v3"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
)

// Names 是三套词表的名字。
var Names = []string{"doc_type", "dept", "topic"}

type Term struct {
	Vocabulary string   `json:"vocabulary" yaml:"-"`
	Code       string   `json:"code" yaml:"code"`
	Name       string   `json:"name" yaml:"name"`
	Synonyms   []string `json:"synonyms" yaml:"synonyms"`
	Parent     string   `json:"parent" yaml:"parent"`
	Status     string   `json:"status" yaml:"status"`
	ReplacedBy string   `json:"replaced_by" yaml:"replaced_by"`
	Owner      string   `json:"owner" yaml:"owner"`
}

// LoadFile 读取词表文件（键是词表名，值是词条列表）并校验：编码唯一、上级与替代编码存在、停用必须有替代。
func LoadFile(path string) ([]Term, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var raw map[string][]Term
	if err := yaml.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var out []Term
	for name, terms := range raw {
		if !slices.Contains(Names, name) {
			return nil, fmt.Errorf("未知词表 %q", name)
		}
		for _, t := range terms {
			t.Vocabulary = name
			if t.Status == "" {
				t.Status = "active"
			}
			out = append(out, t)
		}
	}
	return out, Validate(out)
}

func Validate(terms []Term) error {
	codes := map[string]bool{}
	for _, t := range terms {
		k := t.Vocabulary + "/" + t.Code
		if t.Code == "" || t.Name == "" || codes[k] {
			return fmt.Errorf("词条 %s 缺编码或名称，或编码重复", k)
		}
		codes[k] = true
	}
	var errs []error
	for _, t := range terms {
		switch {
		case t.Status != "active" && t.Status != "deprecated":
			errs = append(errs, fmt.Errorf("%s 的状态 %q 不合法", t.Code, t.Status))
		case t.Status == "deprecated" && !codes[t.Vocabulary+"/"+t.ReplacedBy]:
			errs = append(errs, fmt.Errorf("停用的 %s 必须指定存在的替代编码", t.Code))
		case t.Parent != "" && !codes[t.Vocabulary+"/"+t.Parent]:
			errs = append(errs, fmt.Errorf("%s 的上级 %s 不存在", t.Code, t.Parent))
		}
	}
	return errors.Join(errs...)
}

type Store struct{ Pool *pgxpool.Pool }

// Upsert 写入词条：新增或更新名称、同义词等；不删除（编码只能停用）。
// 已停用的编码不能重新启用：它可能已被替代编码接管，复活会让同一含义有两个编码。
// beforeCommit 不为空时在同一事务里执行（写变更审计）：审计写不进去就不改词表。
func (s Store) Upsert(ctx context.Context, terms []Term, beforeCommit func() error) error {
	return pgx.BeginFunc(ctx, s.Pool, func(tx pgx.Tx) error {
		for _, t := range terms {
			tag, err := tx.Exec(ctx, `
				INSERT INTO governance.vocabulary_terms AS v (vocabulary, code, name, synonyms, parent, status, replaced_by, owner)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
				ON CONFLICT (vocabulary, code) DO UPDATE
				SET name = $3, synonyms = $4, parent = $5, status = $6, replaced_by = $7, owner = $8, updated_at = now()
				WHERE NOT (v.status = 'deprecated' AND $6 = 'active')`,
				t.Vocabulary, t.Code, t.Name, nonNil(t.Synonyms), t.Parent, t.Status, t.ReplacedBy, t.Owner)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return fmt.Errorf("%w：%s/%s", ErrReactivate, t.Vocabulary, t.Code)
			}
		}
		if beforeCommit != nil {
			return beforeCommit()
		}
		return nil
	})
}

// ErrReactivate：已停用的编码不能重新启用。
var ErrReactivate = errors.New("已停用的编码不能重新启用")

// Snapshot 读出全部词条，并以内容摘要作为快照版本：内容不变则版本不变。
func (s Store) Snapshot(ctx context.Context) (string, []Term, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT vocabulary, code, name, synonyms, parent, status, replaced_by, owner
		FROM governance.vocabulary_terms ORDER BY vocabulary, code`)
	if err != nil {
		return "", nil, err
	}
	terms, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Term, error) {
		var t Term
		err := r.Scan(&t.Vocabulary, &t.Code, &t.Name, &t.Synonyms, &t.Parent, &t.Status, &t.ReplacedBy, &t.Owner)
		return t, err
	})
	if err != nil {
		return "", nil, err
	}
	return Version(terms), terms, nil
}

// Version 是词条内容（已按词表、编码排序）的摘要。
func Version(terms []Term) string {
	b, _ := json.Marshal(terms)
	sum := sha256.Sum256(b)
	return "vocab-" + hex.EncodeToString(sum[:])[:16]
}

func nonNil(xs []string) []string {
	if xs == nil {
		return []string{}
	}
	return xs
}

// SaveAck 记下实例最近一次的加载结果。
func (s Store) SaveAck(ctx context.Context, a *configv1.AckRequest) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO governance.config_acks (service, instance_id, snapshot_version, loaded, reason)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (service, instance_id) DO UPDATE SET snapshot_version = EXCLUDED.snapshot_version,
			loaded = EXCLUDED.loaded, reason = EXCLUDED.reason, acked_at = now()`,
		a.GetService(), a.GetInstanceId(), a.GetSnapshotVersion(), a.GetLoaded(), a.GetReason())
	return err
}
