package records

import (
	"context"
	"fmt"
	"time"

	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/dbschema"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/incremental"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

// Rows 是行数据端口：读一张表配置里列出的列与权限列，最多 limit 行；超过时返回 ErrTooManyRows。
type Rows interface {
	Rows(ctx context.Context, t Table, limit int) ([]Row, error)
}

// ErrTooManyRows：表的行数超过 max_rows。
var ErrTooManyRows = fmt.Errorf("行数超过 max_rows")

// Syncer 是一轮同步。
type Syncer struct {
	Config    *Config
	Rows      Rows
	Published dbschema.KV // 上次发出的“内容摘要|权限摘要”
	Publisher dbschema.Publisher
	BaseURI   string
	// 管理员确认后才执行超过删除保护阈值的删除（入库管道详设第 5 节）
	ConfirmDeletes bool
	Now            func() time.Time
}

// Stats 是一轮同步的结果。
type Stats struct {
	Rows, Upserts, ACLOnly, Deletes int
	// 权限表达不了、没有入库的行
	WithoutACL    int
	DeletesPaused string
}

// Run 读全部表的行，与上次发出的比较后发出变化。任一表读取失败时整轮失败，不推断任何删除。
func (s *Syncer) Run(ctx context.Context) (Stats, error) {
	var st Stats
	now := s.Now().UTC()
	docs := map[string]*publish.Document{}
	current := map[string]string{}
	for _, t := range s.Config.Tables {
		rows, err := s.Rows.Rows(ctx, t, t.MaxRows)
		if err != nil {
			return st, fmt.Errorf("读取 %s: %w", t.View, err)
		}
		for _, r := range rows {
			d, ok, err := s.Config.Document(t, r, s.BaseURI, now)
			if err != nil {
				return st, fmt.Errorf("%s: %w", t.View, err)
			}
			if !ok {
				st.WithoutACL++
				continue
			}
			if _, dup := docs[d.DocID]; dup {
				return st, fmt.Errorf("%s 的主键 %s 重复", t.View, d.DocID)
			}
			docs[d.DocID], current[d.DocID] = d, d.ContentHash+"|"+d.ACLFingerprint
			st.Rows++
		}
	}
	prev, err := s.Published.All(ctx)
	if err != nil {
		return st, err
	}
	plan := incremental.Diff(prev, current, s.ConfirmDeletes)
	emit := func(ids []string, change ingestionv1.ChangeType, n *int) error {
		for _, id := range ids {
			if err := s.Publisher.Publish(ctx, docs[id], change, now); err != nil {
				return fmt.Errorf("发出 %s: %w", id, err)
			}
			// 事件发出后才记状态：中途失败的行下一轮重发
			if err := s.Published.Set(ctx, id, current[id]); err != nil {
				return err
			}
			*n++
		}
		return nil
	}
	if err := emit(plan.AclOnly, ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY, &st.ACLOnly); err != nil {
		return st, err
	}
	if err := emit(plan.Upserts, ingestionv1.ChangeType_CHANGE_TYPE_UPSERT, &st.Upserts); err != nil {
		return st, err
	}
	for _, id := range plan.Deletes {
		if err := s.Publisher.Delete(ctx, id, now); err != nil {
			return st, fmt.Errorf("删除 %s: %w", id, err)
		}
		if err := s.Published.Del(ctx, id); err != nil {
			return st, err
		}
		st.Deletes++
	}
	if plan.DeletesPaused {
		st.DeletesPaused = plan.Reason
	}
	return st, nil
}
