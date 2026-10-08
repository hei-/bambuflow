package dbschema

import (
	"context"
	"fmt"
	"time"

	ingestionv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/incremental"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/publish"
)

// KV 是一份 doc_id → 值的状态（Redis 哈希），与其他样例源的同步状态同一种存法。
type KV interface {
	All(ctx context.Context) (map[string]string, error)
	Set(ctx context.Context, docID, v string) error
	Del(ctx context.Context, docID string) error
}

// Publisher 是 publish.Publisher 里用到的部分。
type Publisher interface {
	Publish(ctx context.Context, d *publish.Document, change ingestionv1.ChangeType, detectedAt time.Time) error
	Delete(ctx context.Context, docID string, detectedAt time.Time) error
}

// Syncer 是一轮同步（部署时每小时一次，技术设计文档 5.5）。
type Syncer struct {
	Config    *Config
	Catalog   Catalog
	Published KV // 上次发出的“表结构哈希|权限摘要”
	Synced    KV // 当前这版表结构第一次被发现的时间（RFC 3339），标题里的“同步于”
	Publisher Publisher
	BaseURI   string
	// 管理员确认后才执行超过删除保护阈值的删除（入库管道详设第 5 节），与其他样例源相同
	ConfirmDeletes bool
	Now            func() time.Time
}

// Stats 是一轮同步的结果。
type Stats struct {
	Views, Upserts, ACLOnly, Deletes int
	DeletesPaused                    string
}

// Run 读白名单视图的元数据，与上次发出的状态比较：表结构变了发 UPSERT，只有授权变了发 ACL_ONLY，
// 视图不见了发删除。元数据读取失败时整轮失败，不推断任何删除。
func (s *Syncer) Run(ctx context.Context) (Stats, error) {
	var st Stats
	now := s.Now().UTC()
	views, err := s.Catalog.Views(ctx, s.Config.Views)
	if err != nil {
		return st, fmt.Errorf("读取数据库元数据: %w", err)
	}
	st.Views = len(views)
	prev, err := s.Published.All(ctx)
	if err != nil {
		return st, err
	}
	synced, err := s.Synced.All(ctx)
	if err != nil {
		return st, err
	}
	byID := map[string]View{}
	current := map[string]string{}
	for _, v := range views {
		id := s.Config.DocID(v)
		d := s.Config.Document(v, s.BaseURI, now)
		byID[id], current[id] = v, d.ContentHash+"|"+d.ACLFingerprint
	}
	plan := incremental.Diff(prev, current, s.ConfirmDeletes)
	for _, id := range plan.AclOnly {
		at, err := time.Parse(time.RFC3339, synced[id])
		if err != nil {
			at = now
		}
		if err := s.Publisher.Publish(ctx, s.Config.Document(byID[id], s.BaseURI, at), ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY, now); err != nil {
			return st, fmt.Errorf("发出 %s: %w", id, err)
		}
		if err := s.Published.Set(ctx, id, current[id]); err != nil {
			return st, err
		}
		st.ACLOnly++
	}
	for _, id := range plan.Upserts {
		if err := s.Publisher.Publish(ctx, s.Config.Document(byID[id], s.BaseURI, now), ingestionv1.ChangeType_CHANGE_TYPE_UPSERT, now); err != nil {
			return st, fmt.Errorf("发出 %s: %w", id, err)
		}
		// 事件发出后才记状态：中途失败的视图下一轮重发
		if err := s.Synced.Set(ctx, id, now.Format(time.RFC3339)); err != nil {
			return st, err
		}
		if err := s.Published.Set(ctx, id, current[id]); err != nil {
			return st, err
		}
		st.Upserts++
	}
	for _, id := range plan.Deletes {
		if err := s.Publisher.Delete(ctx, id, now); err != nil {
			return st, fmt.Errorf("删除 %s: %w", id, err)
		}
		if err := s.Published.Del(ctx, id); err != nil {
			return st, err
		}
		if err := s.Synced.Del(ctx, id); err != nil {
			return st, err
		}
		st.Deletes++
	}
	if plan.DeletesPaused {
		st.DeletesPaused = plan.Reason
	}
	return st, nil
}
