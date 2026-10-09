// Package holds 保存删除保护拦下的待删批次（入库管道详设第 5 节），存在 Redis 哈希 bf:connector:<source_id>:holds
// （hold_id → JSON）。
//
// 一个数据源同时最多一批未结束的（pending、awaiting_second、confirmed）。同步时：先执行已确认的批次
// （只删其中源端仍不存在的），再把本轮仍被拦下的待删清单记成一批；与未结束的那批相同就不动，
// 不同就把旧的标为 superseded、另起一批；与最近丢弃的那批相同则不再拦截成新批次（删除继续暂停）。
package holds

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

// 批次状态
const (
	Pending        = "pending"
	AwaitingSecond = "awaiting_second"
	Confirmed      = "confirmed"
	Applied        = "applied"
	Discarded      = "discarded"
	Superseded     = "superseded"
)

// 已结束的批次只保留最近这么多，更早的删掉
const keepClosed = 20

var (
	ErrNotFound     = errors.New("批次不存在")
	ErrState        = errors.New("批次的当前状态不允许这个操作")
	ErrSelfApproval = errors.New("不能复核自己确认的批次")
)

// Hold 是一批被拦下的待删文档。
type Hold struct {
	ID          string    `json:"hold_id"`
	DocIDs      []string  `json:"doc_ids"`
	SyncedDocs  int       `json:"synced_docs"`
	DetectedAt  time.Time `json:"detected_at"`
	Status      string    `json:"status"`
	ConfirmedBy string    `json:"confirmed_by,omitempty"`
	ApprovedBy  string    `json:"approved_by,omitempty"`
	DiscardedBy string    `json:"discarded_by,omitempty"`
	Reason      string    `json:"reason,omitempty"`
	AppliedDocs int       `json:"applied_docs"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (h Hold) open() bool {
	return h.Status == Pending || h.Status == AwaitingSecond || h.Status == Confirmed
}

type Store struct {
	Client *redis.Client
}

func key(sourceID string) string { return "bf:connector:" + sourceID + ":holds" }

// List 返回全部批次，最近的在前。
func (s Store) List(ctx context.Context, sourceID string) ([]Hold, error) {
	return list(ctx, s.Client, sourceID)
}

func list(ctx context.Context, c redis.Cmdable, sourceID string) ([]Hold, error) {
	raw, err := c.HGetAll(ctx, key(sourceID)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Hold, 0, len(raw))
	for _, v := range raw {
		var h Hold
		if err := json.Unmarshal([]byte(v), &h); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].DetectedAt.Equal(out[j].DetectedAt) {
			return out[i].DetectedAt.After(out[j].DetectedAt)
		}
		return out[i].ID > out[j].ID
	})
	return out, nil
}

// update 在 WATCH 事务里读出全部批次、交给 fn 修改后写回；并发修改时重试，最多 5 次。
func (s Store) update(ctx context.Context, sourceID string, fn func(hs []Hold) ([]Hold, error)) error {
	k := key(sourceID)
	for range 5 {
		err := s.Client.Watch(ctx, func(tx *redis.Tx) error {
			hs, err := list(ctx, tx, sourceID)
			if err != nil {
				return err
			}
			changed, err := fn(hs)
			if err != nil || len(changed) == 0 {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
				for _, h := range changed {
					b, err := json.Marshal(h)
					if err != nil {
						return err
					}
					p.HSet(ctx, k, h.ID, b)
				}
				for _, id := range prune(hs, changed) {
					p.HDel(ctx, k, id)
				}
				return nil
			})
			return err
		}, k)
		if !errors.Is(err, redis.TxFailedErr) {
			return err
		}
	}
	return errors.New("批次被并发修改，请重试")
}

// prune 返回要删掉的已结束批次：按修改后的状态，已结束的只保留最近 keepClosed 批。
func prune(hs, changed []Hold) []string {
	byID := map[string]Hold{}
	for _, h := range hs {
		byID[h.ID] = h
	}
	for _, h := range changed {
		byID[h.ID] = h
	}
	var closed []Hold
	for _, h := range byID {
		if !h.open() {
			closed = append(closed, h)
		}
	}
	sort.Slice(closed, func(i, j int) bool { return closed[i].DetectedAt.After(closed[j].DetectedAt) })
	var out []string
	for i := keepClosed; i < len(closed); i++ {
		out = append(out, closed[i].ID)
	}
	return out
}

// Confirmed 返回待执行的已确认批次，没有时为 nil。
func (s Store) Confirmed(ctx context.Context, sourceID string) (*Hold, error) {
	hs, err := s.List(ctx, sourceID)
	if err != nil {
		return nil, err
	}
	for _, h := range hs {
		if h.Status == Confirmed {
			return &h, nil
		}
	}
	return nil, nil
}

// Observe 记录本轮被拦下的待删清单（已排序），返回对应的批次。
func (s Store) Observe(ctx context.Context, sourceID string, docIDs []string, synced int, now time.Time) (Hold, error) {
	var out Hold
	err := s.update(ctx, sourceID, func(hs []Hold) ([]Hold, error) {
		var changed []Hold
		for _, h := range hs {
			if h.open() {
				if slices.Equal(h.DocIDs, docIDs) {
					out = h
					return nil, nil
				}
				h.Status, h.UpdatedAt = Superseded, now
				changed = append(changed, h)
			}
		}
		for _, h := range hs {
			if h.Status == Discarded {
				// 只看最近丢弃的一批：清单没变就不再拦成新批次
				if slices.Equal(h.DocIDs, docIDs) {
					out = h
					return changed, nil
				}
				break
			}
		}
		out = Hold{ID: newID(), DocIDs: docIDs, SyncedDocs: synced, DetectedAt: now, Status: Pending, UpdatedAt: now}
		return append(changed, out), nil
	})
	return out, err
}

// Confirm 是数据属主确认：第一位确认后待复核，第二位（不同的人）确认后批次生效，下一轮同步执行。
func (s Store) Confirm(ctx context.Context, sourceID, holdID, who, reason string, now time.Time) (Hold, error) {
	return s.change(ctx, sourceID, holdID, func(h *Hold) error {
		switch h.Status {
		case Pending:
			h.Status, h.ConfirmedBy, h.Reason = AwaitingSecond, who, reason
		case AwaitingSecond:
			if h.ConfirmedBy == who {
				return ErrSelfApproval
			}
			h.Status, h.ApprovedBy = Confirmed, who
		default:
			return ErrState
		}
		h.UpdatedAt = now
		return nil
	})
}

// Discard 丢弃一批：不删。执行之前任何一位数据属主都能丢弃（不删除是更保守的一方，不需要复核）。
func (s Store) Discard(ctx context.Context, sourceID, holdID, who, reason string, now time.Time) (Hold, error) {
	return s.change(ctx, sourceID, holdID, func(h *Hold) error {
		if !h.open() {
			return ErrState
		}
		h.Status, h.DiscardedBy, h.Reason, h.UpdatedAt = Discarded, who, reason, now
		return nil
	})
}

// MarkApplied 记录已确认批次的执行结果。
func (s Store) MarkApplied(ctx context.Context, sourceID, holdID string, applied int, now time.Time) error {
	_, err := s.change(ctx, sourceID, holdID, func(h *Hold) error {
		if h.Status != Confirmed {
			return ErrState
		}
		h.Status, h.AppliedDocs, h.UpdatedAt = Applied, applied, now
		return nil
	})
	return err
}

func (s Store) change(ctx context.Context, sourceID, holdID string, fn func(*Hold) error) (Hold, error) {
	var out Hold
	err := s.update(ctx, sourceID, func(hs []Hold) ([]Hold, error) {
		for _, h := range hs {
			if h.ID == holdID {
				if err := fn(&h); err != nil {
					return nil, err
				}
				out = h
				return []Hold{h}, nil
			}
		}
		return nil, ErrNotFound
	})
	return out, err
}

func newID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "hold_" + hex.EncodeToString(b)
}
