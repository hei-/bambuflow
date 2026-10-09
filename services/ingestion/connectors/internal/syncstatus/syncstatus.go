// Package syncstatus 记录每个数据源的同步结果与暂停状态（入库管道详设 14.4），存在 Redis 哈希
// bf:connector:<source_id>:sync：last 是最近一轮，last_success 是最近一次成功，pause 是暂停记录。
// 同步命令每轮写一次；管理服务（internal/admin）读它，并写暂停与恢复。
package syncstatus

import (
	"context"
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

// 一轮同步的结果
const (
	Succeeded     = "succeeded"
	Failed        = "failed"
	SkippedPaused = "skipped_paused"
)

const errorLimit = 200

// Run 是一轮同步的结果。
type Run struct {
	Command             string    `json:"command"`
	StartedAt           time.Time `json:"started_at"`
	FinishedAt          time.Time `json:"finished_at"`
	Result              string    `json:"result"`
	Error               string    `json:"error,omitempty"`
	Documents           int       `json:"documents"`
	Upserts             int       `json:"upserts"`
	ACLOnly             int       `json:"acl_only"`
	Deletes             int       `json:"deletes"`
	DeletesPaused       bool      `json:"deletes_paused"`
	DeletesPausedReason string    `json:"deletes_paused_reason,omitempty"`
	Refetched           int       `json:"refetched"`
}

// Finish 按 err 填结果；错误摘要截断到 200 字。
func (r *Run) Finish(err error, at time.Time) {
	r.FinishedAt, r.Result = at, Succeeded
	if err != nil {
		r.Result, r.Error = Failed, truncate(err.Error())
	}
}

func truncate(s string) string {
	if utf8.RuneCountInString(s) <= errorLimit {
		return s
	}
	return string([]rune(s)[:errorLimit])
}

// Pause 是暂停记录。
type Pause struct {
	By     string    `json:"by"`
	At     time.Time `json:"at"`
	Reason string    `json:"reason"`
}

// Store 读写同步状态。
type Store struct {
	Client redis.Cmdable
}

func key(sourceID string) string { return "bf:connector:" + sourceID + ":sync" }

// Record 写一轮同步的结果。
func (s Store) Record(ctx context.Context, sourceID string, r Run) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	fields := []any{"last", b}
	if r.Result == Succeeded {
		fields = append(fields, "last_success", b)
	}
	return s.Client.HSet(ctx, key(sourceID), fields...).Err()
}

// Status 是一个数据源的同步状态；从没同步过、也没暂停过时 Found 为 false。
type Status struct {
	Found       bool
	Last        *Run
	LastSuccess *Run
	Pause       *Pause
}

func (s Store) Get(ctx context.Context, sourceID string) (Status, error) {
	h, err := s.Client.HGetAll(ctx, key(sourceID)).Result()
	if err != nil {
		return Status{}, err
	}
	var st Status
	for field, dst := range map[string]any{"last": &st.Last, "last_success": &st.LastSuccess, "pause": &st.Pause} {
		if v, ok := h[field]; ok {
			if err := json.Unmarshal([]byte(v), dst); err != nil {
				return Status{}, err
			}
			st.Found = true
		}
	}
	return st, nil
}

// Paused 返回数据源是否暂停。
func (s Store) Paused(ctx context.Context, sourceID string) (bool, error) {
	n, err := s.Client.HExists(ctx, key(sourceID), "pause").Result()
	return n, err
}

// SetPause 暂停数据源；已经暂停时不覆盖，返回原来的记录与 changed = false。
func (s Store) SetPause(ctx context.Context, sourceID string, p Pause) (Pause, bool, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return Pause{}, false, err
	}
	ok, err := s.Client.HSetNX(ctx, key(sourceID), "pause", b).Result()
	if err != nil || ok {
		return p, ok, err
	}
	st, err := s.Get(ctx, sourceID)
	if err != nil {
		return Pause{}, false, err
	}
	if st.Pause == nil {
		// 读的间隙被恢复了：按没暂停成功处理，让调用方重试
		return Pause{}, false, errors.New("暂停状态刚被修改，请重试")
	}
	return *st.Pause, false, nil
}

// Resume 恢复数据源，返回之前是否暂停。
func (s Store) Resume(ctx context.Context, sourceID string) (bool, error) {
	n, err := s.Client.HDel(ctx, key(sourceID), "pause").Result()
	return n > 0, err
}
