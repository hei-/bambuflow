// Package syncstatus 记录每个数据源的同步结果与暂停状态（入库管道详设 14.4），存在 Redis 哈希
// bf:connector:<source_id>:sync：last 是最近一轮，last_success 是最近一次成功，last_reconcile 是最近一次对账，
// pause 是暂停记录，revoked_at 是撤销时间（internal/revoke 写入），polling 是常驻清单轮询的当前间隔（internal/poll）。
// 同步命令每轮写一次；管理服务（internal/admin）读它，并写暂停与恢复。
package syncstatus

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
)

// 一轮同步的结果
const (
	Succeeded     = "succeeded"
	Failed        = "failed"
	SkippedPaused = "skipped_paused"
	// 数据源已撤销（governance 详设 6.2）：之后的每轮同步都跳过，不能恢复
	SkippedRevoked = "skipped_revoked"
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
	// 清单里没能读出、本轮跳过的文档（最多记 20 项“名称：原因”）；有跳过时这一轮不推断删除
	Skipped *Skipped `json:"skipped,omitempty"`
	// 对账（reconcile-*）才有
	Reconciliation *Reconciliation `json:"reconciliation,omitempty"`
}

// Skipped 是一轮同步跳过的文档。
type Skipped struct {
	Count int      `json:"count"`
	Items []string `json:"items"`
}

// SkippedOf 汇总跳过的文档。
func SkippedOf(xs []docmeta.Skipped) *Skipped {
	out := &Skipped{Count: len(xs)}
	for _, x := range xs[:min(len(xs), 20)] {
		out.Items = append(out.Items, truncate(x.Name+"："+x.Reason))
	}
	return out
}

// Reconciliation 是一次对账的计数（internal/reconcile.Counts）。
type Reconciliation struct {
	Missing            int `json:"missing"`
	ContentMismatch    int `json:"content_mismatch"`
	ACLMismatch        int `json:"acl_mismatch"`
	Orphaned           int `json:"orphaned"`
	TombstonedInSource int `json:"tombstoned_in_source"`
	Indexed            int `json:"indexed"`
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

// Polling 是常驻清单轮询的状态：当前间隔与下一轮的时间。每轮结束时写，也是轮询进程还活着的心跳。
type Polling struct {
	IntervalSeconds int64     `json:"interval_seconds"`
	NextRoundAt     time.Time `json:"next_round_at"`
}

// pollingStale：过了预定的下一轮这么久还没有新的记录，就认为轮询进程已经不在了（一轮最长约 1 小时）
const pollingStale = time.Hour + 15*time.Minute

// Alive 判断轮询进程是否还在。
func (p *Polling) Alive(now time.Time) bool {
	return p != nil && now.Before(p.NextRoundAt.Add(pollingStale))
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
		fields = append(fields, "last_success", b, "consecutive_failures", 0)
	}
	if r.Reconciliation != nil {
		fields = append(fields, "last_reconcile", b)
	}
	if err := s.Client.HSet(ctx, key(sourceID), fields...).Err(); err != nil {
		return err
	}
	if r.Result == Failed {
		return s.Client.HIncrBy(ctx, key(sourceID), "consecutive_failures", 1).Err()
	}
	return nil
}

// Health 是告警用的同步健康度（入库管道详设 11、13 节：连接器连续失败）。
type Health struct {
	SourceID            string
	ConsecutiveFailures int64
	// 最近一次成功结束的时间；没成功过时为零值
	LastSuccess time.Time
}

// Healths 列出所有有同步记录的数据源的健康度。数据源只有几十到几百个，按键名前缀扫一遍。
func (s Store) Healths(ctx context.Context) ([]Health, error) {
	var out []Health
	iter := s.Client.Scan(ctx, 0, "bf:connector:*:sync", 100).Iterator()
	for iter.Next(ctx) {
		k := iter.Val()
		vals, err := s.Client.HMGet(ctx, k, "consecutive_failures", "last_success").Result()
		if err != nil {
			return nil, err
		}
		h := Health{SourceID: strings.TrimSuffix(strings.TrimPrefix(k, "bf:connector:"), ":sync")}
		if v, ok := vals[0].(string); ok {
			h.ConsecutiveFailures, _ = strconv.ParseInt(v, 10, 64)
		}
		if v, ok := vals[1].(string); ok {
			var r Run
			if json.Unmarshal([]byte(v), &r) == nil {
				h.LastSuccess = r.FinishedAt
			}
		}
		out = append(out, h)
	}
	return out, iter.Err()
}

// Status 是一个数据源的同步状态；从没同步过、也没暂停过时 Found 为 false。
type Status struct {
	Found       bool
	Last        *Run
	LastSuccess *Run
	// 最近一次完成比对的对账（比对之后的同步失败也算）
	LastReconcile *Run
	Pause         *Pause
	RevokedAt     *time.Time
	Polling       *Polling
}

func (s Store) Get(ctx context.Context, sourceID string) (Status, error) {
	h, err := s.Client.HGetAll(ctx, key(sourceID)).Result()
	if err != nil {
		return Status{}, err
	}
	var st Status
	for field, dst := range map[string]any{"last": &st.Last, "last_success": &st.LastSuccess, "last_reconcile": &st.LastReconcile,
		"pause": &st.Pause, "revoked_at": &st.RevokedAt, "polling": &st.Polling} {
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

// SetRevoked 记下数据源已撤销；已经记过时保留最早的时间。
func (s Store) SetRevoked(ctx context.Context, sourceID string, at time.Time) error {
	b, err := json.Marshal(at)
	if err != nil {
		return err
	}
	return s.Client.HSetNX(ctx, key(sourceID), "revoked_at", b).Err()
}

// Revoked 返回数据源是否已撤销。
func (s Store) Revoked(ctx context.Context, sourceID string) (bool, error) {
	return s.Client.HExists(ctx, key(sourceID), "revoked_at").Result()
}

// SetPolling 记下常驻清单轮询的当前间隔与下一轮的时间。
func (s Store) SetPolling(ctx context.Context, sourceID string, p Polling) error {
	b, err := json.Marshal(p)
	if err != nil {
		return err
	}
	return s.Client.HSet(ctx, key(sourceID), "polling", b).Err()
}
