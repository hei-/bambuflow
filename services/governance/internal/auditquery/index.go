package auditquery

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path"
	"slices"
	"strings"
	"time"
)

// 审计索引（技术设计文档 4.1 audit_index、8.3）：索引任务定期把审计桶里新送来的对象写进索引表，查询走索引，原文仍从审计桶读。
//
// 审计桶里的三类对象，键都带送达当天的日期目录（UTC），索引任务每轮只列今天与昨天的目录：
//   - gateway/<日期>/<实例>/audit.jsonl.<封存时刻>：gateway 的入口审计段，每行一条（access；受限级内容的单独访问记录为 restricted）；
//   - <服务>/changes/<日期>/<时刻>-<随机>.json：各服务的变更审计，一个对象一条（change）；
//   - evaluation/replay/<日期>/<时刻>-<随机>.jsonl：评测回放的请求，每行一条，格式同入口审计（eval，evaluation 详设 4.2）。
//
// 第一次运行时把各前缀整个补录一遍（含日期目录之前的 gateway/<实例>/ 旧布局），之后只看日期目录：
// 升级时先升 gateway 再升 governance，否则补录之后旧版 gateway 送来的段不会进索引（原文仍在审计桶里）。

// ChangeServices 是写变更审计的服务（libs/platform-go/changeaudit 与 evaluation 的 audit.py）。
var ChangeServices = []string{"governance", "indexing", "connectors", "evaluation"}

const (
	gatewayPrefix = "gateway/"
	replayPrefix  = "evaluation/replay/"
	backfillState = "backfilled_at"
	// gateway 受限级单独访问记录的 event（services/gateway/internal/audit.EventRestricted）
	restrictedEvent = "restricted_access"
)

// Bucket 是审计桶的只读访问。
type Bucket interface {
	List(ctx context.Context, prefix string) ([]string, error)
	Get(ctx context.Context, key string) ([]byte, error)
}

// Row 是索引里的一行。
type Row struct {
	TS                                        time.Time
	RecordID, Kind, Source, ObjectKey         string
	Line                                      int
	TraceID, Tenant, UserID, Action, ObjectID string
	ShownDocIDs, ContextDocIDs, ListedDocIDs  []string
}

// Cursor 是分页位置：上一页最后一行。
type Cursor struct {
	TS       time.Time `json:"t"`
	RecordID string    `json:"r"`
}

// Filter 是一次查询的条件；Kinds 不为空。
type Filter struct {
	From, To   time.Time
	Sub, DocID string
	Kinds      []string
	After      *Cursor
	Limit      int
}

// Index 是索引表的读写（生产为 PG）。
type Index interface {
	// Indexed 返回 keys 里已经索引过的对象
	Indexed(ctx context.Context, keys []string) (map[string]bool, error)
	// Add 在一个事务里写入一个对象的全部行并记下它已索引；重复写同一条记录不报错
	Add(ctx context.Context, objectKey string, rows []Row) error
	// Find 按时间先后（同一时刻按 record_id）返回符合条件的行，最多 Limit 行
	Find(ctx context.Context, f Filter) ([]Row, error)
	State(ctx context.Context, name string) (string, error)
	SetState(ctx context.Context, name, value string) error
	// Exclusive 在多副本之间互斥地执行 fn；别的副本正在执行时直接返回 false
	Exclusive(ctx context.Context, fn func(ctx context.Context) error) (bool, error)
}

// Indexer 是索引任务。
type Indexer struct {
	Bucket Bucket
	Index  Index
	Log    *slog.Logger
	// seen 是最近两天已索引的对象键，免得每轮都查库
	seen map[string]string
}

// Run 每隔 every 执行一轮，直到 ctx 结束。
func (x *Indexer) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		if n, err := x.Round(ctx, time.Now()); err != nil {
			x.Log.ErrorContext(ctx, "审计索引失败，下一轮重试", slog.Any("err", err))
		} else if n > 0 {
			x.Log.InfoContext(ctx, "已索引审计对象", slog.Int("objects", n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Round 执行一轮，返回新索引的对象数。
func (x *Indexer) Round(ctx context.Context, now time.Time) (int, error) {
	n := 0
	_, err := x.Index.Exclusive(ctx, func(ctx context.Context) error {
		done, err := x.Index.State(ctx, backfillState)
		if err != nil {
			return err
		}
		prefixes := dayPrefixes(now)
		if done == "" {
			prefixes = []string{gatewayPrefix, replayPrefix}
			for _, s := range ChangeServices {
				prefixes = append(prefixes, s+"/changes/")
			}
		}
		for _, p := range prefixes {
			k, err := x.indexPrefix(ctx, p, now)
			n += k
			if err != nil {
				return err
			}
		}
		if done == "" {
			return x.Index.SetState(ctx, backfillState, now.UTC().Format(time.RFC3339))
		}
		return nil
	})
	return n, err
}

// dayPrefixes 是今天与昨天（UTC）的日期目录。
func dayPrefixes(now time.Time) []string {
	var out []string
	for _, d := range []time.Time{now.UTC().AddDate(0, 0, -1), now.UTC()} {
		day := d.Format("2006/01/02") + "/"
		out = append(out, gatewayPrefix+day, replayPrefix+day)
		for _, s := range ChangeServices {
			out = append(out, s+"/changes/"+day)
		}
	}
	return out
}

func (x *Indexer) indexPrefix(ctx context.Context, prefix string, now time.Time) (int, error) {
	if x.seen == nil {
		x.seen = map[string]string{}
	}
	// 只记两天内的键：更早的日期目录不会再列
	yesterday := now.UTC().AddDate(0, 0, -1).Format("2006/01/02")
	for k, day := range x.seen {
		if day < yesterday {
			delete(x.seen, k)
		}
	}
	keys, err := x.Bucket.List(ctx, prefix)
	if err != nil {
		return 0, fmt.Errorf("列出 %s: %w", prefix, err)
	}
	keys = slices.DeleteFunc(keys, func(k string) bool { _, ok := x.seen[k]; return ok || kindOf(k) == "" })
	slices.Sort(keys)
	n := 0
	for chunk := range slices.Chunk(keys, 500) {
		have, err := x.Index.Indexed(ctx, chunk)
		if err != nil {
			return n, err
		}
		for _, k := range chunk {
			if !have[k] {
				body, err := x.Bucket.Get(ctx, k)
				if err != nil {
					return n, fmt.Errorf("读取 %s: %w", k, err)
				}
				if err := x.Index.Add(ctx, k, x.parse(ctx, k, body)); err != nil {
					return n, fmt.Errorf("索引 %s: %w", k, err)
				}
				n++
			}
			x.seen[k] = now.UTC().Format("2006/01/02")
		}
	}
	return n, nil
}

// kindOf 按对象键判断记录类别；不认识的键（如门禁报告）返回空。
func kindOf(key string) string {
	switch {
	case strings.HasPrefix(key, gatewayPrefix) && strings.Contains(path.Base(key), "audit.jsonl."):
		return "access"
	case strings.HasPrefix(key, replayPrefix) && strings.HasSuffix(key, ".jsonl"):
		return "eval"
	}
	for _, s := range ChangeServices {
		if strings.HasPrefix(key, s+"/changes/") && strings.HasSuffix(key, ".json") {
			return "change"
		}
	}
	return ""
}

// segmentID 是入口审计段的标识：实例与段名。段送出后本地删除失败会在另一天重送，键里的日期不同，标识相同。
func segmentID(key string) string {
	return "gateway/" + path.Base(path.Dir(key)) + "/" + path.Base(key)
}

// parse 把一个对象解析成索引行。无法解析的行跳过并告警：原文在只追加的桶里，不会因此丢失。
func (x *Indexer) parse(ctx context.Context, key string, body []byte) []Row {
	kind := kindOf(key)
	var rows []Row
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	line := 0
	for sc.Scan() {
		line++
		if len(bytes.TrimSpace(sc.Bytes())) == 0 {
			continue
		}
		row, err := parseLine(kind, key, line, sc.Bytes())
		if err != nil {
			x.Log.ErrorContext(ctx, "审计对象里有无法解析的行", slog.String("key", key), slog.Int("line", line))
			continue
		}
		rows = append(rows, row)
	}
	return rows
}

func parseLine(kind, key string, line int, raw []byte) (Row, error) {
	row := Row{Kind: kind, ObjectKey: key, Line: line}
	if kind == "change" {
		var c change
		if err := json.Unmarshal(raw, &c); err != nil || c.Time.IsZero() {
			return Row{}, fmt.Errorf("变更审计: %w", err)
		}
		row.TS, row.RecordID, row.Source = c.Time, key, strings.SplitN(key, "/", 2)[0]
		row.TraceID, row.UserID, row.Action, row.ObjectID = c.TraceID, c.Actor, c.Action, c.ObjectID
		return row, nil
	}
	var r record
	if err := json.Unmarshal(raw, &r); err != nil || r.Time.IsZero() {
		return Row{}, fmt.Errorf("入口审计: %w", err)
	}
	row.TS, row.TraceID, row.Tenant, row.UserID, row.Action = r.Time, r.TraceID, r.Tenant, r.Sub, r.Endpoint
	row.ShownDocIDs, row.ContextDocIDs, row.ListedDocIDs = docsOf(r.ShownChunkIDs), docsOf(r.ContextChunkIDs), dedupe(r.ListedDocIDs)
	if kind == "access" && r.Event == restrictedEvent {
		row.Kind = "restricted"
	}
	if kind == "eval" {
		row.Source, row.RecordID = "evaluation", fmt.Sprintf("%s:%d", key, line)
	} else {
		row.Source, row.RecordID = "gateway", fmt.Sprintf("%s:%d", segmentID(key), line)
	}
	return row, nil
}

func docsOf(chunkIDs []string) []string {
	out := make([]string, 0, len(chunkIDs))
	for _, c := range chunkIDs {
		if d := DocOfChunk(c); d != "" {
			out = append(out, d)
		}
	}
	return dedupe(out)
}

func dedupe(ids []string) []string {
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}
