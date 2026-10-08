package logs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	ingestionv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/docmeta"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/publish"
)

// 一轮最多读这么多条：一次性任务也不能无限占用内存，剩下的下一轮再读
const maxPerRun = 50000

// Publisher 是 publish.Publisher 里用到的部分。
type Publisher interface {
	Publish(ctx context.Context, d *publish.Document, change ingestionv1.ChangeType, detectedAt time.Time) error
	Delete(ctx context.Context, docID string, detectedAt time.Time) error
}

// Store 保存各文档已收到的消息（对象存储，与文档包放在一起）。
type Store interface {
	Load(ctx context.Context, docID string) (*DayState, error)
	Save(ctx context.Context, docID string, s *DayState) error
	Delete(ctx context.Context, docID string) error
	List(ctx context.Context) ([]string, error)
}

// Published 记录每份文档上次发出的“内容摘要|权限摘要”，与其他样例源的同步状态同一格式（Redis 哈希）。
type Published interface {
	All(ctx context.Context) (map[string]string, error)
	Set(ctx context.Context, docID, fp string) error
	Del(ctx context.Context, docID string) error
}

// Syncer 是一轮同步。
type Syncer struct {
	Config    *Config
	Source    Subscriber
	Store     Store
	Published Published
	Publisher Publisher
	BaseURI   string
	Log       *slog.Logger
	Now       func() time.Time
}

// Stats 是一轮同步的结果。
type Stats struct {
	Read, Accepted, Duplicates, Upserts, ACLOnly, Expired int
}

// Run 读完主题里当前可读的消息，更新受影响的文档，再处理到期删除与权限变化。
func (s *Syncer) Run(ctx context.Context) (Stats, error) {
	var st Stats
	now := s.Now()
	var msgs []Message
	for len(msgs) < maxPerRun {
		batch, err := s.Source.Read(ctx, min(1000, maxPerRun-len(msgs)))
		if err != nil {
			return st, fmt.Errorf("读取日志主题: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		msgs = append(msgs, batch...)
	}
	st.Read = len(msgs)
	published, err := s.Published.All(ctx)
	if err != nil {
		return st, fmt.Errorf("读取同步状态: %w", err)
	}
	for id, entries := range s.Config.accepted(msgs, now) {
		st.Accepted += len(entries)
		state, err := s.Store.Load(ctx, id)
		if err != nil {
			return st, fmt.Errorf("读取 %s 的状态: %w", id, err)
		}
		st.Duplicates += len(entries) - state.add(entries)
		if err := s.Store.Save(ctx, id, state); err != nil {
			return st, fmt.Errorf("保存 %s 的状态: %w", id, err)
		}
	}
	ids, err := s.Store.List(ctx)
	if err != nil {
		return st, fmt.Errorf("列出日志文档: %w", err)
	}
	cutoff := s.Config.cutoff(now)
	for _, id := range ids {
		day, service, ok := dayOf(id)
		if !ok {
			continue
		}
		if day.Before(cutoff) || s.Config.Services[service].Groups == nil {
			// 保留期到了（或服务已从配置里去掉）：整天写删除标记（技术设计文档 5.5“按时间分区到期”）。
			// 这是业务上的正常到期，不走删除保护的阈值（入库管道详设第 5 节）
			if err := s.expire(ctx, id, published, now); err != nil {
				return st, err
			}
			st.Expired++
			continue
		}
		change, err := s.publish(ctx, id, service, day, published[id], now)
		if err != nil {
			return st, err
		}
		switch change {
		case ingestionv1.ChangeType_CHANGE_TYPE_UPSERT:
			st.Upserts++
		case ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY:
			st.ACLOnly++
		}
	}
	// 文档都发出、状态都保存之后才确认消息：中途失败的下一轮重读，按偏移量去重，不会重复计入
	if len(msgs) > 0 {
		all := make([]string, len(msgs))
		for i, m := range msgs {
			all[i] = m.ID
		}
		if err := s.Source.Ack(ctx, all); err != nil {
			return st, fmt.Errorf("确认消息: %w", err)
		}
	}
	return st, nil
}

// publish 按正文与权限的变化发事件：正文变了发 UPSERT，只有权限变了（服务归属调整）发 ACL_ONLY，都没变不发。
func (s *Syncer) publish(ctx context.Context, id, service string, day time.Time, prev string, now time.Time) (ingestionv1.ChangeType, error) {
	state, err := s.Store.Load(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("读取 %s 的状态: %w", id, err)
	}
	if len(state.Entries) == 0 {
		return 0, nil
	}
	body := Render(service, day, state.Entries)
	sum := sha256.Sum256([]byte(body))
	hash := hex.EncodeToString(sum[:])
	acl := s.Config.aclFingerprint(service)
	fp := hash + "|" + acl
	if fp == prev {
		return 0, nil
	}
	change := ingestionv1.ChangeType_CHANGE_TYPE_UPSERT
	if oldHash, _, ok := strings.Cut(prev, "|"); ok && oldHash == hash {
		change = ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY
	}
	d := &publish.Document{
		Meta: docmeta.Meta{
			DocID: id, Title: fmt.Sprintf("%s 运行日志 %s", service, day.Format(time.DateOnly)),
			Owner: service, Classification: s.Config.Classification, ACLTokens: s.Config.aclTokens(service),
			// 文档的发生时间取最后一条消息的时间：后到的消息让文档更新，乱序的旧事件按它丢弃
			UpdatedAt: state.Entries[len(state.Entries)-1].Time, ACLFingerprint: acl,
		},
		SourceURI: s.BaseURI + service + "?date=" + day.Format(time.DateOnly), SourceType: "log",
		ContentType: "text/markdown", Text: body, ContentHash: hash,
	}
	if err := s.Publisher.Publish(ctx, d, change, now); err != nil {
		return 0, fmt.Errorf("发出 %s: %w", id, err)
	}
	if err := s.Published.Set(ctx, id, fp); err != nil {
		return 0, err
	}
	return change, nil
}

func (s *Syncer) expire(ctx context.Context, id string, published map[string]string, now time.Time) error {
	if _, ok := published[id]; ok {
		if err := s.Publisher.Delete(ctx, id, now); err != nil {
			return fmt.Errorf("删除 %s: %w", id, err)
		}
	}
	// 先发删除事件、再清状态：中途失败的下一轮还能看到这份文档，重发删除（下游按删除标记幂等）
	if err := s.Published.Del(ctx, id); err != nil {
		return err
	}
	return s.Store.Delete(ctx, id)
}

// RedisTopic 是 Subscriber 的开发默认实现：Redis Streams 消费组。消息字段为 service、level、ts（RFC 3339）、msg。
type RedisTopic struct {
	Client   *redis.Client
	Stream   string
	Group    string
	Consumer string
	Log      *slog.Logger
	pending  bool
	started  bool
}

// Read 第一次调用时先建消费组（已存在不报错），先取本消费者已读未确认的消息，读完再取新消息。
// 格式不对的消息记日志后丢弃（也会被确认），不让一条坏消息卡住整个主题。
func (r *RedisTopic) Read(ctx context.Context, max int) ([]Message, error) {
	if !r.started {
		err := r.Client.XGroupCreateMkStream(ctx, r.Stream, r.Group, "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return nil, err
		}
		r.started, r.pending = true, true
	}
	for {
		start := ">"
		if r.pending {
			start = "0"
		}
		res, err := r.Client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group: r.Group, Consumer: r.Consumer, Streams: []string{r.Stream, start}, Count: int64(max), Block: -1,
		}).Result()
		if errors.Is(err, redis.Nil) {
			res, err = nil, nil
		}
		if err != nil {
			return nil, err
		}
		var out []Message
		for _, st := range res {
			for _, m := range st.Messages {
				out = append(out, r.decode(m))
			}
		}
		if len(out) == 0 && r.pending {
			// 挂起的消息读完了，接着读新消息
			r.pending = false
			continue
		}
		if r.pending {
			// 挂起列表从 0 开始读：确认之前会一直读到同样的消息，读一轮就转去读新消息
			r.pending = false
		}
		return out, nil
	}
}

func (r *RedisTopic) decode(m redis.XMessage) Message {
	str := func(k string) string { v, _ := m.Values[k].(string); return v }
	out := Message{ID: m.ID, Service: str("service"), Level: str("level"), Text: str("msg")}
	t, err := time.Parse(time.RFC3339Nano, str("ts"))
	if err != nil || out.Service == "" {
		r.Log.Warn("日志消息格式不对，丢弃", slog.String("id", m.ID))
		// 没有服务名的消息任何配置都不会接受
		return Message{ID: m.ID}
	}
	out.Time = t
	return out
}

func (r *RedisTopic) Ack(ctx context.Context, ids []string) error {
	for i := 0; i < len(ids); i += 1000 {
		if err := r.Client.XAck(ctx, r.Stream, r.Group, ids[i:min(i+1000, len(ids))]...).Err(); err != nil {
			return err
		}
	}
	return nil
}
