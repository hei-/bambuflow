// Package denials 接收紧急拒绝规则的备用通道（governance 详设 8.2，部署配置清单 P8）：
// 事件流的独立主题（governance 登记规则时发出，governance 不可用时信息安全用 governance emergency-deny 直接发）
// 与配置卷里的规则文件（运维用同一个工具签发、经部署系统投放）。两条通道只接受验签通过的规则，只能增加拒绝；
// 收到的规则放在进程内存里，与索引库的拒绝名单同时生效（search.Server.Local）。
//
// 规则只增不减：解除一条规则要先经变更单解除正式规则，再由运维删掉事件流里的条目或卷里的文件，滚动重启 retrieval。
package denials

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/hei-/bambuflow/libs/platform-go/denyrule"
)

// Set 是收到的规则，按范围索引。
type Set struct {
	mu                     sync.RWMutex
	ids                    map[string]bool
	sources, docs, tenants map[string]bool
}

func NewSet() *Set {
	return &Set{ids: map[string]bool{}, sources: map[string]bool{}, docs: map[string]bool{}, tenants: map[string]bool{}}
}

// Add 加入一条规则；已有同一 ID 时返回 false。
func (s *Set) Add(r denyrule.Rule) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ids[r.ID] {
		return false
	}
	s.ids[r.ID] = true
	switch r.Scope {
	case "source":
		s.sources[r.Target] = true
	case "document":
		s.docs[r.Target] = true
	case "tenant":
		s.tenants[r.Target] = true
	}
	return true
}

func (s *Set) has(m map[string]bool, k string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return m[k]
}

func (s *Set) Source(id string) bool   { return s.has(s.sources, id) }
func (s *Set) Doc(id string) bool      { return s.has(s.docs, id) }
func (s *Set) Tenant(name string) bool { return s.has(s.tenants, name) }

// Channels 订阅两条备用通道。公钥从 Vault 取，取到之前通道不启用（不阻塞 retrieval 启动），每 30 秒重试。
type Channels struct {
	Set *Set
	// Redis 为 nil 时不订阅事件流；Dir 为空时不读规则文件
	Redis redis.Cmdable
	Dir   string
	// PublicKey 读验签公钥
	PublicKey func(ctx context.Context) (*ecdsa.PublicKey, error)
	Log       *slog.Logger
}

// Run 启动两条通道，直到 ctx 结束。
func (c *Channels) Run(ctx context.Context) {
	var v *denyrule.Verifier
	for v == nil {
		pub, err := c.PublicKey(ctx)
		if err == nil {
			v, err = denyrule.NewVerifier(pub)
		}
		if err != nil {
			c.Log.ErrorContext(ctx, "紧急拒绝规则的备用通道未启用：取不到验签公钥，30 秒后重试", slog.Any("err", err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
		}
	}
	var wg sync.WaitGroup
	if c.Redis != nil {
		wg.Go(func() { c.follow(ctx, v) })
	}
	if c.Dir != "" {
		wg.Go(func() { c.watch(ctx, v, 30*time.Second) })
	}
	wg.Wait()
}

// follow 从头读事件流的独立主题并跟随新条目：主题不裁剪，每次启动都拿到全部规则。
func (c *Channels) follow(ctx context.Context, v *denyrule.Verifier) {
	last := "0"
	for ctx.Err() == nil {
		res, err := c.Redis.XRead(ctx, &redis.XReadArgs{Streams: []string{denyrule.Stream, last}, Count: 100, Block: 5 * time.Second}).Result()
		if err != nil {
			if !errors.Is(err, redis.Nil) && ctx.Err() == nil {
				c.Log.ErrorContext(ctx, "读取紧急拒绝规则的事件流失败，5 秒后重试", slog.Any("err", err))
				select {
				case <-ctx.Done():
				case <-time.After(5 * time.Second):
				}
			}
			continue
		}
		for _, st := range res {
			for _, m := range st.Messages {
				last = m.ID
				raw, _ := m.Values["rule"].(string)
				c.accept(ctx, v, "stream", []byte(raw))
			}
		}
	}
}

// watch 每隔 every 读一遍配置卷里的 *.json 规则文件（新投放的文件在下一轮生效）。
func (c *Channels) watch(ctx context.Context, v *denyrule.Verifier, every time.Duration) {
	seen := map[string]time.Time{}
	for {
		c.scan(ctx, v, seen)
		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}

func (c *Channels) scan(ctx context.Context, v *denyrule.Verifier, seen map[string]time.Time) {
	entries, err := os.ReadDir(c.Dir)
	if err != nil {
		c.Log.ErrorContext(ctx, "读取紧急拒绝规则目录失败", slog.Any("err", err))
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		info, err := e.Info()
		if err != nil || seen[e.Name()].Equal(info.ModTime()) {
			continue
		}
		seen[e.Name()] = info.ModTime()
		b, err := os.ReadFile(filepath.Join(c.Dir, e.Name()))
		if err != nil {
			c.Log.ErrorContext(ctx, "读取紧急拒绝规则文件失败", slog.String("file", e.Name()), slog.Any("err", err))
			continue
		}
		c.accept(ctx, v, "file", b)
	}
}

func (c *Channels) accept(ctx context.Context, v *denyrule.Verifier, channel string, raw []byte) {
	r, err := v.Verify(raw)
	if err != nil {
		c.Log.ErrorContext(ctx, "备用通道收到验签失败的紧急拒绝规则，已丢弃", slog.String("channel", channel), slog.Any("err", err))
		return
	}
	if c.Set.Add(r) {
		c.Log.WarnContext(ctx, "紧急拒绝规则已生效（备用通道）", slog.String("channel", channel), slog.String("denial_id", r.ID),
			slog.String("scope", r.Scope), slog.String("issued_by", r.IssuedBy))
	}
}
