// Package identitysync 把公司目录中的用户属性与群组写入平台身份库（gateway 详设 3.5）。上游三选一：
// LDAP / Active Directory（本文件与 ldap.go）、SCIM 推送（scim.go）、导出文件（file.go）。
//
// LDAP 每次运行按修改时间增量拉取用户与群组；启动时和每隔 full_every 做一次全量对账，
// 把目录里已不存在的用户标为停用、清掉已删除的群组（增量拉取看不到删除）。
// 一次运行的全部写入在一个事务里完成，成功后才更新 last_success_at；gateway 据此判断身份是否新鲜。
package identitysync

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"
)

// Entry 是目录中的一个用户，属性按配置的映射取出，Modified 为目录的修改时间（可比较大小的字符串）。
type Entry struct {
	DN       string
	Attrs    map[string]string
	Modified string
}

// Group 是目录中的一个群组；Members 已解析为用户名。
type Group struct {
	Name     string
	Members  []string
	Modified string
}

// Directory 是公司目录的读取端口；since 为空表示全量。
type Directory interface {
	Users(ctx context.Context, since string) ([]Entry, error)
	Groups(ctx context.Context, since string) ([]Group, error)
}

// Identity 是写入平台身份库的一行。
type Identity struct {
	Username, DisplayName, Dept, Title, Region, DN string
	Clearance                                      int
	Active                                         bool
}

// State 是同步源的进度。
type State struct {
	LastFullAt                time.Time
	UsersCursor, GroupsCursor string
}

// Batch 是一次运行要写入的全部变化。
type Batch struct {
	Full                      bool
	Identities                []Identity
	Groups                    []Group
	UsersCursor, GroupsCursor string
}

// Store 是平台身份库的写入端口。
type Store interface {
	State(ctx context.Context, source string) (State, error)
	// Apply 在一个事务里写入 batch 并把同步源标为成功。
	Apply(ctx context.Context, source string, b Batch) error
	RecordFailure(ctx context.Context, source string, cause error) error
}

type Syncer struct {
	cfg   *Config
	dir   Directory
	store Store
	log   *slog.Logger
	now   func() time.Time
}

// NewSyncer 创建拉取式的同步：ldap 时 dir 为 LDAP 目录，file 时 dir 为 nil（读导出文件）；scim 是推送式的，见 SCIMServer。
func NewSyncer(cfg *Config, dir Directory, store Store, log *slog.Logger) *Syncer {
	return &Syncer{cfg: cfg, dir: dir, store: store, log: log, now: time.Now}
}

// RunOnce 执行一次同步。失败时记录原因并返回错误，last_success_at 不变。
func (s *Syncer) RunOnce(ctx context.Context) error {
	err := s.runOnce(ctx)
	if err != nil {
		if rerr := s.store.RecordFailure(context.WithoutCancel(ctx), s.cfg.Source, err); rerr != nil {
			s.log.ErrorContext(ctx, "记录同步失败状态时出错", slog.Any("err", rerr))
		}
	}
	return err
}

func (s *Syncer) runOnce(ctx context.Context) error {
	st, err := s.store.State(ctx, s.cfg.Source)
	if err != nil {
		return fmt.Errorf("读取同步状态: %w", err)
	}
	if s.cfg.Kind() == KindFile {
		return s.runFile(ctx, st)
	}
	full := st.LastFullAt.IsZero() || s.now().Sub(st.LastFullAt) >= s.cfg.FullEvery
	usersSince, groupsSince := st.UsersCursor, st.GroupsCursor
	if full {
		usersSince, groupsSince = "", ""
	}
	entries, err := s.dir.Users(ctx, usersSince)
	if err != nil {
		return fmt.Errorf("读取目录用户: %w", err)
	}
	groups, err := s.dir.Groups(ctx, groupsSince)
	if err != nil {
		return fmt.Errorf("读取目录群组: %w", err)
	}
	b := Batch{Full: full, Groups: groups, UsersCursor: st.UsersCursor, GroupsCursor: st.GroupsCursor}
	for _, e := range entries {
		id, ok := s.mapEntry(e)
		if !ok {
			continue
		}
		b.Identities = append(b.Identities, id)
		b.UsersCursor = maxString(b.UsersCursor, e.Modified)
	}
	for _, g := range groups {
		b.GroupsCursor = maxString(b.GroupsCursor, g.Modified)
	}
	if err := s.store.Apply(ctx, s.cfg.Source, b); err != nil {
		return fmt.Errorf("写入平台身份库: %w", err)
	}
	s.log.InfoContext(ctx, "身份同步完成",
		slog.Bool("full", full), slog.Int("users", len(b.Identities)), slog.Int("groups", len(groups)))
	return nil
}

func (s *Syncer) mapEntry(e Entry) (Identity, bool) {
	m := s.cfg.Mapping
	username := e.Attrs[m.Username]
	if username == "" {
		// 没有账号属性的条目无法对应到登录用户，跳过；不记 DN，避免日志出现个人信息
		s.log.Warn("目录条目缺少账号属性，已跳过")
		return Identity{}, false
	}
	clearance := clearanceCodes[m.ClearanceByGrade[e.Attrs[m.Grade]]]
	if clearance == 0 {
		clearance = clearanceCodes["public"]
	}
	active := m.Status.Attr == "" || !slices.Contains(m.Status.DisabledValues, e.Attrs[m.Status.Attr])
	return Identity{
		Username: username, DisplayName: e.Attrs[m.DisplayName], Dept: e.Attrs[m.Dept],
		Title: e.Attrs[m.Title], Region: e.Attrs[m.Region], DN: e.DN, Clearance: clearance, Active: active,
	}, true
}

func maxString(a, b string) string {
	if b > a {
		return b
	}
	return a
}

// Run 按 interval 循环同步，直到 ctx 结束。单次失败只记录，不退出：下一轮会重试，
// 持续失败时 last_success_at 不再前进，gateway 在 15 分钟后 fail-closed。
func (s *Syncer) Run(ctx context.Context) {
	t := time.NewTicker(s.cfg.Interval)
	defer t.Stop()
	for {
		runCtx, cancel := context.WithTimeout(ctx, s.cfg.Interval)
		if err := s.RunOnce(runCtx); err != nil {
			s.log.ErrorContext(ctx, "身份同步失败", slog.Any("err", err))
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
