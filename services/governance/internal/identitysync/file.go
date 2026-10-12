package identitysync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// 导出文件（gateway 详设 3.5 的兜底方式）：上游定时放置一份 CSV，首行为列名，每行一个用户，列按 mapping 取，
// 群组列里多个群组用分隔符隔开。整份替换：文件内容变了就全量写入（文件里没有的用户停用、没有的群组清掉），没变只记一次成功。
// 上游须整份写完再改名放到位（避免读到写了一半的文件）；文件超过 max_age 没有更新算同步失败，
// 过期的导出不能当作新鲜的身份，持续 15 分钟 gateway 就 fail-closed。
func (s *Syncer) runFile(ctx context.Context, st State) error {
	f := s.cfg.File
	info, err := os.Stat(f.Path)
	if err != nil {
		return fmt.Errorf("读取导出文件: %w", err)
	}
	if age := s.now().Sub(info.ModTime()); age > f.MaxAge {
		return fmt.Errorf("导出文件已有 %s 没有更新（上限 %s）", age.Round(time.Second), f.MaxAge)
	}
	body, err := os.ReadFile(filepath.Clean(f.Path))
	if err != nil {
		return fmt.Errorf("读取导出文件: %w", err)
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	if digest == st.UsersCursor {
		if err := s.store.Apply(ctx, s.cfg.Source, Batch{UsersCursor: digest}); err != nil {
			return fmt.Errorf("写入平台身份库: %w", err)
		}
		return nil
	}
	entries, groups, err := parseExport(body, s.cfg.Mapping.Username, f.GroupsColumn, f.GroupsSeparator)
	if err != nil {
		return fmt.Errorf("导出文件: %w", err)
	}
	b := Batch{Full: true, Groups: groups, UsersCursor: digest}
	for _, e := range entries {
		if id, ok := s.mapEntry(e); ok {
			b.Identities = append(b.Identities, id)
		}
	}
	if len(b.Identities) == 0 {
		// 空文件整份替换会停用所有人：多半是上游导出出错，按失败处理
		return errors.New("导出文件里没有用户")
	}
	if err := s.store.Apply(ctx, s.cfg.Source, b); err != nil {
		return fmt.Errorf("写入平台身份库: %w", err)
	}
	s.log.InfoContext(ctx, "身份同步完成（导出文件）", slog.Int("users", len(b.Identities)), slog.Int("groups", len(groups)))
	return nil
}

// parseExport 解析导出文件：每行一个用户（列名 → 值），群组按群组列汇总成员。账号重复的文件整份拒绝。
func parseExport(body []byte, usernameCol, groupsCol, sep string) ([]Entry, []Group, error) {
	r := csv.NewReader(bytes.NewReader(bytes.TrimPrefix(body, []byte("\xef\xbb\xbf"))))
	header, err := r.Read()
	if err != nil {
		return nil, nil, fmt.Errorf("读取列名: %w", err)
	}
	for _, need := range []string{usernameCol, groupsCol} {
		if !slices.Contains(header, need) {
			return nil, nil, fmt.Errorf("缺少列 %s", need)
		}
	}
	var entries []Entry
	members := map[string][]string{}
	seen := map[string]bool{}
	for line := 2; ; line++ {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("第 %d 行: %w", line, err)
		}
		attrs := make(map[string]string, len(header))
		for i, col := range header {
			attrs[col] = strings.TrimSpace(rec[i])
		}
		user := attrs[usernameCol]
		if user == "" {
			continue
		}
		if seen[user] {
			return nil, nil, fmt.Errorf("第 %d 行: 账号重复", line)
		}
		seen[user] = true
		entries = append(entries, Entry{DN: "file:" + user, Attrs: attrs})
		for _, g := range strings.Split(attrs[groupsCol], sep) {
			if g = strings.TrimSpace(g); g != "" {
				members[g] = append(members[g], user)
			}
		}
	}
	groups := make([]Group, 0, len(members))
	for _, name := range slices.Sorted(maps.Keys(members)) {
		groups = append(groups, Group{Name: name, Members: members[name]})
	}
	return entries, groups, nil
}
