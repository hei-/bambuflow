package forge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

// GitLab 经 GitLab REST API（v4）打开合并请求：从目标分支拉出 bambuflow/<变更单 ID> 分支，一个提交写入全部文件，
// 再开合并请求（合并后删除源分支）。令牌是 governance 服务账号的项目访问令牌（api 权限），经 Vault 以文件注入。
type GitLab struct {
	BaseURL string
	// 项目 ID 或 namespace/project
	Project string
	Token   string
	// 目标分支，默认 main
	Target string
	Client *http.Client
}

func (g *GitLab) Open(ctx context.Context, mr MergeRequest) (string, error) {
	paths := make([]string, 0, len(mr.Files))
	for p := range mr.Files {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	var actions []map[string]string
	for _, p := range paths {
		exists, err := g.exists(ctx, p)
		if err != nil {
			return "", err
		}
		action := "create"
		if exists {
			action = "update"
		}
		actions = append(actions, map[string]string{"action": action, "file_path": p, "content": mr.Files[p]})
	}
	branch := "bambuflow/" + mr.ID
	if err := g.call(ctx, http.MethodPost, "/repository/commits", map[string]any{
		"branch": branch, "start_branch": g.Target, "commit_message": mr.Title + "\n\n" + mr.Description, "actions": actions,
	}, nil); err != nil {
		return "", fmt.Errorf("提交修改: %w", err)
	}
	var out struct {
		WebURL string `json:"web_url"`
	}
	if err := g.call(ctx, http.MethodPost, "/merge_requests", map[string]any{
		"source_branch": branch, "target_branch": g.Target, "title": mr.Title, "description": mr.Description,
		"remove_source_branch": true,
	}, &out); err != nil {
		return "", fmt.Errorf("开合并请求: %w", err)
	}
	if out.WebURL == "" {
		return "", fmt.Errorf("GitLab 没有返回合并请求的链接")
	}
	return out.WebURL, nil
}

func (g *GitLab) exists(ctx context.Context, path string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead,
		g.endpoint("/repository/files/"+url.PathEscape(path))+"?ref="+url.QueryEscape(g.Target), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("PRIVATE-TOKEN", g.Token)
	resp, err := g.Client.Do(req)
	if err != nil {
		return false, err
	}
	_ = resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNotFound:
		return false, nil
	}
	return false, fmt.Errorf("查询文件 %s: GitLab 返回 %d", path, resp.StatusCode)
}

func (g *GitLab) endpoint(path string) string {
	return strings.TrimRight(g.BaseURL, "/") + "/api/v4/projects/" + url.PathEscape(g.Project) + path
}

func (g *GitLab) call(ctx context.Context, method, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, g.endpoint(path), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("PRIVATE-TOKEN", g.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := g.Client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		// 返回体里可能有路径与提交信息，不含令牌；只截一段写进错误
		return fmt.Errorf("GitLab 返回 %d: %.200s", resp.StatusCode, data)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}
