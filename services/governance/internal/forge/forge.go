// Package forge 是 governance 写合并请求的端口（governance 详设 3.1：console 修改 Git 类对象时，governance 用服务账号
// 把修改写成一个合并请求，之后与工程师的修改走同一条路——评审、CI 门禁、合并后 CI 调发布接口，不存在绕过 Git 直接改
// 生效版本的口子）。
//
// 实现按部署选（BF_GIT_FORGE）：gitlab（公司 GitLab 的 REST API）；outbox（写进本地目录，由人或流水线同步进仓库，
// 开发与演示用）；不配置时 console 不能修改 Git 类对象（保守默认值，ADR-13），CI 发布照常。
package forge

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// MergeRequest 是一次修改：一个变更单对应一个分支、一个提交、一个合并请求。
type MergeRequest struct {
	// 变更单 ID，用作分支名与目录名
	ID          string
	Title       string
	Description string
	// 仓库内路径 → 新内容
	Files map[string]string
}

// Forge 打开合并请求，返回它的链接。
type Forge interface {
	Open(ctx context.Context, mr MergeRequest) (string, error)
}

// ErrDisabled：部署没有配置合并请求的去处。
var ErrDisabled = errors.New("没有配置合并请求（BF_GIT_FORGE），Git 类配置只能经 CI 发布")

// Disabled 是不配置时的实现：一律拒绝。
type Disabled struct{}

func (Disabled) Open(context.Context, MergeRequest) (string, error) { return "", ErrDisabled }

// Outbox 把合并请求写成目录 <Dir>/<变更单 ID>/：MERGE_REQUEST.md（标题与说明）与按仓库路径放置的文件。
type Outbox struct{ Dir string }

func (o Outbox) Open(_ context.Context, mr MergeRequest) (string, error) {
	root := filepath.Join(o.Dir, mr.ID)
	write := func(rel, content string) error {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if !strings.HasPrefix(p, root+string(filepath.Separator)) {
			return fmt.Errorf("仓库路径 %q 越出目录", rel)
		}
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			return err
		}
		return os.WriteFile(p, []byte(content), 0o600)
	}
	if err := write("MERGE_REQUEST.md", "# "+mr.Title+"\n\n"+mr.Description+"\n"); err != nil {
		return "", err
	}
	for path, content := range mr.Files {
		if err := write(path, content); err != nil {
			return "", err
		}
	}
	return "file://" + root, nil
}

// FromEnv 按 BF_GIT_FORGE 选实现。
func FromEnv(getenv func(string) string) (Forge, error) {
	switch kind := getenv("BF_GIT_FORGE"); kind {
	case "":
		return Disabled{}, nil
	case "outbox":
		dir := getenv("BF_GIT_OUTBOX_DIR")
		if dir == "" {
			return nil, errors.New("BF_GIT_FORGE=outbox 需要 BF_GIT_OUTBOX_DIR")
		}
		return Outbox{Dir: dir}, nil
	case "gitlab":
		token, err := os.ReadFile(filepath.Clean(getenv("BF_GITLAB_TOKEN_FILE")))
		if err != nil {
			return nil, fmt.Errorf("读取 GitLab 令牌（BF_GITLAB_TOKEN_FILE）: %w", err)
		}
		g := &GitLab{BaseURL: getenv("BF_GITLAB_URL"), Project: getenv("BF_GITLAB_PROJECT"), Token: strings.TrimSpace(string(token)),
			Target: getenv("BF_GIT_TARGET_BRANCH"), Client: &http.Client{Timeout: 10 * time.Second}}
		if g.Target == "" {
			g.Target = "main"
		}
		if g.BaseURL == "" || g.Project == "" || g.Token == "" {
			return nil, errors.New("BF_GIT_FORGE=gitlab 需要 BF_GITLAB_URL、BF_GITLAB_PROJECT 与令牌")
		}
		return g, nil
	default:
		return nil, fmt.Errorf("BF_GIT_FORGE 只能是 gitlab 或 outbox，得到 %q", kind)
	}
}
