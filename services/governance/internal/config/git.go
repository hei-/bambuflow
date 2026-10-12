package config

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"

	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/services/governance/internal/forge"
)

// gitObject：事实来源在 Git 的对象（governance 详设 3.1）。console 对它们的修改写成合并请求，不在这里审批生效；
// 租户配置是运营数据，变更单审批后直接生效。
func gitObject(object string) bool {
	return object == ObjectPipeline || object == ObjectRouting || object == ObjectPolicy || object == ObjectPrompt
}

// RepoPath 是配置对象在配置仓库里的路径（与 CI 发布时读取的位置一致）。
func RepoPath(object, scope string) string {
	switch object {
	case ObjectPipeline:
		return "config/tenants/" + scope + "/pipeline.yaml"
	case ObjectRouting:
		return "config/routing/routing.yaml"
	case ObjectPolicy:
		// global.rego、tenant.<租户>.rego、source.<数据源>.rego：与 BF_POLICY_SEED_DIRS 的文件名一致
		return "config/policies/" + strings.Replace(scope, ":", ".", 1) + ".rego"
	case ObjectPrompt:
		return "config/prompts/" + scope + ".yaml"
	}
	return ""
}

func (s *Server) openMergeRequest(ctx context.Context, c Change, v Version) (string, error) {
	f := s.Forge
	if f == nil {
		f = forge.Disabled{}
	}
	var widened string
	if len(v.Analysis) > 0 {
		widened = "\n\n可见范围变大的主体（发布前差异分析）：\n- " + strings.Join(v.Analysis, "\n- ")
	}
	link, err := f.Open(ctx, forge.MergeRequest{
		ID:    c.ID,
		Title: fmt.Sprintf("%s %s：%s", c.Object, c.Scope, c.Reason),
		Description: fmt.Sprintf("由 console 提交（%s，变更单 %s），基于生效版本 %s。合并前须过评测门禁；合并后 CI 调发布接口生效。%s",
			c.Author, c.ID, orNone(c.BaseVersionID), widened),
		Files: map[string]string{RepoPath(c.Object, c.Scope): v.Content},
	})
	if err != nil {
		s.Log.ErrorContext(ctx, "开合并请求失败", "err", err, "chg_id", c.ID)
		return "", grpcmw.Reason(codes.FailedPrecondition, "merge_request_unavailable", err.Error())
	}
	return link, nil
}

func orNone(s string) string {
	if s == "" {
		return "（新建）"
	}
	return s
}
