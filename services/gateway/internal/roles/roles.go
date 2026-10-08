// Package roles 把平台身份库里的群组映射成平台角色（gateway 详设 4.2：管理接口按路径和方法做 RBAC）。
//
// 角色不放进平台令牌：令牌只证明“是谁”，属性与群组一律来自平台身份库（gateway 详设 3.4），
// 这样从群组里移出一个人，下一次管理请求就不再有这个角色，不必等令牌过期。
// 哪个群组对应哪个角色是部署配置（BF_ADMIN_ROLES_FILE），按 governance 详设 9.1 的角色划分。
package roles

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"go.yaml.in/yaml/v3"
)

// 平台角色（governance 详设 9.1、入库管道详设 14.4）
const (
	PlatformAdmin    = "platform_admin"    // 平台管理员：注册数据源、管理实验等
	PlatformOperator = "platform_operator" // 平台运维：索引版本切换与回滚
	InfoSec          = "infosec"           // 信息安全：紧急拒绝规则
	DataOwner        = "data_owner"        // 数据属主：本部门数据源的审批、撤销、授权与紧急拒绝
	Auditor          = "auditor"           // 审计岗：查审计
)

var known = []string{PlatformAdmin, PlatformOperator, InfoSec, DataOwner, Auditor}

// Map 是角色 → 群组。
type Map map[string][]string

type file struct {
	Roles Map `yaml:"roles"`
}

// Load 读取角色映射文件。未知角色名报错：写错的角色名不能悄悄变成“没人有这个角色”。
func Load(path string) (Map, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("读取角色映射: %w", err)
	}
	var f file
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("角色映射格式不对: %w", err)
	}
	for r := range f.Roles {
		if !slices.Contains(known, r) {
			return nil, fmt.Errorf("未知的平台角色 %q", r)
		}
	}
	if f.Roles == nil {
		f.Roles = Map{}
	}
	return f.Roles, nil
}

// Of 返回群组对应的全部角色，按名称排序。
func (m Map) Of(groups []string) []string {
	var out []string
	for r, gs := range m {
		for _, g := range gs {
			if slices.Contains(groups, g) {
				out = append(out, r)
				break
			}
		}
	}
	sort.Strings(out)
	return out
}
