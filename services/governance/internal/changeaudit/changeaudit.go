// Package changeaudit 写 governance 的业务审计（governance 详设 9.3）：每次紧急拒绝、数据源注册与审批、授权变更、
// 审计查询都写一条，记谁（带哪些平台角色）在什么时候对什么对象做了什么、依据是什么，以及入口的 trace_id，
// 与 gateway 的管理审计关联（gateway 详设 8.2）。
//
// 与问答审计写同一个只追加的桶（libs/platform-go/worm），一条一个对象。写不进去时调用方不执行操作（fail-closed）。
package changeaudit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
)

// Prefix 是变更审计在审计桶里的键前缀。
const Prefix = "governance/changes/"

// Record 是一条变更审计。
type Record struct {
	Time    time.Time `json:"time"`
	TraceID string    `json:"trace_id,omitempty"`
	Actor   string    `json:"actor"`
	Roles   []string  `json:"roles,omitempty"`
	// 动作，如 emergency_denial.create、source.approve、audit.query
	Action string `json:"action"`
	// 对象 ID，如 den_…、src_…
	ObjectID string `json:"object_id,omitempty"`
	// 动作的参数与依据（不含密钥与个人信息以外的正文）
	Detail any `json:"detail,omitempty"`
}

// Store 是只追加的对象存储。
type Store interface {
	Put(ctx context.Context, key string, body []byte) error
}

type Writer struct {
	Store Store
	Now   func() time.Time
}

type serviceKey struct{}

// AsService 标记操作由服务发起（如 evaluation 的护栏停止实验），没有管理 Principal 时记为 service:<名称>。
func AsService(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, serviceKey{}, name)
}

// Write 按调用者（管理 Principal，或 AsService 标记的服务）与入口 trace_id 补全记录后写入。
func (w *Writer) Write(ctx context.Context, action, objectID string, detail any) error {
	p := grpcmw.PrincipalFrom(ctx)
	actor := p.GetSub()
	if svc, ok := ctx.Value(serviceKey{}).(string); ok && p == nil {
		actor = "service:" + svc
	}
	r := Record{Time: w.Now().UTC(), TraceID: grpcmw.TraceID(ctx), Actor: actor, Roles: p.GetRoles(),
		Action: action, ObjectID: objectID, Detail: detail}
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}
	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	key := fmt.Sprintf("%s%s/%019d-%s.json", Prefix, r.Time.Format("2006/01/02"), r.Time.UnixNano(), hex.EncodeToString(b))
	if err := w.Store.Put(ctx, key, append(body, '\n')); err != nil {
		return fmt.Errorf("写变更审计: %w", err)
	}
	return nil
}
