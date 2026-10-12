// Package partition 是分区级数据源（总体设计 ADR-14，入库管道详设第 5 节）：按知识库或目录给部门、群组授权的源，
// 文档只带分区 token（part:<数据源>/<分区>）与密级，“谁能看哪个分区”单独同步成分区授权。
//
// 样例源的分区授权写在一份 YAML 里（BF_PARTITIONS_FILE，源系统没有目录权限接口时也这样在部署配置里登记）；
// 每轮同步与上次发出的比对，变了的分区发 PartitionGrantChanged，由 indexing 的快速通道改分区授权表，不改索引。
// 单份文档自带 ACL 的是例外：按文档级处理并告警（入库管道详设第 5 节）。
package partition

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
)

// Prefix 是分区 token 的前缀（与 retrieval 的 search.PartitionPrefix 一致）。
const Prefix = "part:"

var (
	granteePattern = regexp.MustCompile(`^(group|dept|user):[^\s:]+$`)
	// 分区名：知识库或目录路径，不含空白与冒号
	namePattern = regexp.MustCompile(`^[^\s:]{1,256}$`)
)

// Grants 是分区 → 被授权方（已排序去重）。
type Grants map[string][]string

// Load 读分区授权文件：
//
//	partitions:
//	  制度库/风险管理: [dept:风险管理部, group:risk-auditors]
func Load(path string) (Grants, error) {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	var f struct {
		Partitions map[string][]string `yaml:"partitions"`
	}
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("分区授权文件: %w", err)
	}
	out := Grants{}
	for name, gs := range f.Partitions {
		if !namePattern.MatchString(name) {
			return nil, fmt.Errorf("分区名 %q 不合法", name)
		}
		for _, g := range gs {
			if !granteePattern.MatchString(g) {
				return nil, fmt.Errorf("分区 %s 的被授权方 %q 不合法，应为 group:/dept:/user: 开头", name, g)
			}
		}
		gs = slices.Clone(gs)
		sort.Strings(gs)
		out[name] = slices.Compact(gs)
	}
	return out, nil
}

// Token 是文档的分区 token。
func Token(sourceID, partition string) string { return Prefix + sourceID + "/" + partition }

// Apply 把分区级源的文档改为只带分区 token：权限摘要随之重算，文档在分区之间移动时走快速通道。
// 自带 ACL 的文档按文档级处理；没写分区的文档没有分区可授权，按文档级（只有属主可见）处理。两种都告警。
func Apply(docs []*publish.Document, sourceID string, grants Grants, log *slog.Logger) error {
	for _, d := range docs {
		switch {
		case d.ExplicitACL:
			log.Warn("分区级数据源里的文档自带单文档 ACL，按文档级处理", slog.String("source", sourceID), slog.String("doc_id", d.DocID))
			continue
		case d.Partition == "":
			log.Warn("分区级数据源里的文档没有分区，按文档级处理", slog.String("source", sourceID), slog.String("doc_id", d.DocID))
			continue
		case !namePattern.MatchString(d.Partition):
			return fmt.Errorf("文档 %s 的分区 %q 不合法", d.DocID, d.Partition)
		}
		if _, ok := grants[d.Partition]; !ok {
			// 分区还没登记授权：照常入库，在授权登记之前谁都看不到
			log.Warn("文档所在的分区没有授权，入库后暂时无人可见", slog.String("source", sourceID), slog.String("doc_id", d.DocID),
				slog.String("partition", d.Partition))
		}
		d.ACLTokens = []string{Token(sourceID, d.Partition)}
		d.ACLFingerprint = docmeta.Fingerprint(d.ACLTokens, d.Classification)
	}
	return nil
}

// State 是已发出的分区授权：分区 → 被授权方摘要。
type State interface {
	All(ctx context.Context) (map[string]string, error)
	Set(ctx context.Context, partition, digest string) error
	Delete(ctx context.Context, partition string) error
}

// Emitter 发出分区授权变化（publish.Publisher.PartitionGrants）。
type Emitter interface {
	PartitionGrants(ctx context.Context, partition string, grantees []string, at time.Time) error
}

// Sync 发出与上次不同的分区授权：变了的与新增的发现在的被授权方，文件里删掉的分区发空列表（不再授权给任何人）。
// 事件发出后才记状态，中途失败的下一轮重发。返回发出的分区数。
func Sync(ctx context.Context, grants Grants, state State, emit Emitter, now time.Time) (int, error) {
	sent, err := state.All(ctx)
	if err != nil {
		return 0, fmt.Errorf("读取已发出的分区授权: %w", err)
	}
	n := 0
	names := make([]string, 0, len(grants))
	for name := range grants {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		d := digest(grants[name])
		if sent[name] == d {
			continue
		}
		if err := emit.PartitionGrants(ctx, name, grants[name], now); err != nil {
			return n, fmt.Errorf("分区 %s: %w", name, err)
		}
		if err := state.Set(ctx, name, d); err != nil {
			return n, err
		}
		n++
	}
	for name := range sent {
		if _, ok := grants[name]; ok {
			continue
		}
		if err := emit.PartitionGrants(ctx, name, nil, now); err != nil {
			return n, fmt.Errorf("分区 %s: %w", name, err)
		}
		if err := state.Delete(ctx, name); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

func digest(gs []string) string {
	sum := sha256.Sum256([]byte(strings.Join(gs, "\n")))
	return hex.EncodeToString(sum[:])
}
