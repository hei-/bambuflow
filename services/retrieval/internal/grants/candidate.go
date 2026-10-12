package grants

import (
	"context"
	"log/slog"
	"regexp"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/policy"
)

// 评测副本的候选配置（governance 详设 4.1 第 ③④ 步、evaluation 详设 4.2）：配置发布的门禁按请求指定一个候选版本，
// 这里向 governance 按版本取（ConfigService.GetCandidate，转发评测身份；只对评测身份开放），验签、编译后只用于这次请求。
// 候选版本不可变，但策略包的候选是“替换这一层后合成的整个包”，会随其他层的生效版本变化，所以缓存只留 candidateTTL。

const (
	candidateTTL = 10 * time.Minute
	// 同时在评的候选不多（门禁最多并行 4 个）；超过就整份清掉重取
	maxCandidates = 32
)

var candidatePattern = regexp.MustCompile(`^cfv_[0-9a-f]{24}$`)

type candidate struct {
	canary *configv1.Canary
	// 策略包的候选编译好的引擎；不是策略包时为 nil
	engine *policy.Engine
	at     time.Time
}

type candidateKey struct{}

// WithCandidate 取候选版本并放进 ctx，之后这次请求的 Plan 与 Decide 用它。取不到（版本不存在、governance 不可用、
// 验签或编译失败）就拒绝这次请求：门禁拿不到候选时不能拿生效的配置冒充候选去评（fail-closed）。
func (w *Watcher) WithCandidate(ctx context.Context, version string) (context.Context, error) {
	if !candidatePattern.MatchString(version) {
		return nil, status.Error(codes.InvalidArgument, "候选配置版本不合法")
	}
	now := w.Now()
	w.candMu.Lock()
	c, ok := w.cands[version]
	w.candMu.Unlock()
	if !ok || now.Sub(c.at) > candidateTTL {
		var err error
		if c, err = w.fetchCandidate(ctx, version, now); err != nil {
			w.Log.ErrorContext(ctx, "取候选配置失败", slog.String("version_id", version), slog.Any("err", err))
			return nil, status.Error(codes.FailedPrecondition, "候选配置不可用")
		}
		w.candMu.Lock()
		if w.cands == nil || len(w.cands) >= maxCandidates {
			w.cands = map[string]candidate{}
		}
		w.cands[version] = c
		w.candMu.Unlock()
	}
	return context.WithValue(ctx, candidateKey{}, c), nil
}

func (w *Watcher) fetchCandidate(ctx context.Context, version string, now time.Time) (candidate, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	resp, err := w.Client.GetCandidate(grpcmw.ForwardPrincipal(ctx), &configv1.GetCandidateRequest{Service: "retrieval", VersionId: version})
	if err != nil {
		return candidate{}, err
	}
	snap, err := w.Open(ctx, resp.GetSnapshot())
	if err != nil {
		return candidate{}, err
	}
	if len(snap.GetCanaries()) != 1 || snap.GetCanaries()[0].GetVersionId() != version {
		return candidate{}, status.Error(codes.Internal, "候选快照与请求的版本不符")
	}
	c := candidate{canary: snap.GetCanaries()[0], at: now}
	if pb := c.canary.GetPolicy(); pb != nil {
		b := policy.Bundle{Version: pb.GetVersion()}
		for _, m := range pb.GetModules() {
			b.Modules = append(b.Modules, policy.Module{Path: m.GetPath(), Source: m.GetSource()})
		}
		if c.engine, err = policy.Compile(ctx, b); err != nil {
			return candidate{}, err
		}
	}
	w.Log.InfoContext(ctx, "已加载候选配置", slog.String("version_id", version), slog.String("object", c.canary.GetObject()),
		slog.String("scope", c.canary.GetScope()))
	return c, nil
}

func candidateFrom(ctx context.Context) (candidate, bool) {
	c, ok := ctx.Value(candidateKey{}).(candidate)
	return c, ok
}
