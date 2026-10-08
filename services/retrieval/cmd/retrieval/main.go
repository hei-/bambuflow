// retrieval 是唯一的索引读入口和权限过滤执行点（retrieval 详设）。P0 范围：稠密向量召回、策略编译过滤、组装复核、CheckAccess。
// 租户可访问的数据源由 governance 下发（BF_CONFIG_ADDR），策略包从本地目录加载（BF_POLICY_DIR）。
//
//	retrieval serve        启动 RetrievalService（gRPC）与内部 HTTP 端口（/healthz、/readyz）
//	retrieval healthcheck  容器健康检查（访问内部端口的 /readyz）
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
	"github.com/bambu-ai/bambuflow/libs/platform-go/telemetry"
	"github.com/bambu-ai/bambuflow/libs/textseg"
	"github.com/bambu-ai/bambuflow/services/retrieval/internal/embed"
	"github.com/bambu-ai/bambuflow/services/retrieval/internal/grants"
	"github.com/bambu-ai/bambuflow/services/retrieval/internal/guard"
	"github.com/bambu-ai/bambuflow/services/retrieval/internal/policy"
	"github.com/bambu-ai/bambuflow/services/retrieval/internal/search"
	"github.com/bambu-ai/bambuflow/services/retrieval/internal/store"
)

func main() {
	log := logging.New(os.Stdout, "retrieval", slog.LevelInfo)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: retrieval serve | healthcheck")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "serve":
		err = serve(ctx, log)
	case "healthcheck":
		err = healthcheck(ctx)
	default:
		err = fmt.Errorf("未知子命令 %q", os.Args[1])
	}
	if err != nil {
		log.Error("退出", slog.Any("err", err))
		os.Exit(1)
	}
}

func env(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("未设置 %s", name)
	}
	return v, nil
}

func admission() (*guard.Admission, error) {
	vals := map[string]int{"BF_TENANT_RPS": 50, "BF_TENANT_BURST": 100, "BF_MAX_INFLIGHT": 64}
	for name := range vals {
		if v := os.Getenv(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return nil, fmt.Errorf("%s 不合法: %q", name, v)
			}
			vals[name] = n
		}
	}
	return &guard.Admission{
		TenantRate: rate.Limit(vals["BF_TENANT_RPS"]), TenantBurst: vals["BF_TENANT_BURST"], MaxInflight: vals["BF_MAX_INFLIGHT"],
	}, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func serve(ctx context.Context, log *slog.Logger) error {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.Setup(ctx, "retrieval")
		if err != nil {
			return err
		}
		defer func() { _ = shutdown(context.WithoutCancel(ctx)) }()
	}
	jwksURL, err := env("BF_PRINCIPAL_JWKS_URL")
	if err != nil {
		return err
	}
	embedderURL, err := env("BF_EMBEDDER_URL")
	if err != nil {
		return err
	}
	dsn, err := env("BF_PG_DSN")
	if err != nil {
		return err
	}
	st, err := store.Open(ctx, dsn)
	if err != nil {
		return err
	}
	defer st.Close()

	// 租户授权以 governance 为唯一事实来源（ConfigService 的 retrieval 快照）：每 10 秒拉一次，
	// 取消授权 1 分钟内生效；还没拿到、或 5 分钟没刷新成功时拒绝请求、readiness 为 false（fail-closed）
	configAddr, err := env("BF_CONFIG_ADDR")
	if err != nil {
		return err
	}
	configConn, err := grpc.NewClient(configAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), telemetry.DialOption())
	if err != nil {
		return err
	}
	defer func() { _ = configConn.Close() }()
	grantsWatcher := &grants.Watcher{Client: configv1.NewConfigServiceClient(configConn), Every: 10 * time.Second,
		MaxAge: 5 * time.Minute, Log: log, Now: time.Now}
	go grantsWatcher.Run(ctx)

	srv := &search.Server{Store: st, Embedder: embed.New(embedderURL, envOr("BF_EMBEDDING_MODEL", "bge-m3")), Grants: grantsWatcher, Log: log}
	// 熔断与准入（详设 9.2）：单路预算 150 ms；租户令牌桶与在途上限是运行参数，按副本规格在部署配置里调
	srv.Breakers = guard.NewBreakers(150 * time.Millisecond)
	if srv.Admission, err = admission(); err != nil {
		return err
	}
	// 启用的召回路（详设 6.2 的检索计划，P1 先由部署配置给出）；对照评测时可只开稠密路
	if v := os.Getenv("BF_RETRIEVERS"); v != "" {
		srv.Disabled = map[string]bool{"dense": true, "keyword": true, "graph": true}
		for _, r := range strings.Split(v, ",") {
			delete(srv.Disabled, strings.TrimSpace(r))
		}
	}
	// 精排是可选环节：未配置地址时不精排，响应的 degraded 里注明（详设 9.4）
	if u := os.Getenv("BF_RERANKER_URL"); u != "" {
		srv.Reranker = embed.NewReranker(u, envOr("BF_RERANKER_MODEL", "bge-reranker-v2-m3"))
	}
	// 分词词典加载失败时关键词路降级（Tokenize 为 nil），稠密路照常；不因此拒绝启动
	if err := textseg.Load(); err != nil {
		log.Error("分词词典加载失败，关键词召回不可用", slog.Any("err", err))
	} else {
		srv.Tokenize = textseg.Tokens
	}
	// 策略包加载失败不退出：进程保持运行，readiness 为 false，请求一律 UNAVAILABLE（详设 10.3、9.4）。
	// 这样故障时还能看到健康状态和日志，而不是反复重启。
	engine, err := policy.Load(ctx, os.DirFS(envOr("BF_POLICY_DIR", "/etc/bambuflow/policy")))
	if err != nil {
		log.Error("策略包不可用，拒绝所有请求", slog.Any("err", err))
	} else {
		srv.Policy = engine
		log.Info("已加载策略包", slog.String("policy_version", engine.Version()))
	}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", envOr("BF_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	gs := grpc.NewServer(telemetry.ServerOption(),
		grpc.UnaryInterceptor(grpcmw.UnaryServer(principal.NewVerifier(principal.NewJWKS(jwksURL, nil)), log)))
	retrievalv1.RegisterRetrievalServiceServer(gs, srv)

	internal := http.NewServeMux()
	internal.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	internal.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case srv.Policy == nil:
			http.Error(w, "policy bundle unavailable", http.StatusServiceUnavailable)
		case !grantsWatcher.Ready():
			http.Error(w, "tenant grants unavailable", http.StatusServiceUnavailable)
		case st.Ping(r.Context()) != nil:
			http.Error(w, "metadata store unavailable", http.StatusServiceUnavailable)
		default:
			_, _ = w.Write([]byte("ok"))
		}
	})
	hs := &http.Server{Addr: envOr("BF_INTERNAL_LISTEN", ":8081"), Handler: internal, ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	go func() { errc <- gs.Serve(lis) }()
	go func() {
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()
	log.Info("retrieval 已启动")
	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdownCtx)
	gs.GracefulStop()
	return nil
}

func healthcheck(ctx context.Context) error {
	_, port, err := net.SplitHostPort(envOr("BF_INTERNAL_LISTEN", ":8081"))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort("127.0.0.1", port)+"/readyz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("readyz 返回 %d", resp.StatusCode)
	}
	return nil
}
