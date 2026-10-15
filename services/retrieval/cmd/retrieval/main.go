// retrieval 是唯一的索引读入口和权限过滤执行点（retrieval 详设）。P0 范围：稠密向量召回、策略编译过滤、组装复核、CheckAccess。
// 租户可访问的数据源、检索计划与检索权限策略包都由 governance 经配置快照下发（BF_CONFIG_ADDR）。
//
//	retrieval serve        启动 RetrievalService（gRPC）与内部 HTTP 端口（/healthz、/readyz）
//	retrieval healthcheck  容器健康检查（访问内部端口的 /readyz）
package main

import (
	"context"
	"crypto/ecdsa"
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

	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	configv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/config/v1"
	retrievalv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/hei-/bambuflow/libs/platform-go/denyrule"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
	"github.com/hei-/bambuflow/libs/platform-go/principal"
	"github.com/hei-/bambuflow/libs/platform-go/revocation"
	"github.com/hei-/bambuflow/libs/platform-go/snapshot"
	"github.com/hei-/bambuflow/libs/platform-go/telemetry"
	"github.com/hei-/bambuflow/libs/platform-go/vault"
	"github.com/hei-/bambuflow/libs/textseg"
	"github.com/hei-/bambuflow/services/retrieval/internal/cache"
	"github.com/hei-/bambuflow/services/retrieval/internal/denials"
	"github.com/hei-/bambuflow/services/retrieval/internal/embed"
	"github.com/hei-/bambuflow/services/retrieval/internal/grants"
	"github.com/hei-/bambuflow/services/retrieval/internal/guard"
	"github.com/hei-/bambuflow/services/retrieval/internal/pii"
	"github.com/hei-/bambuflow/services/retrieval/internal/search"
	"github.com/hei-/bambuflow/services/retrieval/internal/store"
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

// list 读逗号分隔的环境变量，去掉空项
func list(name string) []string {
	var out []string
	for v := range strings.SplitSeq(os.Getenv(name), ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
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
	if err := telemetry.ServeMetrics(ctx, "retrieval", log); err != nil {
		return err
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
	// 撤权位点与待生效撤权名单所在的 Redis，与事件流的 Redis 分开（元数据库可用性详设第 9 节第 3 项）
	revAddr, err := env("BF_REVOCATION_REDIS_ADDR")
	if err != nil {
		return err
	}
	revRDB := redis.NewClient(&redis.Options{Addr: revAddr, Password: os.Getenv("BF_REVOCATION_REDIS_PASSWORD")})
	defer func() { _ = revRDB.Close() }()
	// 权限读副本组、查询读副本组与同步备库（详设 3.1）；都不配时全部读 BF_PG_DSN（单实例部署）
	st, err := store.Open(ctx, store.Config{DSN: dsn, PermissionDSNs: list("BF_PERMISSION_REPLICA_DSNS"),
		QueryDSN: os.Getenv("BF_QUERY_REPLICA_DSN"), StandbyDSNs: list("BF_SYNC_STANDBY_DSNS"),
		Revocation: &revocation.Store{Redis: revRDB}, Log: log})
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
	configClient := configv1.NewConfigServiceClient(configConn)
	host, _ := os.Hostname()
	verifier := &snapshot.Verifier{Client: configClient, Now: time.Now}
	grantsWatcher := &grants.Watcher{Client: configClient, Open: verifier.Open, Instance: host, Every: 10 * time.Second,
		MaxAge: 5 * time.Minute, Log: log, Now: time.Now}
	// 重启时 governance 不可用，用盘上 5 分钟内确认过的一份继续服务（governance 详设 8.1）
	if dir := os.Getenv("BF_SNAPSHOT_CACHE_DIR"); dir != "" {
		grantsWatcher.Cache = &snapshot.Cache{Dir: dir, Service: "retrieval", Keys: verifier, Now: time.Now}
		if err := grantsWatcher.Restore(ctx); err != nil && !errors.Is(err, snapshot.ErrNoCache) {
			log.WarnContext(ctx, "本地磁盘上的租户授权不可用", slog.Any("err", err))
		}
	}
	go grantsWatcher.Run(ctx)
	go grantsWatcher.Watch(ctx)

	srv := &search.Server{Store: st, Embedder: embed.New(embedderURL, envOr("BF_EMBEDDING_MODEL", "bge-m3")), Grants: grantsWatcher,
		// 策略包随快照下发（grants）；没有或编译失败时 readiness 为 false，请求一律 UNAVAILABLE（详设 10.3、9.4）。
		// 进程保持运行：故障时还能看到健康状态和日志，而不是反复重启
		Policy: grantsWatcher, Log: log}
	// 紧急拒绝规则的备用通道（governance 详设 8.2）：事件流的独立主题与配置卷里的规则文件，验签后与索引库的拒绝名单同时生效。
	// 验签公钥从 Vault 取，取不到时通道暂不启用、后台重试，不阻塞启动
	redisAddr, err := env("BF_REDIS_ADDR")
	if err != nil {
		return err
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("BF_REDIS_PASSWORD")})
	defer func() { _ = rdb.Close() }()
	localDenials := denials.NewSet()
	srv.Local = localDenials
	go (&denials.Channels{Set: localDenials, Redis: rdb, Dir: os.Getenv("BF_EMERGENCY_DENY_DIR"), Log: log,
		PublicKey: func(ctx context.Context) (*ecdsa.PublicKey, error) {
			vc, err := vault.FromEnv()
			if err != nil {
				return nil, err
			}
			data, err := vc.ReadKV(ctx, envOr("BF_VAULT_KV_MOUNT", "secret"), denyrule.KeyPath)
			if err != nil {
				return nil, err
			}
			return denyrule.ParsePublicKey(data["public_key"])
		}}).Run(ctx)
	// 检索结果与查询向量缓存（详设第 8 节，internal/cache）：用单独的 Redis，不配置时不缓存
	if addr := os.Getenv("BF_CACHE_REDIS_ADDR"); addr != "" {
		crdb := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("BF_CACHE_REDIS_PASSWORD")})
		defer func() { _ = crdb.Close() }()
		results := &cache.Results{Client: crdb, Log: log}
		srv.Results, srv.Vectors = results, &cache.Vectors{Client: crdb, Log: log}
		go (&cache.Invalidator{Events: rdb, Results: results, Log: log}).Run(ctx)
	}
	// 熔断与准入（详设 9.2）：单路预算 150 ms；租户令牌桶与在途上限是运行参数，按副本规格在部署配置里调
	srv.Breakers = guard.NewBreakers(150 * time.Millisecond)
	if srv.Admission, err = admission(); err != nil {
		return err
	}
	// 启用的召回路（详设 6.2 的检索计划，P1 先由部署配置给出）；对照评测时可只开稠密路
	// 稀疏召回要求嵌入模型给稀疏向量，默认不开：BF_RETRIEVERS 列出 sparse 才执行（详设 6.2）
	srv.Disabled = map[string]bool{"sparse": true}
	if v := os.Getenv("BF_RETRIEVERS"); v != "" {
		srv.Disabled = map[string]bool{"dense": true, "keyword": true, "graph": true, "identifier": true, "sparse": true}
		for _, r := range strings.Split(v, ",") {
			delete(srv.Disabled, strings.TrimSpace(r))
		}
	}
	// 精排是可选环节：未配置地址时不精排，响应的 degraded 里注明（详设 9.4）
	if u := os.Getenv("BF_RERANKER_URL"); u != "" {
		srv.Reranker = embed.NewReranker(u, envOr("BF_RERANKER_MODEL", "bge-reranker-v2-m3"))
	}
	// PII 原值的还原（技术设计文档 8.6）：经 Vault transit 解开入库时包裹的数据密钥。没有配置 Vault 时，
	// 能看 PII 的调用者也只看到掩码（fail-closed）
	if addr := os.Getenv("VAULT_ADDR"); addr != "" {
		srv.PII = &pii.Restorer{
			Keys: &pii.VaultTransit{Addr: addr, Token: os.Getenv("VAULT_TOKEN"), TokenFile: os.Getenv("VAULT_TOKEN_FILE"),
				HTTP: &http.Client{Timeout: 2 * time.Second}},
			TTL: 10 * time.Minute, Max: 10000, Now: time.Now,
		}
	}
	// 分词词典加载失败时关键词路降级（Tokenize 为 nil），稠密路照常；不因此拒绝启动
	if err := textseg.Load(); err != nil {
		log.Error("分词词典加载失败，关键词召回不可用", slog.Any("err", err))
	} else {
		srv.Tokenize = textseg.Tokens
	}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", envOr("BF_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	// 评测副本（BF_EVAL_MODE=1）接受指定索引版本与候选配置的请求，线上实例收到就拒绝（evaluation 详设 4.2）
	srv.EvalMode = os.Getenv("BF_EVAL_MODE") == "1"
	// 评测副本只信任 evaluation 签发的评测身份，不接受 gateway 签发的 Principal；线上实例反之
	issuer := principal.Issuer
	if srv.EvalMode {
		issuer = principal.EvalIssuer
	}
	gs := grpc.NewServer(telemetry.ServerOption(),
		grpc.ChainUnaryInterceptor(grpcmw.UnaryServer(principal.NewVerifier(principal.NewJWKS(jwksURL, nil)).WithIssuer(issuer), log),
			search.EvalPinInterceptor(srv.EvalMode, grantsWatcher)))
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
