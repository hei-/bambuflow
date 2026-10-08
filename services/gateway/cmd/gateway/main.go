// gateway 是所有外部调用的唯一入口（gateway 详设）：平台令牌认证、Principal 签发、租户与功能授权、限流、
// /v1/search、/v1/answers（含 SSE）、MCP 端点与兼容 chat completions 端点、管理接口的分发（/v1/admin）、入口审计。
//
//	gateway serve        启动服务
//	gateway healthcheck  容器健康检查（访问内部端口的 /healthz）
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
	_ "time/tzdata" // 镜像里不一定带时区数据库，配额按 BF_QUOTA_TIMEZONE 的自然日计数要用到它

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	batchv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
	"github.com/bambu-ai/bambuflow/libs/platform-go/telemetry"
	"github.com/bambu-ai/bambuflow/libs/platform-go/vault"
	"github.com/bambu-ai/bambuflow/libs/platform-go/worm"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/admission"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/api"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/audit"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/authn"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/idempotency"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/identity"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/issuer"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/ratelimit"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/roles"
	"github.com/bambu-ai/bambuflow/services/gateway/internal/tenancy"
)

func main() {
	log := logging.New(os.Stdout, "gateway", slog.LevelInfo)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: gateway serve | healthcheck")
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

// auditShipper 连接审计桶（必填：审计送不到只追加的存储就不启动）。
func auditShipper(ctx context.Context, w *audit.Writer, log *slog.Logger) (*audit.Shipper, time.Duration, error) {
	vals := make([]string, 4)
	for i, name := range []string{"BF_AUDIT_S3_ENDPOINT", "BF_AUDIT_S3_ACCESS_KEY", "BF_AUDIT_S3_SECRET_KEY", "BF_AUDIT_BUCKET"} {
		v, err := env(name)
		if err != nil {
			return nil, 0, err
		}
		vals[i] = v
	}
	// 默认保留 7 年：企业审计记录的常见留存期，按合规要求在部署配置里改
	days, err := strconv.Atoi(envOr("BF_AUDIT_RETENTION_DAYS", "2555"))
	if err != nil || days < 1 {
		return nil, 0, fmt.Errorf("BF_AUDIT_RETENTION_DAYS 不合法: %q", os.Getenv("BF_AUDIT_RETENTION_DAYS"))
	}
	every, err := time.ParseDuration(envOr("BF_AUDIT_SHIP_INTERVAL", "1m"))
	if err != nil || every <= 0 {
		return nil, 0, fmt.Errorf("BF_AUDIT_SHIP_INTERVAL 不合法: %q", os.Getenv("BF_AUDIT_SHIP_INTERVAL"))
	}
	store, err := worm.Open(ctx, vals[0], vals[1], vals[2], vals[3], time.Duration(days)*24*time.Hour)
	if err != nil {
		return nil, 0, err
	}
	host, err := os.Hostname()
	if err != nil {
		return nil, 0, err
	}
	return &audit.Shipper{W: w, Store: store, Prefix: "gateway/" + host + "/", Log: log}, every, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func serve(ctx context.Context, log *slog.Logger) error {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.Setup(ctx, "gateway")
		if err != nil {
			return err
		}
		defer func() { _ = shutdown(context.WithoutCancel(ctx)) }()
	}

	// 签名密钥只从 Vault 取；取不到就不启动，不生成临时密钥
	vc, err := vault.FromEnv()
	if err != nil {
		return err
	}
	secret, err := vc.ReadKV(ctx, envOr("BF_SIGNING_KEY_MOUNT", "secret"), envOr("BF_SIGNING_KEY_PATH", "gateway/principal-signing"))
	if err != nil {
		return fmt.Errorf("读取 Principal 签名密钥: %w", err)
	}
	iss, err := issuer.FromPEM(secret["private_key"])
	if err != nil {
		return err
	}

	dsn, err := env("BF_PG_DSN")
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return err
	}
	defer pool.Close()

	tenantsDir, err := env("BF_TENANTS_DIR")
	if err != nil {
		return err
	}
	tenants, err := tenancy.Load(tenantsDir, strings.Split(envOr("BF_PLATFORM_CLIENTS", "console"), ","))
	if err != nil {
		return err
	}

	// 平台角色 → 群组（管理接口的 RBAC，部署配置）；文件必须存在，内容可以为空（没有人能调管理接口）
	rolesFile, err := env("BF_ADMIN_ROLES_FILE")
	if err != nil {
		return err
	}
	adminRoles, err := roles.Load(rolesFile)
	if err != nil {
		return err
	}

	platformIssuer, err := env("BF_PLATFORM_ISSUER")
	if err != nil {
		return err
	}
	jwksURL := envOr("BF_PLATFORM_JWKS_URL", platformIssuer+"/protocol/openid-connect/certs")
	// 对外根地址：MCP 的资源标识与受保护资源元数据要用它（gateway 详设 7.2），取不到就不启动
	publicURL, err := env("BF_PUBLIC_URL")
	if err != nil {
		return err
	}
	publicURL = strings.TrimSuffix(publicURL, "/")

	auditW, err := audit.Open(envOr("BF_AUDIT_FILE", "/var/lib/gateway/audit/audit.jsonl"))
	if err != nil {
		return err
	}
	defer func() { _ = auditW.Close() }()
	shipper, every, err := auditShipper(ctx, auditW, log)
	if err != nil {
		return err
	}
	shipped := make(chan struct{})
	go func() { defer close(shipped); shipper.Run(ctx, every) }()
	// 先等最后一轮送出再关文件
	defer func() { <-shipped }()

	dial := func(name string) (*grpc.ClientConn, error) {
		addr, err := env(name)
		if err != nil {
			return nil, err
		}
		// 服务间 mTLS 由服务网格提供（技术设计文档 12.6），P0 的 compose 内是明文
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), telemetry.DialOption())
	}
	retrievalConn, err := dial("BF_RETRIEVAL_ADDR")
	if err != nil {
		return err
	}
	defer func() { _ = retrievalConn.Close() }()
	orchestratorConn, err := dial("BF_ORCHESTRATOR_ADDR")
	if err != nil {
		return err
	}
	defer func() { _ = orchestratorConn.Close() }()

	batchConn, err := dial("BF_BATCH_ADDR")
	if err != nil {
		return err
	}
	defer func() { _ = batchConn.Close() }()
	batches := batchv1.NewBatchServiceClient(batchConn)

	// 管理接口的实现方：governance（紧急拒绝规则等）
	governanceConn, err := dial("BF_GOVERNANCE_ADDR")
	if err != nil {
		return err
	}
	defer func() { _ = governanceConn.Close() }()
	// evaluation（A/B 分析）
	evaluationConn, err := dial("BF_EVALUATION_ADDR")
	if err != nil {
		return err
	}
	defer func() { _ = evaluationConn.Close() }()
	// indexing（索引版本的切换，入库平面）
	indexingConn, err := dial("BF_INDEXING_ADDR")
	if err != nil {
		return err
	}
	defer func() { _ = indexingConn.Close() }()

	redisAddr, err := env("BF_REDIS_ADDR")
	if err != nil {
		return err
	}
	// 口令来自 Vault 同步的 Secret（生产 Redis 需要认证；开发环境的 Redis 不设口令）
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("BF_REDIS_PASSWORD")})
	defer func() { _ = rdb.Close() }()
	// 默认值是 gateway 详设 5.1 与第 6 节的建议值：调用方应用 20 QPS；用户问答 1 次/秒、突发 5；每用户并发 SSE 3 路；
	// 单实例同时处理的请求上限取单实例并发流的建议上限 5000，全局自适应并发只在下游变慢时从这里往下调
	limits := map[string]int{"BF_GATEWAY_INSTANCES": 1, "BF_CLIENT_RPS": 20, "BF_USER_ANSWER_RPS": 1, "BF_USER_ANSWER_BURST": 5,
		"BF_USER_STREAMS": 3, "BF_MAX_INFLIGHT": 5000}
	for name := range limits {
		if v := os.Getenv(name); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				return fmt.Errorf("%s 不合法: %q", name, v)
			}
			limits[name] = n
		}
	}

	// 租户每日 token 配额按这个时区的自然日计数（部署配置；默认取公司所在地）
	tz, err := time.LoadLocation(envOr("BF_QUOTA_TIMEZONE", "Asia/Shanghai"))
	if err != nil {
		return fmt.Errorf("BF_QUOTA_TIMEZONE 不合法: %w", err)
	}
	instances := limits["BF_GATEWAY_INSTANCES"]

	srv := &api.Server{
		Verifier:      authn.NewVerifier(principal.NewJWKS(jwksURL, nil), platformIssuer, envOr("BF_SERVICE_ACCOUNT_PREFIX", "service-account-")),
		Identities:    identity.NewPGStore(pool),
		Tenants:       tenants,
		Issuer:        iss,
		Retrieval:     retrievalv1.NewRetrievalServiceClient(retrievalConn),
		Orchestrator:  answerv1.NewAnswerServiceClient(orchestratorConn),
		Batches:       batches,
		Denials:       adminv1.NewEmergencyDenialAdminServiceClient(governanceConn),
		IndexVersions: adminv1.NewIndexVersionAdminServiceClient(indexingConn),
		AuditQuery:    adminv1.NewAuditAdminServiceClient(governanceConn),
		Sources:       adminv1.NewSourceAdminServiceClient(governanceConn),
		Experiments:   adminv1.NewExperimentAdminServiceClient(governanceConn),
		Analysis:      adminv1.NewExperimentAnalysisServiceClient(evaluationConn),
		Trends:        adminv1.NewQualityTrendServiceClient(evaluationConn),
		Audit:         auditW,
		Limits:        &ratelimit.Limiter{Redis: rdb, Instances: instances, Log: log},
		Slots:         &admission.Slots{Redis: rdb, Instances: instances, Lease: admission.DefaultLease, Log: log},
		Quota:         &admission.Quota{Redis: rdb, Instances: instances, Location: tz, Log: log, Now: time.Now},
		Overload:      admission.NewAdaptive(limits["BF_MAX_INFLIGHT"], time.Now),
		UserStreams:   limits["BF_USER_STREAMS"],
		ClientRule: ratelimit.Rule{
			Name: "client", Rate: float64(limits["BF_CLIENT_RPS"]), Burst: limits["BF_CLIENT_RPS"],
		},
		UserAnswerRule: ratelimit.Rule{
			Name: "user-answers", Rate: float64(limits["BF_USER_ANSWER_RPS"]), Burst: limits["BF_USER_ANSWER_BURST"],
		},
		PublicURL: publicURL, AuthorizationServer: platformIssuer,
		AdminRoles: adminRoles, Idempotency: &idempotency.Store{Redis: rdb},
		Log: log,
		Now: time.Now,
	}

	internal := http.NewServeMux()
	internal.HandleFunc("GET /internal/jwks.json", iss.JWKS)
	// 存活探针只看进程：依赖故障时重启 gateway 无济于事，只会丢掉本地还没送出的审计段的处理进度
	internal.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	internal.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := pool.Ping(r.Context()); err != nil {
			http.Error(w, "identity store unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	})

	// 内部 gRPC 端口：PrincipalService.Exchange，只供 qa/batch 调用（gateway 详设 3.3）。
	// 不对外暴露；生产环境由服务网格 mTLS 与网络策略限定调用方（技术设计文档 12.6）
	grpcLis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", envOr("BF_INTERNAL_GRPC_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	gs := grpc.NewServer(telemetry.ServerOption())
	identityv1.RegisterPrincipalServiceServer(gs, &api.PrincipalServer{S: srv})
	defer gs.GracefulStop()

	servers := []*http.Server{
		{Addr: envOr("BF_LISTEN", ":8080"), Handler: srv.Handler(), ReadHeaderTimeout: 5 * time.Second},
		{Addr: envOr("BF_INTERNAL_LISTEN", ":8081"), Handler: internal, ReadHeaderTimeout: 5 * time.Second},
	}
	errc := make(chan error, len(servers)+1)
	go func() {
		if err := gs.Serve(grpcLis); err != nil {
			errc <- err
		}
	}()
	for _, s := range servers {
		go func() {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- err
			}
		}()
	}
	log.Info("gateway 已启动", slog.String("kid", iss.KID()))
	select {
	case <-ctx.Done():
	case err := <-errc:
		return err
	}
	// 下线前先停止接新连接，等进行中的请求结束（gateway 详设第 6 节）
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(shutdownCtx)
	}
	return nil
}

func healthcheck(ctx context.Context) error {
	addr := envOr("BF_INTERNAL_LISTEN", ":8081")
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host == "" {
		host = "127.0.0.1"
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/healthz", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("健康检查返回 %d", resp.StatusCode)
	}
	return nil
}
