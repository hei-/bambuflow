// governance 现在的范围：表结构迁移、身份同步（技术设计文档 8.1、gateway 详设 3.5）、
// 受控词表与它的快照下发（governance 详设 6.1、7.1）、Webhook 事件投递（技术设计文档 13.7）、
// 经 gateway /v1/admin 调用的管理接口（紧急拒绝规则等，governance 详设 7.2）。配置发布、审批、策略包等功能在后续阶段加入。
//
//	governance migrate           执行表结构迁移（升级前以 Job 运行）
//	governance identity-sync     常驻运行身份同步
//	governance healthcheck       容器健康检查：身份同步在 3 个周期内成功过
//	governance vocab-load <文件>  写入词表文件里的词条（开发与 CI 的种子；生产经管理接口维护）
//	governance sources-load <文件> 写入数据源与租户授权的种子（开发与 CI；生产经管理接口维护）
//	governance serve             常驻运行 ConfigService 与管理接口（gRPC）
//	governance serve-healthcheck 容器健康检查：能取到快照
//	governance webhook           常驻运行 Webhook 投递：消费平台事件流、按订阅签名投递，并提供 WebhookAdminService（gRPC）
//	governance webhook-healthcheck 容器健康检查：WebhookAdminService 端口可连
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	configv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/config/v1"

	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
	"github.com/bambu-ai/bambuflow/libs/platform-go/pgmigrate"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
	"github.com/bambu-ai/bambuflow/libs/platform-go/telemetry"
	"github.com/bambu-ai/bambuflow/libs/platform-go/vault"
	"github.com/bambu-ai/bambuflow/libs/platform-go/worm"
	"github.com/bambu-ai/bambuflow/services/governance/internal/access"
	"github.com/bambu-ai/bambuflow/services/governance/internal/auditquery"
	"github.com/bambu-ai/bambuflow/services/governance/internal/changeaudit"
	"github.com/bambu-ai/bambuflow/services/governance/internal/experiment"
	"github.com/bambu-ai/bambuflow/services/governance/internal/identitysync"
	"github.com/bambu-ai/bambuflow/services/governance/internal/schema"
	"github.com/bambu-ai/bambuflow/services/governance/internal/vocab"
	"github.com/bambu-ai/bambuflow/services/governance/internal/webhook"
)

func main() {
	log := logging.New(os.Stdout, "governance", slog.LevelInfo)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: governance migrate | identity-sync | healthcheck | vocab-load <文件> | sources-load <文件> | serve | serve-healthcheck | webhook | webhook-healthcheck")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "migrate":
		err = runMigrate(ctx, log)
	case "identity-sync":
		err = runIdentitySync(ctx, log)
	case "healthcheck":
		err = healthcheck(ctx)
	case "vocab-load":
		if len(os.Args) != 3 {
			err = errors.New("用法: governance vocab-load <文件>")
			break
		}
		err = runVocabLoad(ctx, log, os.Args[2])
	case "sources-load":
		if len(os.Args) != 3 {
			err = errors.New("用法: governance sources-load <文件>")
			break
		}
		err = runSourcesLoad(ctx, log, os.Args[2])
	case "serve":
		err = runServe(ctx, log)
	case "serve-healthcheck":
		err = serveHealthcheck(ctx)
	case "webhook":
		err = runWebhook(ctx, log)
	case "webhook-healthcheck":
		err = webhookHealthcheck(ctx)
	default:
		err = fmt.Errorf("未知子命令 %q", os.Args[1])
	}
	if err != nil {
		log.Error("退出", slog.Any("err", err))
		os.Exit(1)
	}
}

func dsn() (string, error) {
	d := os.Getenv("BF_PG_DSN")
	if d == "" {
		return "", errors.New("未设置 BF_PG_DSN")
	}
	return d, nil
}

func runMigrate(ctx context.Context, log *slog.Logger) error {
	d, err := dsn()
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, d)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	applied, err := pgmigrate.Up(ctx, conn, "governance", schema.Files, "sql")
	log.Info("迁移完成", slog.Any("applied", applied))
	return err
}

func runIdentitySync(ctx context.Context, log *slog.Logger) error {
	cfg, err := identitysync.LoadConfig(os.Getenv("BF_IDENTITY_SYNC_CONFIG"))
	if err != nil {
		return err
	}
	dir, err := identitysync.NewLDAPDirectory(cfg)
	if err != nil {
		return err
	}
	d, err := dsn()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, d)
	if err != nil {
		return err
	}
	defer pool.Close()
	identitysync.NewSyncer(cfg, dir, identitysync.NewPGStore(pool), log).Run(ctx)
	return nil
}

func runVocabLoad(ctx context.Context, log *slog.Logger, path string) error {
	terms, err := vocab.LoadFile(path)
	if err != nil {
		return err
	}
	d, err := dsn()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, d)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := (vocab.Store{Pool: pool}).Upsert(ctx, terms); err != nil {
		return err
	}
	log.Info("词表已写入", slog.Int("terms", len(terms)))
	return nil
}

func runServe(ctx context.Context, log *slog.Logger) error {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.Setup(ctx, "governance")
		if err != nil {
			return err
		}
		defer func() { _ = shutdown(context.WithoutCancel(ctx)) }()
	}
	d, err := dsn()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, d)
	if err != nil {
		return err
	}
	defer pool.Close()
	// 管理接口只接受 gateway 签发的管理 Principal（gateway 详设 4.2）
	jwksURL, err := env("BF_PRINCIPAL_JWKS_URL")
	if err != nil {
		return err
	}
	// 变更审计写只追加的审计桶，写不进去就不启动（governance 详设 9.3）；审计查询也从这个桶读入口审计段
	auditBucket, err := auditStore(ctx)
	if err != nil {
		return err
	}
	changes := &changeaudit.Writer{Store: auditBucket, Now: time.Now}
	// 紧急拒绝规则经 indexing 写进索引库的拒绝名单：只有 indexing 写索引库
	indexAddr, err := env("BF_INDEXING_ADDR")
	if err != nil {
		return err
	}
	// 平台事件流（撤销数据源的 source.revoked）
	redisAddr, err := env("BF_REDIS_ADDR")
	if err != nil {
		return err
	}
	indexConn, err := grpc.NewClient(indexAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), telemetry.DialOption())
	if err != nil {
		return err
	}
	defer func() { _ = indexConn.Close() }()

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", envOr("BF_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	store := &access.PG{Pool: pool}
	denials := &access.DenialServer{Store: store, Audit: changes, Index: adminv1.NewIndexAdminServiceClient(indexConn), Log: log}
	sources := &access.SourceServer{Store: store, Audit: changes, Denials: denials, WriteEvent: access.WriteEvent, Log: log, Now: time.Now}
	experiments := &experiment.Server{Store: &experiment.PG{Pool: pool}, Audit: changes, Log: log, Now: time.Now}
	// 服务间 mTLS 由服务网格提供（技术设计文档 12.6）；ConfigService 与 ExperimentControlService 是服务间接口，不要求 Principal
	gs := grpc.NewServer(telemetry.ServerOption(), grpc.UnaryInterceptor(grpcmw.UnaryServerFor(
		[]string{"bambuflow.admin.v1.EmergencyDenialAdminService", "bambuflow.admin.v1.AuditAdminService",
			"bambuflow.admin.v1.SourceAdminService", "bambuflow.admin.v1.ExperimentAdminService"},
		principal.NewOperatorVerifier(principal.NewJWKS(jwksURL, nil)), log)))
	configv1.RegisterConfigServiceServer(gs, &vocab.Server{Store: vocab.Store{Pool: pool}, Retrieval: sources.Snapshot,
		Experiments: experiments.Snapshot, Log: log})
	adminv1.RegisterSourceAdminServiceServer(gs, sources)
	adminv1.RegisterEmergencyDenialAdminServiceServer(gs, denials)
	adminv1.RegisterAuditAdminServiceServer(gs, &auditquery.Server{Store: auditBucket, Audit: changes, Log: log})
	adminv1.RegisterExperimentAdminServiceServer(gs, experiments)
	configv1.RegisterExperimentControlServiceServer(gs, &experiment.ControlServer{Experiments: experiments})
	errc := make(chan error, 1)
	go func() { errc <- gs.Serve(lis) }()
	// 还没写进拒绝名单的规则每 10 秒重试一次，1 分钟内生效的要求留出余量（governance 详设 4.2）
	go denials.Reconcile(ctx, 10*time.Second)
	// 发件表里的平台事件（source.revoked）转发到事件流；Redis 暂时不可用时事件留在发件表里，恢复后补发
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("BF_REDIS_PASSWORD")})
	defer func() { _ = rdb.Close() }()
	go (&outbox.Relay{Pool: pool, Redis: rdb, SelectSQL: outboxSelectSQL, MarkSQL: outboxMarkSQL, Log: log}).Run(ctx, 2*time.Second)
	log.Info("ConfigService 与管理接口已启动")
	select {
	case <-ctx.Done():
		gs.GracefulStop()
		return nil
	case err := <-errc:
		return err
	}
}

// 发件表的转发语句（outbox.Relay）。
const (
	outboxSelectSQL = `SELECT event_id, payload FROM governance.outbox WHERE sent_at IS NULL ORDER BY created_at LIMIT $1 FOR UPDATE SKIP LOCKED`
	outboxMarkSQL   = `UPDATE governance.outbox SET sent_at = now() WHERE event_id = ANY($1)`
)

func env(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("未设置 %s", name)
	}
	return v, nil
}

// auditStore 连接审计桶（与 gateway 的入口审计同一个桶，开启对象锁）。
func auditStore(ctx context.Context) (*worm.Store, error) {
	vals := make([]string, 4)
	for i, name := range []string{"BF_AUDIT_S3_ENDPOINT", "BF_AUDIT_S3_ACCESS_KEY", "BF_AUDIT_S3_SECRET_KEY", "BF_AUDIT_BUCKET"} {
		v, err := env(name)
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	days, err := strconv.Atoi(envOr("BF_AUDIT_RETENTION_DAYS", "2555"))
	if err != nil || days < 1 {
		return nil, fmt.Errorf("BF_AUDIT_RETENTION_DAYS 不合法: %q", os.Getenv("BF_AUDIT_RETENTION_DAYS"))
	}
	return worm.Open(ctx, vals[0], vals[1], vals[2], vals[3], time.Duration(days)*24*time.Hour)
}

func runSourcesLoad(ctx context.Context, log *slog.Logger, path string) error {
	seed, err := access.LoadSeed(path)
	if err != nil {
		return err
	}
	d, err := dsn()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, d)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := (&access.PG{Pool: pool}).Seed(ctx, seed); err != nil {
		return err
	}
	log.Info("数据源种子已写入", slog.Int("sources", len(seed)))
	return nil
}

func serveHealthcheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.NewClient("127.0.0.1"+envOr("BF_LISTEN", ":9090"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	_, err = configv1.NewConfigServiceClient(conn).GetSnapshot(ctx, &configv1.GetSnapshotRequest{Service: "healthcheck"})
	return err
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func healthcheck(ctx context.Context) error {
	cfg, err := identitysync.LoadConfig(os.Getenv("BF_IDENTITY_SYNC_CONFIG"))
	if err != nil {
		return err
	}
	d, err := dsn()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, d)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var fresh bool
	err = conn.QueryRow(ctx, `SELECT COALESCE(now() - last_success_at < make_interval(secs => $2), false) FROM identity.sync_state WHERE source = $1`,
		cfg.Source, (3 * cfg.Interval).Seconds()).Scan(&fresh)
	if err != nil {
		return fmt.Errorf("读取身份同步状态: %w", err)
	}
	if !fresh {
		return fmt.Errorf("身份同步在 %s 内没有成功过", 3*cfg.Interval)
	}
	return nil
}

// runWebhook 常驻运行 Webhook 投递（技术设计文档 13.7）：消费平台事件流登记投递、按订阅签名发送，
// 并提供订阅管理接口。签名密钥存在 Vault，库里只记路径。
func runWebhook(ctx context.Context, log *slog.Logger) error {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.Setup(ctx, "governance-webhook")
		if err != nil {
			return err
		}
		defer func() { _ = shutdown(context.WithoutCancel(ctx)) }()
	}
	d, err := dsn()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, d)
	if err != nil {
		return err
	}
	defer pool.Close()
	vc, err := vault.FromEnv()
	if err != nil {
		return err
	}
	redisAddr := os.Getenv("BF_REDIS_ADDR")
	if redisAddr == "" {
		return errors.New("未设置 BF_REDIS_ADDR")
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("BF_REDIS_PASSWORD")})
	defer func() { _ = rdb.Close() }()
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	mount := envOr("BF_WEBHOOK_SECRET_MOUNT", "secret")
	store := &webhook.PG{Pool: pool}

	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", envOr("BF_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	gs := grpc.NewServer(telemetry.ServerOption())
	adminv1.RegisterWebhookAdminServiceServer(gs, &webhook.Admin{
		// 只有开发与 CI 的 compose 放开 http 回调地址（e2e 的接收方在容器网络里）
		Store: store, Secrets: vc, Mount: mount, AllowHTTP: os.Getenv("BF_WEBHOOK_ALLOW_HTTP") == "true", Log: log,
	})
	consumer := &webhook.Consumer{Redis: rdb, Store: store, Name: host, Log: log}
	deliverer := &webhook.Deliverer{Store: store, Secrets: vc, Mount: mount, HTTP: webhook.NewHTTPClient(), Log: log,
		Now: time.Now, Concurrency: 8, PollInterval: time.Second}

	errc := make(chan error, 2)
	go func() { errc <- gs.Serve(lis) }()
	go func() { errc <- consumer.Run(ctx) }()
	done := make(chan struct{})
	go func() { defer close(done); deliverer.Run(ctx) }()
	log.Info("Webhook 投递已启动")
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	gs.GracefulStop()
	<-done
	return err
}

func webhookHealthcheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1"+envOr("BF_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	return conn.Close()
}
