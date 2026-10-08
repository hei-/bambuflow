// qa/batch 执行异步批量问答（技术设计文档 13.5）：受理 gateway 转来的批次，逐条以调用目的 batch 调 orchestrator，
// 结果存在元数据库的 batch schema 里，保留 7 天；批次结束时发 batch.completed / batch.failed 事件。
//
//	batch migrate      执行表结构迁移
//	batch serve        启动 BatchService（gRPC）与执行循环
//	batch healthcheck  容器健康检查：gRPC 端口可连
//
// 设计里批量问答由 Temporal 工作流编排（ADR-12）；P3 先用元数据库里的条目表与租约实现同样的语义
// （逐条执行、有上限的重试、实例退出后由别的实例接手），引入 Temporal 时只换执行循环，契约与表结构不变。
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

	answerv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/answer/v1"
	batchv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/batch/v1"
	identityv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/identity/v1"
	retrievalv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/retrieval/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
	"github.com/bambu-ai/bambuflow/libs/platform-go/pgmigrate"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
	"github.com/bambu-ai/bambuflow/libs/platform-go/telemetry"
	"github.com/bambu-ai/bambuflow/services/qa/batch/internal/schema"
	"github.com/bambu-ai/bambuflow/services/qa/batch/internal/service"
	"github.com/bambu-ai/bambuflow/services/qa/batch/internal/store"
	"github.com/bambu-ai/bambuflow/services/qa/batch/internal/worker"
)

func main() {
	log := logging.New(os.Stdout, "qa-batch", slog.LevelInfo)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: batch migrate | serve | healthcheck")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "migrate":
		err = migrate(ctx, log)
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

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func migrate(ctx context.Context, log *slog.Logger) error {
	dsn, err := env("BF_PG_DSN")
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	applied, err := pgmigrate.Up(ctx, conn, "qa-batch", schema.Files, "sql")
	log.Info("迁移完成", slog.Any("applied", applied))
	return err
}

// config 读取执行参数。默认值是 P3 的建议值，按容量在部署配置里调整：每实例 4 路并发（批量只用在线容量的空闲部分）、
// 每条最多 5 次、单条 30 秒、退避 2 秒起最长 5 分钟。
func config() (worker.Config, error) {
	c := worker.Config{BackoffBase: 2 * time.Second, BackoffMax: 5 * time.Minute, PollInterval: time.Second}
	var err error
	if c.Concurrency, err = strconv.Atoi(envOr("BF_BATCH_CONCURRENCY", "4")); err != nil || c.Concurrency < 1 {
		return c, fmt.Errorf("BF_BATCH_CONCURRENCY 不合法: %q", os.Getenv("BF_BATCH_CONCURRENCY"))
	}
	if c.MaxAttempts, err = strconv.Atoi(envOr("BF_BATCH_MAX_ATTEMPTS", "5")); err != nil || c.MaxAttempts < 1 || c.MaxAttempts > 20 {
		return c, fmt.Errorf("BF_BATCH_MAX_ATTEMPTS 应为 1–20: %q", os.Getenv("BF_BATCH_MAX_ATTEMPTS"))
	}
	if c.AnswerTimeout, err = time.ParseDuration(envOr("BF_BATCH_ANSWER_TIMEOUT", "30s")); err != nil || c.AnswerTimeout <= 0 {
		return c, fmt.Errorf("BF_BATCH_ANSWER_TIMEOUT 不合法: %q", os.Getenv("BF_BATCH_ANSWER_TIMEOUT"))
	}
	return c, nil
}

func serve(ctx context.Context, log *slog.Logger) error {
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		shutdown, err := telemetry.Setup(ctx, "qa-batch")
		if err != nil {
			return err
		}
		defer func() { _ = shutdown(context.WithoutCancel(ctx)) }()
	}
	cfg, err := config()
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
	jwksURL, err := env("BF_PRINCIPAL_JWKS_URL")
	if err != nil {
		return err
	}
	redisAddr, err := env("BF_REDIS_ADDR")
	if err != nil {
		return err
	}
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("BF_REDIS_PASSWORD")})
	defer func() { _ = rdb.Close() }()

	dial := func(name string) (*grpc.ClientConn, error) {
		addr, err := env(name)
		if err != nil {
			return nil, err
		}
		// 服务间 mTLS 由服务网格提供（技术设计文档 12.6），compose 内是明文
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), telemetry.DialOption())
	}
	conns := map[string]*grpc.ClientConn{}
	for _, name := range []string{"BF_GATEWAY_ADDR", "BF_ORCHESTRATOR_ADDR", "BF_RETRIEVAL_ADDR"} {
		c, err := dial(name)
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()
		conns[name] = c
	}

	st := &store.PG{Pool: pool}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", envOr("BF_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	gs := grpc.NewServer(telemetry.ServerOption(),
		grpc.UnaryInterceptor(grpcmw.UnaryServer(principal.NewVerifier(principal.NewJWKS(jwksURL, nil)), log)))
	batchv1.RegisterBatchServiceServer(gs, &service.Server{
		Store: st, Retrieval: retrievalv1.NewRetrievalServiceClient(conns["BF_RETRIEVAL_ADDR"]), Log: log,
	})

	w := &worker.Worker{
		Store: st, Principals: identityv1.NewPrincipalServiceClient(conns["BF_GATEWAY_ADDR"]),
		Orchestrator: answerv1.NewAnswerServiceClient(conns["BF_ORCHESTRATOR_ADDR"]), Config: cfg, Log: log, Now: time.Now,
	}
	relay := &outbox.Relay{Pool: pool, Redis: rdb, SelectSQL: store.OutboxSelectSQL, MarkSQL: store.OutboxMarkSQL, Log: log}

	done := make(chan struct{})
	go func() { defer close(done); w.Run(ctx) }()
	go relay.Run(ctx, 2*time.Second)
	go cleanup(ctx, st, log)
	errc := make(chan error, 1)
	go func() { errc <- gs.Serve(lis) }()
	log.Info("qa/batch 已启动", slog.Int("concurrency", cfg.Concurrency))
	select {
	case <-ctx.Done():
	case err = <-errc:
	}
	gs.GracefulStop()
	// 正在执行的条目做完再退出；没做完的由租约过期后别的实例接手
	<-done
	return err
}

// cleanup 每小时删除结果已过保留期的批次。
func cleanup(ctx context.Context, st *store.PG, log *slog.Logger) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		n, err := st.DeleteExpired(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			log.WarnContext(ctx, "清理过期批次失败", slog.Any("err", err))
		case n > 0:
			log.InfoContext(ctx, "已清理过期批次", slog.Int64("batches", n))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func healthcheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, port, err := net.SplitHostPort(envOr("BF_LISTEN", ":9090"))
	if err != nil {
		return err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		return errors.Join(errors.New("gRPC 端口不可连"), err)
	}
	return conn.Close()
}
