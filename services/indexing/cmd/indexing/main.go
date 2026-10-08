// indexing 是唯一写索引的服务（入库管道详设）。P0 范围：表结构迁移、IndexWriterService、发布待发布版本。
//
//	indexing migrate      执行表结构迁移
//	indexing serve        启动 IndexWriterService 与管理接口（gRPC），并转发平台事件（index.version.live）
//
// 索引版本的切换与回滚只经管理接口 /v1/admin/index-versions 双人复核执行（入库管道详设 14.4），没有命令行入口。
//
//	indexing publish      把待发布版本切换为线上版本（prod 别名）
//	indexing healthcheck  容器健康检查：gRPC 端口可连
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"

	adminv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	ingestionv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/grpcmw"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/libs/platform-go/outbox"
	"github.com/bambu-ai/bambuflow/libs/platform-go/pgmigrate"
	"github.com/bambu-ai/bambuflow/libs/platform-go/principal"
	"github.com/bambu-ai/bambuflow/libs/textseg"
	"github.com/bambu-ai/bambuflow/services/indexing/internal/schema"
	"github.com/bambu-ai/bambuflow/services/indexing/internal/writer"
)

func main() {
	log := logging.New(os.Stdout, "indexing", slog.LevelInfo)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: indexing migrate | serve | publish | healthcheck")
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
	case "publish":
		err = publish(ctx, log)
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

func dsn() (string, error) {
	d := os.Getenv("BF_PG_DSN")
	if d == "" {
		return "", errors.New("未设置 BF_PG_DSN")
	}
	return d, nil
}

func listenAddr() string {
	if v := os.Getenv("BF_LISTEN"); v != "" {
		return v
	}
	return ":9090"
}

func pool(ctx context.Context) (*pgxpool.Pool, error) {
	d, err := dsn()
	if err != nil {
		return nil, err
	}
	cfg, err := pgxpool.ParseConfig(d)
	if err != nil {
		return nil, err
	}
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error { return pgxvec.RegisterTypes(ctx, c) }
	return pgxpool.NewWithConfig(ctx, cfg)
}

func migrate(ctx context.Context, log *slog.Logger) error {
	d, err := dsn()
	if err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, d)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	applied, err := pgmigrate.Up(ctx, conn, "indexing", schema.Files, "sql")
	log.Info("迁移完成", slog.Any("applied", applied))
	return err
}

func serve(ctx context.Context, log *slog.Logger) error {
	// 分词词典启动时加载一次；加载失败不接收写入，否则关键词索引会悄悄缺词
	if err := textseg.Load(); err != nil {
		return fmt.Errorf("加载分词词典: %w", err)
	}
	redisAddr := os.Getenv("BF_REDIS_ADDR")
	if redisAddr == "" {
		return errors.New("未设置 BF_REDIS_ADDR")
	}
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", listenAddr())
	if err != nil {
		return err
	}
	// 管理接口（索引版本切换）只接受 gateway 签发的管理 Principal（gateway 详设 4.2）
	jwksURL := os.Getenv("BF_PRINCIPAL_JWKS_URL")
	if jwksURL == "" {
		return errors.New("未设置 BF_PRINCIPAL_JWKS_URL")
	}
	// IndexWriterService 的调用方是 processing（服务身份，经服务网格 mTLS），不是终端用户，所以不校验 Principal
	s := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServerFor([]string{"bambuflow.admin.v1.IndexVersionAdminService"},
		principal.NewOperatorVerifier(principal.NewJWKS(jwksURL, nil)), log)))
	ingestionv1.RegisterIndexWriterServiceServer(s, &writer.Server{Pool: p, Log: log})
	// 转发发件表里的平台事件（index.version.live）到 Redis 事件流；Redis 暂时不可用时事件留在发件表里，恢复后补发
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("BF_REDIS_PASSWORD")})
	defer func() { _ = rdb.Close() }()
	relay := &outbox.Relay{Pool: p, Redis: rdb, SelectSQL: writer.OutboxSelectSQL, MarkSQL: writer.OutboxMarkSQL, Log: log}
	go relay.Run(ctx, 2*time.Second)
	adminv1.RegisterIndexAdminServiceServer(s, &writer.Admin{Pool: p, Log: log})
	adminv1.RegisterIndexVersionAdminServiceServer(s, &writer.VersionAdmin{Pool: p, Log: log, Now: time.Now})
	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()
	log.Info("indexing 已启动")
	return s.Serve(lis)
}

func publish(ctx context.Context, log *slog.Logger) error {
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	version, carried, err := writer.Publish(ctx, p)
	if errors.Is(err, writer.ErrNoPending) {
		// 增量同步本轮没有变更：线上版本保持不变
		log.Info("没有待发布的变更，线上版本保持不变")
		return nil
	}
	if err != nil {
		return err
	}
	log.Info("已发布", slog.String("version", version), slog.Int64("carried_from_live", carried))
	return nil
}

func healthcheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1"+listenAddr())
	if err != nil {
		return err
	}
	return conn.Close()
}
