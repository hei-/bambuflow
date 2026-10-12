// indexing 是唯一写索引的服务（入库管道详设）。P0 范围：表结构迁移、IndexWriterService、发布待发布版本。
//
//	indexing migrate      执行表结构迁移
//	indexing serve        启动 IndexWriterService 与管理接口（gRPC），并转发平台事件（index.version.live）
//
// 索引版本的切换与回滚只经管理接口 /v1/admin/index-versions 双人复核执行（入库管道详设 14.4），没有命令行入口。
//
//	indexing publish      先对待发布版本跑评测门禁，通过后切换为线上版本（prod 别名）
//	indexing expire       按数据源的保留期（BF_RETENTION_FILE）给到期文档写删除标记，每天由调度方运行
//	indexing purge-deleted 物理删除删除标记已超过观察期（BF_PURGE_GRACE_HOURS，默认 168）的文档，写删除凭证
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
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	evaluationv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/evaluation/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/changeaudit"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
	"github.com/hei-/bambuflow/libs/platform-go/outbox"
	"github.com/hei-/bambuflow/libs/platform-go/pgmigrate"
	"github.com/hei-/bambuflow/libs/platform-go/principal"
	"github.com/hei-/bambuflow/libs/platform-go/worm"
	"github.com/hei-/bambuflow/libs/textseg"
	"github.com/hei-/bambuflow/services/indexing/internal/access"
	"github.com/hei-/bambuflow/services/indexing/internal/schema"
	"github.com/hei-/bambuflow/services/indexing/internal/writer"
)

func main() {
	log := logging.New(os.Stdout, "indexing", slog.LevelInfo)
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "用法: indexing migrate | serve | publish | expire | purge-deleted | healthcheck")
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
	case "expire":
		err = expire(ctx, log)
	case "purge-deleted":
		err = purgeDeleted(ctx, log)
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
	// 管理接口（索引版本切换、撤销删除标记、全量重建）只接受 gateway 签发的管理 Principal（gateway 详设 4.2）
	jwksURL := os.Getenv("BF_PRINCIPAL_JWKS_URL")
	if jwksURL == "" {
		return errors.New("未设置 BF_PRINCIPAL_JWKS_URL")
	}
	// IndexWriterService 的调用方是 processing（服务身份，经服务网格 mTLS），不是终端用户，所以不校验 Principal
	s := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServerFor([]string{"bambuflow.admin.v1.IndexVersionAdminService", "bambuflow.admin.v1.TombstoneAdminService",
		"bambuflow.admin.v1.RebuildAdminService", "bambuflow.admin.v1.DeletionReceiptAdminService"},
		principal.NewOperatorVerifier(principal.NewJWKS(jwksURL, nil)), log)))
	// 转发发件表里的平台事件（index.version.live）到 Redis 事件流；Redis 暂时不可用时事件留在发件表里，恢复后补发
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr, Password: os.Getenv("BF_REDIS_PASSWORD")})
	defer func() { _ = rdb.Close() }()
	ws := &writer.Server{Pool: p, Log: log, Notify: writer.ChunkAclNotifier(rdb)}
	ingestionv1.RegisterIndexWriterServiceServer(s, ws)
	relay := &outbox.Relay{Pool: p, Redis: rdb, SelectSQL: writer.OutboxSelectSQL, MarkSQL: writer.OutboxMarkSQL, Log: log}
	go relay.Run(ctx, 2*time.Second)
	// RefetchRequested（撤销删除标记后要求 connectors 重新拉取）发到入库平面自己的事件流，不进平台事件流
	refetch := &outbox.Relay{Pool: p, Redis: rdb, SelectSQL: writer.RefetchOutboxSelectSQL, MarkSQL: writer.RefetchOutboxMarkSQL,
		Log: log, Stream: writer.RefetchStream}
	go refetch.Run(ctx, 2*time.Second)
	// DocumentPurged（物理删除后要求 connectors 删对象存储里的原文）同样发到入库平面自己的事件流
	purged := &outbox.Relay{Pool: p, Redis: rdb, SelectSQL: writer.PurgeOutboxSelectSQL, MarkSQL: writer.PurgeOutboxMarkSQL,
		Log: log, Stream: writer.PurgeStream}
	go purged.Run(ctx, 2*time.Second)
	// 权限与删除的快速通道：直接消费 connectors 发出的删除与仅权限变化事件，不经过 processing（入库管道详设第 9 节）
	host, err := os.Hostname()
	if err != nil {
		return err
	}
	go (&access.Consumer{Redis: rdb, Writer: ws, Name: host, Log: log}).Run(ctx)
	audit, err := changes(ctx)
	if err != nil {
		return err
	}
	adminv1.RegisterIndexAdminServiceServer(s, &writer.Admin{Pool: p, Audit: audit, Log: log})
	adminv1.RegisterIndexVersionAdminServiceServer(s, &writer.VersionAdmin{Pool: p, Log: log, Now: time.Now})
	adminv1.RegisterTombstoneAdminServiceServer(s, &writer.TombstoneAdmin{Pool: p, Log: log, Now: time.Now})
	adminv1.RegisterRebuildAdminServiceServer(s, &writer.RebuildAdmin{Pool: p, Log: log, Now: time.Now})
	adminv1.RegisterDeletionReceiptAdminServiceServer(s, &writer.DeletionReceiptAdmin{Pool: p, Log: log})
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
	// 先在待发布版本上跑评测门禁，通过才切别名（入库管道详设第 10 节，evaluation 详设 5.1）：
	// 门禁不通过或拿不到结论都不发布，变更留在下一批；没有跳过门禁的入口
	evalAddr := os.Getenv("BF_EVAL_ADDR")
	if evalAddr == "" {
		return errors.New("未设置 BF_EVAL_ADDR：没有评测门禁不发布")
	}
	conn, err := grpc.NewClient(evalAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	incremental, rebuild, err := writer.PendingVersions(ctx, p)
	if err != nil {
		return err
	}
	audit, err := changes(ctx)
	if err != nil {
		return err
	}
	gate := &writer.Gate{Client: evaluationv1.NewEvalServiceClient(conn), Every: 5 * time.Second, Budget: 6 * time.Minute, Log: log}
	// 全量重建的版本写完全部在用文档后跑完整评测集门禁，通过就上线（增量版本随之作废）；没写完或门禁还在跑就照常发布增量版本
	if rebuild != nil {
		published, err := publishRebuild(ctx, p, gate, rebuild.Version, audit, log)
		if err != nil || published {
			return err
		}
	}
	if incremental == nil {
		log.Info("没有待发布的变更，线上版本保持不变")
		return nil
	}
	live, err := writer.HasLive(ctx, p)
	if err != nil {
		return err
	}
	// 门禁评测上线后的完整版本：先把线上版本里没变的文档带入，文档数随之更新（幂等键按它区分批次）
	before, err := writer.CarryForward(ctx, p, incremental.Version)
	if err != nil {
		return err
	}
	if err := gate.Check(ctx, incremental.Version, incremental.Docs+before, !live); err != nil {
		return err
	}
	carried, err := writer.Publish(ctx, p, incremental.Version, audit)
	if errors.Is(err, writer.ErrNoPending) {
		// 门禁期间版本已被别的发布处理（例如重建上线时作废）
		log.Info("没有待发布的变更，线上版本保持不变")
		return nil
	}
	if err != nil {
		return err
	}
	log.Info("已发布", slog.String("version", incremental.Version), slog.Int64("carried_from_live", before+carried))
	return nil
}

func publishRebuild(ctx context.Context, p *pgxpool.Pool, gate *writer.Gate, version string, audit writer.Auditor, log *slog.Logger) (bool, error) {
	if err := writer.RebuildComplete(ctx, p, version); err != nil {
		if errors.Is(err, writer.ErrRebuildIncomplete) {
			log.Info("全量重建进行中，本轮不发布重建版本", slog.Any("progress", err))
			return false, nil
		}
		return false, err
	}
	passed, err := gate.CheckRebuild(ctx, version)
	if err != nil || !passed {
		return false, err
	}
	if _, err := writer.Publish(ctx, p, version, audit); err != nil {
		if errors.Is(err, writer.ErrRebuildIncomplete) {
			// 门禁期间又有内容变更，新模型的 processing 还没跟上：下一轮再发布
			log.Info("全量重建还有未补齐的变更，本轮不发布", slog.Any("progress", err))
			return false, nil
		}
		return false, err
	}
	log.Info("全量重建已上线", slog.String("version", version))
	return true, nil
}

// expire 按部署配置的保留期给到期文档写删除标记（技术设计文档 9.2）；没有配置文件时不按时间过期。
func expire(ctx context.Context, log *slog.Logger) error {
	path := os.Getenv("BF_RETENTION_FILE")
	if path == "" {
		log.Info("没有配置数据源的保留期，不按时间过期")
		return nil
	}
	retention, err := writer.LoadRetention(path)
	if err != nil {
		return err
	}
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	audit, err := changes(ctx)
	if err != nil {
		return err
	}
	got, err := writer.Expire(ctx, p, retention, time.Now(), audit)
	if err != nil {
		return err
	}
	log.Info("已按保留期写删除标记", slog.Any("tombstoned", got))
	return nil
}

// purgeDeleted 物理删除过了观察期的删除标记文档在索引侧的全部副本（技术设计文档 9.2）。
func purgeDeleted(ctx context.Context, log *slog.Logger) error {
	grace := writer.DefaultPurgeGrace
	if v := os.Getenv("BF_PURGE_GRACE_HOURS"); v != "" {
		h, err := strconv.Atoi(v)
		if err != nil || h <= 0 {
			return fmt.Errorf("BF_PURGE_GRACE_HOURS 应为正整数：%q", v)
		}
		grace = time.Duration(h) * time.Hour
	}
	p, err := pool(ctx)
	if err != nil {
		return err
	}
	defer p.Close()
	audit, err := changes(ctx)
	if err != nil {
		return err
	}
	n, err := writer.PurgeDeleted(ctx, p, grace, time.Now(), audit)
	if err != nil {
		return err
	}
	log.Info("已物理删除到期的删除标记文档", slog.Int64("docs", n), slog.Duration("grace", grace))
	return nil
}

// changes 是 indexing 的变更审计：发布、按保留期过期、物理删除写进审计桶（与 gateway 入口审计同一个桶）。
// 审计桶连不上或没开对象锁时这些操作都不做（fail-closed）。
func changes(ctx context.Context) (*changeaudit.Writer, error) {
	store, err := worm.OpenFromEnv(ctx)
	if err != nil {
		return nil, fmt.Errorf("连接审计桶: %w", err)
	}
	return &changeaudit.Writer{Store: store, Service: "indexing", Now: time.Now}, nil
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
