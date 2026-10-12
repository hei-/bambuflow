// connectors 负责变更检测与原文、ACL 拉取（入库管道详设第 5 节）。样例源按全量清单增量同步：
// 新增与内容变化发 UPSERT，只有权限变化发 ACL_ONLY，源端消失的发 DELETE（带删除保护）。
//
//	connectors sync-wiki       读取 Wiki 样例目录，为每个页面写文档包并发出 DocumentChanged
//	connectors sync-fileshare  读取共享盘样例目录（PDF、Office、邮件、图片），同上
//	connectors sync-logs       读取日志平台消息主题里的新消息，按服务一天一份文档发出（internal/logs）
//	connectors sync-dbschema   读取白名单视图的表结构与字段说明，一个视图一份文档发出（internal/dbschema，部署时每小时一次）
//	connectors sync-records    按表开启的结构化记录入库：一行一张记录卡片（internal/records）
//	connectors reconcile-wiki、reconcile-fileshare
//	                           每日对账：先与 indexing 已入库的文档比对、修正已发出状态（internal/reconcile），再照常同步一轮
//	connectors serve-admin     常驻的数据源运维接口：同步状态、暂停与恢复、待删批次（internal/admin）与推送入库（internal/push）；
//	                           并消费平台事件 source.revoked，对撤销的数据源全部文档写删除标记（internal/revoke），
//	                           消费 DocumentPurged，删掉物理删除了的文档在对象存储里的原文（internal/rawpurge）
//	connectors poll <同步命令>  常驻的清单轮询（internal/poll）：按 5–15 分钟自适应的间隔反复跑上面某个 sync-* 命令，
//	                           源系统的 Webhook（配置了 BF_WEBHOOK_SECRET 时在 /webhook 接收，internal/hook）
//	                           或立即同步会提前唤醒一轮
//	connectors healthcheck     容器健康检查：管理服务（或轮询进程）的端口可连
//
// 每轮同步的结果写进同步状态（internal/syncstatus）；数据源暂停时整轮跳过。
// sync-wiki、sync-fileshare 设了 BF_PARTITIONS_FILE 时按分区级数据源处理（internal/partition）。
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	adminv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/admin/v1"
	ingestionv1 "github.com/hei-/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/hei-/bambuflow/libs/platform-go/changeaudit"
	"github.com/hei-/bambuflow/libs/platform-go/grpcmw"
	"github.com/hei-/bambuflow/libs/platform-go/logging"
	"github.com/hei-/bambuflow/libs/platform-go/principal"
	"github.com/hei-/bambuflow/libs/platform-go/worm"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/admin"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/dbschema"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/docmeta"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/fileshare"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/holds"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/hook"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/incremental"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/logs"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/partition"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/poll"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/publish"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/push"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/rawpurge"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/reconcile"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/records"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/refetch"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/revoke"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/syncstatus"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/trigger"
	"github.com/hei-/bambuflow/services/ingestion/connectors/internal/wiki"
)

// lister 读取一个样例源的全部文档；样例目录与 source_uri 前缀由环境变量给出。没能读出的项在 skipped 里。
type lister func(dir, baseURI string) ([]*publish.Document, []docmeta.Skipped, error)

var commands = map[string]struct {
	dirEnv, uriEnv string
	list           lister
	reconcile      bool
}{
	"sync-wiki":           {"BF_WIKI_DIR", "BF_WIKI_BASE_URI", listWiki, false},
	"sync-fileshare":      {"BF_FILESHARE_DIR", "BF_FILESHARE_BASE_URI", listFileshare, false},
	"reconcile-wiki":      {"BF_WIKI_DIR", "BF_WIKI_BASE_URI", listWiki, true},
	"reconcile-fileshare": {"BF_FILESHARE_DIR", "BF_FILESHARE_BASE_URI", listFileshare, true},
}

func main() {
	log := logging.New(os.Stdout, "connectors", slog.LevelInfo)
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	sync, ok := syncCommand(os.Args[1])
	switch {
	case os.Args[1] == "serve-admin":
		err = serveAdmin(ctx, log)
	case os.Args[1] == "healthcheck":
		err = healthcheck(ctx)
	case os.Args[1] == "poll" && len(os.Args) == 3:
		err = runPoll(ctx, log, os.Args[2])
	case ok:
		err = recorded(ctx, log, os.Args[1], sync)
	default:
		usage()
	}
	if err != nil {
		log.Error("退出", slog.Any("err", err))
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法: connectors sync-wiki | sync-fileshare | sync-logs | sync-dbschema | sync-records | reconcile-wiki | reconcile-fileshare | "+
		"poll <sync-*> | serve-admin | healthcheck")
	os.Exit(2)
}

type syncFunc func(ctx context.Context, log *slog.Logger, run *syncstatus.Run) error

// syncCommand 取同步或对账命令的实现。
func syncCommand(name string) (syncFunc, bool) {
	switch name {
	case "sync-logs":
		return syncLogs, true
	case "sync-dbschema":
		return syncDBSchema, true
	case "sync-records":
		return syncRecords, true
	}
	cmd, ok := commands[name]
	if !ok {
		return nil, false
	}
	return func(ctx context.Context, log *slog.Logger, run *syncstatus.Run) error {
		return syncSource(ctx, log, run, cmd.dirEnv, cmd.uriEnv, cmd.list, cmd.reconcile)
	}, true
}

// runPoll 是常驻的清单轮询（internal/poll）。对账仍按每日定时任务跑，不进轮询。
func runPoll(ctx context.Context, log *slog.Logger, command string) error {
	sync, ok := syncCommand(command)
	if !ok || !strings.HasPrefix(command, "sync-") {
		return fmt.Errorf("poll 只能跑 sync-* 命令，收到 %q", command)
	}
	vals, err := envs("BF_SOURCE_ID", "BF_REDIS_ADDR")
	if err != nil {
		return err
	}
	sourceID := vals[0]
	pacer, err := pacerFromEnv()
	if err != nil {
		return err
	}
	rdb := newRedis(vals[1])
	defer func() { _ = rdb.Close() }()
	store := syncstatus.Store{Client: rdb}

	// 端口总是开着（健康检查连它）；配置了密钥才接收源系统的 Webhook
	mux := http.NewServeMux()
	if secrets, err := webhookSecrets(); err != nil {
		return err
	} else if secrets != nil {
		mux.Handle("/webhook", &hook.Handler{Secrets: secrets, Log: log, Now: time.Now,
			Wake: func(ctx context.Context) error { return poll.Wake(ctx, rdb, sourceID) }})
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", listenAddr())
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Webhook 接收退出", slog.Any("err", err))
		}
	}()
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()

	log.Info("清单轮询已启动", slog.String("source", sourceID), slog.String("command", command),
		slog.Duration("min", pacer.Min), slog.Duration("max", pacer.Max))
	loop := &poll.Loop{
		Pacer: pacer, Waker: poll.RedisWaker{Client: rdb, SourceID: sourceID}, Log: log, Now: time.Now,
		Round:  func(ctx context.Context) error { return recorded(ctx, log, command, sync) },
		Status: pollStatus{store: store, sourceID: sourceID},
	}
	return loop.Run(ctx)
}

type pollStatus struct {
	store    syncstatus.Store
	sourceID string
}

func (p pollStatus) SetPolling(ctx context.Context, interval time.Duration, next time.Time) error {
	return p.store.SetPolling(ctx, p.sourceID, syncstatus.Polling{IntervalSeconds: int64(interval.Seconds()), NextRoundAt: next})
}

// pacerFromEnv 取轮询间隔的上下限（BF_POLL_MIN、BF_POLL_MAX，Go 的时长写法），不设时用建议值 5 与 15 分钟。
func pacerFromEnv() (poll.Pacer, error) {
	p := poll.DefaultPacer
	for name, dst := range map[string]*time.Duration{"BF_POLL_MIN": &p.Min, "BF_POLL_MAX": &p.Max} {
		if v := os.Getenv(name); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d < time.Minute {
				return poll.Pacer{}, fmt.Errorf("%s 须是不短于 1 分钟的时长，收到 %q", name, v)
			}
			*dst = d
		}
	}
	if p.Max < p.Min {
		return poll.Pacer{}, errors.New("BF_POLL_MAX 不能小于 BF_POLL_MIN")
	}
	return p, nil
}

// webhookSecrets 取源系统 Webhook 的签名密钥（经 Vault 注入）：BF_WEBHOOK_SECRET，轮换期间另有 BF_WEBHOOK_SECRET_PREVIOUS。
// 都没有时返回 nil（不接收 Webhook）。
func webhookSecrets() ([]string, error) {
	if os.Getenv("BF_WEBHOOK_SECRET") == "" && os.Getenv("BF_WEBHOOK_SECRET_PREVIOUS") != "" {
		return nil, errors.New("只设了 BF_WEBHOOK_SECRET_PREVIOUS，没有 BF_WEBHOOK_SECRET")
	}
	var out []string
	for _, name := range []string{"BF_WEBHOOK_SECRET", "BF_WEBHOOK_SECRET_PREVIOUS"} {
		v := os.Getenv(name)
		if v == "" {
			continue
		}
		if len(v) < hook.MinSecret {
			return nil, fmt.Errorf("%s 至少 %d 个字符", name, hook.MinSecret)
		}
		out = append(out, v)
	}
	return out, nil
}

// recorded 跑一轮同步并把结果写进同步状态；数据源暂停时整轮跳过（入库管道详设 14.4）。
func recorded(ctx context.Context, log *slog.Logger, command string, sync syncFunc) error {
	vals, err := envs("BF_SOURCE_ID", "BF_REDIS_ADDR")
	if err != nil {
		return err
	}
	sourceID := vals[0]
	rdb := newRedis(vals[1])
	defer func() { _ = rdb.Close() }()
	store := syncstatus.Store{Client: rdb}
	run := syncstatus.Run{Command: command, StartedAt: time.Now()}
	// 已撤销的源不再同步，也就不会把文档重新入库；读不到撤销状态就不同步
	revoked, err := store.Revoked(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("读取撤销状态: %w", err)
	}
	if revoked {
		log.Warn("数据源已撤销，本轮跳过", slog.String("source", sourceID))
		run.FinishedAt, run.Result = time.Now(), syncstatus.SkippedRevoked
		return store.Record(ctx, sourceID, run)
	}
	// 读不到暂停状态就不同步：暂停常常正是为了让这个源停下
	paused, err := store.Paused(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("读取暂停状态: %w", err)
	}
	if paused {
		log.Warn("数据源已暂停，本轮跳过", slog.String("source", sourceID))
		run.FinishedAt, run.Result = time.Now(), syncstatus.SkippedPaused
		return store.Record(ctx, sourceID, run)
	}
	err = sync(ctx, log, &run)
	run.Finish(err, time.Now())
	// 收到停止信号时也要把这一轮记下来
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if rerr := store.Record(rctx, sourceID, run); rerr != nil {
		return errors.Join(err, fmt.Errorf("写同步状态: %w", rerr))
	}
	return err
}

// serveAdmin 是数据源运维接口（internal/admin），只接受 gateway 签发的管理 Principal。
func serveAdmin(ctx context.Context, log *slog.Logger) error {
	vals, err := envs("BF_REDIS_ADDR", "BF_PRINCIPAL_JWKS_URL", "BF_INDEXING_ADDR")
	if err != nil {
		return err
	}
	rdb := newRedis(vals[0])
	defer func() { _ = rdb.Close() }()
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", listenAddr())
	if err != nil {
		return err
	}
	// 立即同步经调度方拉起（internal/trigger）：BF_SYNC_TRIGGER=kubernetes 时从数据源的 CronJob 创建 Job，不配置时不支持
	var tr trigger.Trigger = trigger.Unconfigured{}
	switch v := os.Getenv("BF_SYNC_TRIGGER"); v {
	case "":
	case "kubernetes":
		k, err := trigger.InCluster()
		if err != nil {
			return fmt.Errorf("BF_SYNC_TRIGGER=kubernetes: %w", err)
		}
		tr = k
	default:
		return fmt.Errorf("BF_SYNC_TRIGGER 只能是 kubernetes 或不设置，收到 %q", v)
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(principal.NewOperatorVerifier(principal.NewJWKS(vals[1], nil)), log)))
	adminv1.RegisterConnectorAdminServiceServer(s, &admin.Server{Store: syncstatus.Store{Client: rdb}, Holds: holds.Store{Client: rdb},
		Trigger: tr, Wake: func(ctx context.Context, id string) error { return poll.Wake(ctx, rdb, id) }, Log: log, Now: time.Now})
	// 撤销数据源（internal/revoke）：消费平台事件流里的 source.revoked，对该源全部文档写删除标记
	indexing, err := grpc.NewClient(vals[2], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = indexing.Close() }()
	host, _ := os.Hostname()
	index := ingestionv1.NewIndexWriterServiceClient(indexing)
	// 撤销执行完写变更审计（与 gateway 入口审计同一个审计桶）；审计桶连不上时管理服务不启动
	auditStore, err := worm.OpenFromEnv(ctx)
	if err != nil {
		return fmt.Errorf("连接审计桶: %w", err)
	}
	revocations := (&revoke.Consumer{Log: log, Revoker: &revoke.Revoker{
		Index: index, Status: syncstatus.Store{Client: rdb}, Redis: rdb,
		Audit: &changeaudit.Writer{Store: auditStore, Service: "connectors", Now: time.Now}, Log: log}}).Loop(rdb, host)
	go func() {
		if err := revocations.Run(ctx); err != nil {
			log.Error("撤销数据源的事件消费退出", slog.Any("err", err))
		}
	}()
	// 物理删除后清理对象存储里的原文与文档包（internal/rawpurge，DocumentPurged），补记删除凭证
	s3, bucket, s3rdb, err := connect(ctx)
	if err != nil {
		return err
	}
	_ = s3rdb.Close()
	// 推送入库（internal/push）：gateway 以角色 source_pusher:<数据源> 的管理 Principal 调用
	ingestionv1.RegisterPushServiceServer(s, &push.Server{
		Publisher: func(id string) push.Publisher {
			return &publish.Publisher{S3: s3, Bucket: bucket, Redis: rdb, SourceID: id, Detection: ingestionv1.Detection_DETECTION_PUSH}
		},
		State: push.RedisState{Client: rdb}, Status: syncstatus.Store{Client: rdb}, Log: log, Now: time.Now,
	})
	purges := (&rawpurge.Purger{Objects: &rawpurge.S3{Client: s3, Bucket: bucket}, Index: index, Log: log, Now: time.Now}).Loop(rdb, host)
	go func() {
		if err := purges.Run(ctx); err != nil {
			log.Error("清理对象存储原文的事件消费退出", slog.Any("err", err))
		}
	}()
	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()
	log.Info("connectors 管理服务已启动")
	return s.Serve(lis)
}

// newRedis 连接事件流与同步状态所在的 Redis；口令经 Vault 注入（BF_REDIS_PASSWORD），开发环境没有口令。
func newRedis(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("BF_REDIS_PASSWORD")})
}

func listenAddr() string {
	if v := os.Getenv("BF_LISTEN"); v != "" {
		return v
	}
	return ":9090"
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

func env(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("未设置 %s", name)
	}
	return v, nil
}

func listWiki(dir, baseURI string) ([]*publish.Document, []docmeta.Skipped, error) {
	pages, skipped, err := wiki.List(dir)
	if err != nil {
		return nil, nil, err
	}
	out := make([]*publish.Document, 0, len(pages))
	for _, p := range pages {
		out = append(out, &publish.Document{
			Meta: p.Meta, SourceURI: baseURI + p.DocID, SourceType: "wiki", ContentType: "text/markdown",
			Text: p.Body, ContentHash: p.ContentHash,
		})
	}
	return out, skipped, nil
}

func listFileshare(dir, baseURI string) ([]*publish.Document, []docmeta.Skipped, error) {
	files, skipped, err := fileshare.List(dir)
	if err != nil {
		return nil, nil, err
	}
	out := make([]*publish.Document, 0, len(files))
	for _, f := range files {
		out = append(out, &publish.Document{
			Meta: f.Meta, SourceURI: baseURI + url.PathEscape(f.Name), SourceType: f.Format.SourceType,
			ContentType: f.Format.ContentType, FileName: f.Name, Raw: f.Raw, ContentHash: f.ContentHash,
		})
	}
	return out, skipped, nil
}

// envs 读取一组必填的环境变量。
func envs(names ...string) ([]string, error) {
	out := make([]string, len(names))
	for i, name := range names {
		v, err := env(name)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// connect 连接对象存储（桶不存在时创建）与事件流。
func connect(ctx context.Context) (*minio.Client, string, *redis.Client, error) {
	vals, err := envs("BF_S3_ENDPOINT", "BF_S3_ACCESS_KEY", "BF_S3_SECRET_KEY", "BF_S3_BUCKET", "BF_REDIS_ADDR")
	if err != nil {
		return nil, "", nil, err
	}
	endpoint, accessKey, secretKey, bucket, redisAddr := vals[0], vals[1], vals[2], vals[3], vals[4]
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, "", nil, err
	}
	s3, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: u.Scheme == "https"})
	if err != nil {
		return nil, "", nil, err
	}
	exists, err := s3.BucketExists(ctx, bucket)
	if err != nil {
		return nil, "", nil, fmt.Errorf("检查对象存储桶: %w", err)
	}
	if !exists {
		if err := s3.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, "", nil, err
		}
	}
	return s3, bucket, newRedis(redisAddr), nil
}

// syncLogs 是日志数据源的一轮同步（internal/logs）。配置文件给出主题、入库的服务与级别、服务归属的运维组与保留期。
func syncLogs(ctx context.Context, log *slog.Logger, run *syncstatus.Run) error {
	vals, err := envs("BF_SOURCE_ID", "BF_LOG_CONFIG", "BF_LOG_BASE_URI")
	if err != nil {
		return err
	}
	sourceID, configPath, baseURI := vals[0], vals[1], vals[2]
	cfg, err := logs.LoadConfig(configPath)
	if err != nil {
		return err
	}
	s3, bucket, rdb, err := connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	host, _ := os.Hostname()
	s := &logs.Syncer{
		Config:    cfg,
		Source:    &logs.RedisTopic{Client: rdb, Stream: cfg.Topic, Group: "connectors-" + sourceID, Consumer: host, Log: log},
		Store:     &logs.S3Store{Client: s3, Bucket: bucket, Prefix: "state/" + sourceID + "/"},
		Published: &logs.RedisPublished{Client: rdb, Key: "bf:connector:" + sourceID + ":state"},
		Publisher: &publish.Publisher{S3: s3, Bucket: bucket, Redis: rdb, SourceID: sourceID},
		BaseURI:   baseURI, Log: log, Now: time.Now,
	}
	st, err := s.Run(ctx)
	if err != nil {
		return err
	}
	run.Documents, run.Upserts, run.ACLOnly = st.Upserts+st.ACLOnly, st.Upserts, st.ACLOnly
	log.Info("日志同步完成", slog.String("source", sourceID), slog.Int("read", st.Read), slog.Int("accepted", st.Accepted),
		slog.Int("duplicates", st.Duplicates), slog.Int("upserts", st.Upserts), slog.Int("acl_only", st.ACLOnly),
		slog.Int("expired", st.Expired))
	return nil
}

// syncDBSchema 是关系型数据库数据源的一轮同步（internal/dbschema）。元数据默认用只读账号读系统表；
// 接数据平台的元数据目录时只换 Catalog 的实现。
func syncDBSchema(ctx context.Context, log *slog.Logger, run *syncstatus.Run) error {
	vals, err := envs("BF_SOURCE_ID", "BF_DBSCHEMA_CONFIG", "BF_DBSCHEMA_DSN", "BF_DBSCHEMA_BASE_URI")
	if err != nil {
		return err
	}
	sourceID, configPath, dsn, baseURI := vals[0], vals[1], vals[2], vals[3]
	cfg, err := dbschema.LoadConfig(configPath)
	if err != nil {
		return err
	}
	catalog, err := dbschema.OpenPostgres(ctx, dsn)
	if err != nil {
		return err
	}
	defer catalog.Close()
	s3, bucket, rdb, err := connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	s := &dbschema.Syncer{
		Config: cfg, Catalog: catalog,
		Published: &logs.RedisPublished{Client: rdb, Key: "bf:connector:" + sourceID + ":state"},
		Synced:    &logs.RedisPublished{Client: rdb, Key: "bf:connector:" + sourceID + ":synced"},
		Publisher: &publish.Publisher{S3: s3, Bucket: bucket, Redis: rdb, SourceID: sourceID},
		BaseURI:   baseURI, ConfirmDeletes: os.Getenv("BF_CONFIRM_DELETES") == "1", Now: time.Now,
	}
	st, err := s.Run(ctx)
	if err != nil {
		return err
	}
	run.Documents, run.Upserts, run.ACLOnly, run.Deletes = st.Views, st.Upserts, st.ACLOnly, st.Deletes
	run.DeletesPaused, run.DeletesPausedReason = st.DeletesPaused != "", st.DeletesPaused
	if st.DeletesPaused != "" {
		log.Warn("删除已暂停，等待确认", slog.String("reason", st.DeletesPaused), slog.String("source", sourceID))
	}
	log.Info("表结构同步完成", slog.String("source", sourceID), slog.Int("views", st.Views), slog.Int("upserts", st.Upserts),
		slog.Int("acl_only", st.ACLOnly), slog.Int("deletes", st.Deletes))
	return nil
}

// syncRecords 是结构化记录入库的一轮同步（internal/records）：按表开启，一行一张记录卡片。
func syncRecords(ctx context.Context, log *slog.Logger, run *syncstatus.Run) error {
	vals, err := envs("BF_SOURCE_ID", "BF_RECORDS_CONFIG", "BF_RECORDS_DSN", "BF_RECORDS_BASE_URI")
	if err != nil {
		return err
	}
	sourceID, configPath, dsn, baseURI := vals[0], vals[1], vals[2], vals[3]
	cfg, err := records.LoadConfig(configPath)
	if err != nil {
		return err
	}
	rows, err := records.OpenPostgres(ctx, dsn)
	if err != nil {
		return err
	}
	defer rows.Close()
	s3, bucket, rdb, err := connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()
	s := &records.Syncer{
		Config: cfg, Rows: rows,
		Published: &logs.RedisPublished{Client: rdb, Key: "bf:connector:" + sourceID + ":state"},
		Publisher: &publish.Publisher{S3: s3, Bucket: bucket, Redis: rdb, SourceID: sourceID},
		BaseURI:   baseURI, ConfirmDeletes: os.Getenv("BF_CONFIRM_DELETES") == "1", Now: time.Now,
	}
	st, err := s.Run(ctx)
	if err != nil {
		return err
	}
	run.Documents, run.Upserts, run.ACLOnly, run.Deletes = st.Rows, st.Upserts, st.ACLOnly, st.Deletes
	run.DeletesPaused, run.DeletesPausedReason = st.DeletesPaused != "", st.DeletesPaused
	if st.WithoutACL > 0 {
		log.Warn("有行的权限表达不了，没有入库", slog.String("source", sourceID), slog.Int("rows", st.WithoutACL))
	}
	log.Info("记录同步完成", slog.String("source", sourceID), slog.Int("rows", st.Rows), slog.Int("upserts", st.Upserts),
		slog.Int("acl_only", st.ACLOnly), slog.Int("deletes", st.Deletes))
	return nil
}

// splitHeld 把被拦下的待删文档分成两部分：不在已确认批次里的（仍需确认）与在批次里的（本轮执行）。
func splitHeld(held, confirmed []string) (rest, apply []string) {
	in := make(map[string]bool, len(confirmed))
	for _, id := range confirmed {
		in[id] = true
	}
	for _, id := range held {
		if in[id] {
			apply = append(apply, id)
		} else {
			rest = append(rest, id)
		}
	}
	return rest, apply
}

func syncSource(ctx context.Context, log *slog.Logger, run *syncstatus.Run, dirEnv, uriEnv string, list lister, reconcileFirst bool) (err error) {
	vals, err := envs("BF_SOURCE_ID", dirEnv, uriEnv)
	if err != nil {
		return err
	}
	sourceID, dir, baseURI := vals[0], vals[1], vals[2]

	docs, skipped, err := list(dir, baseURI)
	if err != nil {
		return err
	}
	// 单份文档读不出不中止整轮（入库管道详设第 11 节）：跳过它、照常同步其余的，但清单不完整，这一轮不推断删除
	incomplete := len(skipped) > 0
	if incomplete {
		run.Skipped = syncstatus.SkippedOf(skipped)
		for _, x := range skipped[:min(len(skipped), 20)] {
			log.Warn("跳过读不出的文档", slog.String("source", sourceID), slog.String("name", x.Name), slog.String("reason", x.Reason))
		}
	}
	s3, bucket, rdb, err := connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	p := &publish.Publisher{S3: s3, Bucket: bucket, Redis: rdb, SourceID: sourceID}
	// 分区级数据源（总体设计 ADR-14）：文档只带分区 token，分区授权单独同步（internal/partition）
	if path := os.Getenv("BF_PARTITIONS_FILE"); path != "" {
		grants, err := partition.Load(path)
		if err != nil {
			return err
		}
		if err := partition.Apply(docs, sourceID, grants, log); err != nil {
			return err
		}
		n, err := partition.Sync(ctx, grants, partition.RedisState{Client: rdb, SourceID: sourceID}, p, time.Now())
		if err != nil {
			return fmt.Errorf("同步分区授权: %w", err)
		}
		if n > 0 {
			log.Info("已发出分区授权变化", slog.String("source", sourceID), slog.Int("partitions", n))
		}
	}
	// 已发出的状态：doc_id → 内容摘要|权限摘要。只发新增与变化的文档（增量入库），两种样例源共用
	stateKey := "bf:connector:" + sourceID + ":state"
	// 撤销删除标记后要求重新拉取的文档：先从状态里去掉，本轮就会作为新文档重新发出（internal/refetch）
	host, _ := os.Hostname()
	refetched, err := refetch.Apply(ctx, &refetch.RedisStream{Client: rdb, SourceID: sourceID, Consumer: host},
		&refetch.RedisState{Client: rdb, Key: stateKey}, sourceID, log)
	if err != nil {
		return fmt.Errorf("处理重新拉取请求: %w", err)
	}
	byID := map[string]*publish.Document{}
	current := map[string]string{}
	for _, d := range docs {
		byID[d.DocID] = d
		// 内容部分含源端字段摘要：只改了源端字段也按内容变化处理（入库管道详设第 7 节，元数据要跟着更新）。
		// 加入这一项后，已有状态里的摘要都对不上，各文档会重新处理一次，正好补齐历史文档的元数据
		current[d.DocID] = d.ContentHash + "+" + d.SourceFingerprint + "|" + d.ACLFingerprint
	}
	if reconcileFirst {
		counts, err := reconcileState(ctx, rdb, stateKey, sourceID, docs, current)
		if err != nil {
			return fmt.Errorf("对账: %w", err)
		}
		run.Reconciliation = &counts
		log.Info("对账完成", slog.String("source", sourceID), slog.Int("indexed", counts.Indexed), slog.Int("missing", counts.Missing),
			slog.Int("content_mismatch", counts.ContentMismatch), slog.Int("acl_mismatch", counts.ACLMismatch),
			slog.Int("orphaned", counts.Orphaned), slog.Int("tombstoned_in_source", counts.TombstonedInSource))
	}
	state, err := rdb.HGetAll(ctx, stateKey).Result()
	if err != nil {
		return fmt.Errorf("读取同步状态: %w", err)
	}
	plan := incremental.Diff(state, current, os.Getenv("BF_CONFIRM_DELETES") == "1")
	if incomplete {
		plan.Incomplete()
	}
	now := time.Now()
	changes := map[ingestionv1.ChangeType][]string{
		ingestionv1.ChangeType_CHANGE_TYPE_UPSERT:   plan.Upserts,
		ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY: plan.AclOnly,
	}
	// 单份文档发出失败不挡住其他文档：记下来，这一轮结束时整体记为失败，失败的下一轮重发
	var failed []string
	for _, change := range []ingestionv1.ChangeType{ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY, ingestionv1.ChangeType_CHANGE_TYPE_UPSERT} {
		for _, id := range changes[change] {
			if err := p.Publish(ctx, byID[id], change, now); err != nil {
				log.Error("文档发出失败，下一轮重发", slog.String("source", sourceID), slog.String("doc_id", id), slog.Any("err", err))
				failed = append(failed, id)
				continue
			}
			// 事件发出后才记状态：中途失败的文档下一轮会重发
			if err := rdb.HSet(ctx, stateKey, id, current[id]).Err(); err != nil {
				return err
			}
		}
	}
	if len(failed) > 0 {
		defer func() {
			err = errors.Join(err, fmt.Errorf("%d 份文档发出失败（如 %s），下一轮重发", len(failed), strings.Join(failed[:min(len(failed), 5)], "、")))
		}()
	}
	// 删除保护（入库管道详设第 5 节）：执行两位数据属主确认过的批次，只删其中本轮源端仍不存在的；
	// 本轮仍被拦下的其余待删文档记成一批，等确认
	holdStore := holds.Store{Client: rdb}
	confirmed, err := holdStore.Confirmed(ctx, sourceID)
	if err != nil {
		return fmt.Errorf("读取待删批次: %w", err)
	}
	if confirmed != nil && !incomplete {
		var applied []string
		plan.Held, applied = splitHeld(plan.Held, confirmed.DocIDs)
		plan.Deletes = append(plan.Deletes, applied...)
		defer func() {
			// 删除全部发出后才记为已执行；中途失败时批次仍是 confirmed，下一轮重做
			if err == nil {
				err = holdStore.MarkApplied(context.WithoutCancel(ctx), sourceID, confirmed.ID, len(applied), time.Now())
			}
		}()
	}
	if len(plan.Held) > 0 {
		h, err := holdStore.Observe(ctx, sourceID, plan.Held, len(state), now)
		if err != nil {
			return fmt.Errorf("记录待删批次: %w", err)
		}
		log.Warn("删除保护拦下一批待删文档", slog.String("hold_id", h.ID), slog.String("status", h.Status),
			slog.Int("docs", len(plan.Held)), slog.String("source", sourceID))
	}
	if plan.Reason == "over_threshold" && len(plan.Held) == 0 {
		// 拦下的都在已确认的批次里，本轮已执行
		plan.DeletesPaused, plan.Reason = false, ""
	}
	for _, id := range plan.Deletes {
		if err := p.Delete(ctx, id, now); err != nil {
			return errors.Join(fmt.Errorf("删除 %s", id), err)
		}
		if err := rdb.HDel(ctx, stateKey, id).Err(); err != nil {
			return err
		}
	}
	run.Documents, run.Upserts, run.ACLOnly, run.Deletes = len(docs), len(plan.Upserts), len(plan.AclOnly), len(plan.Deletes)
	run.DeletesPaused, run.DeletesPausedReason, run.Refetched = plan.DeletesPaused, plan.Reason, refetched
	if plan.DeletesPaused {
		// 删除保护：等两位数据属主在 /v1/admin/sources/{src_id}/pending-deletions 确认（入库管道详设第 5 节）；
		// BF_CONFIRM_DELETES=1 是运维直接确认本轮全部删除的应急开关（如数据源下线）
		log.Warn("删除已暂停，等待确认", slog.String("reason", plan.Reason), slog.String("source", sourceID))
	}
	log.Info("同步完成", slog.String("source", sourceID), slog.Int("documents", len(docs)),
		slog.Int("upserts", len(plan.Upserts)), slog.Int("acl_only", len(plan.AclOnly)), slog.Int("deletes", len(plan.Deletes)),
		slog.Int("refetched", refetched))
	return nil
}

// reconcileState 取 indexing 已入库的文档，与源端清单比对后修正已发出状态（internal/reconcile）；随后的增量同步据此补发。
func reconcileState(ctx context.Context, rdb *redis.Client, stateKey, sourceID string, docs []*publish.Document,
	current map[string]string) (syncstatus.Reconciliation, error) {
	addr, err := env("BF_INDEXING_ADDR")
	if err != nil {
		return syncstatus.Reconciliation{}, err
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return syncstatus.Reconciliation{}, err
	}
	defer func() { _ = conn.Close() }()
	stream, err := ingestionv1.NewIndexWriterServiceClient(conn).ListSourceDocuments(ctx, &ingestionv1.ListSourceDocumentsRequest{SourceId: sourceID})
	if err != nil {
		return syncstatus.Reconciliation{}, err
	}
	var indexed []*ingestionv1.IndexedDocument
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// 没拿全就不修正：缺的部分会被当成“漏同步”全部重发
			return syncstatus.Reconciliation{}, fmt.Errorf("读取已入库文档: %w", err)
		}
		indexed = append(indexed, resp.GetDocuments()...)
	}
	source := make(map[string]reconcile.Source, len(docs))
	for _, d := range docs {
		source[d.DocID] = reconcile.Source{Fingerprint: current[d.DocID], ContentHash: d.ContentHash, ACLTokens: d.ACLTokens,
			Classification: d.Classification}
	}
	fixes, c := reconcile.Compare(source, indexed)
	pipe := rdb.TxPipeline()
	if len(fixes.Del) > 0 {
		pipe.HDel(ctx, stateKey, fixes.Del...)
	}
	for id, v := range fixes.Set {
		pipe.HSet(ctx, stateKey, id, v)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return syncstatus.Reconciliation{}, fmt.Errorf("修正同步状态: %w", err)
	}
	return syncstatus.Reconciliation{Missing: c.Missing, ContentMismatch: c.ContentMismatch, ACLMismatch: c.ACLMismatch,
		Orphaned: c.Orphaned, TombstonedInSource: c.TombstonedInSource, Indexed: c.Indexed}, nil
}
