// connectors 负责变更检测与原文、ACL 拉取（入库管道详设第 5 节）。样例源按全量清单增量同步：
// 新增与内容变化发 UPSERT，只有权限变化发 ACL_ONLY，源端消失的发 DELETE（带删除保护）。
//
//	connectors sync-wiki       读取 Wiki 样例目录，为每个页面写文档包并发出 DocumentChanged
//	connectors sync-fileshare  读取共享盘样例目录（PDF、Office、邮件、图片），同上
//	connectors sync-logs       读取日志平台消息主题里的新消息，按服务一天一份文档发出（internal/logs）
//	connectors sync-dbschema   读取白名单视图的表结构与字段说明，一个视图一份文档发出（internal/dbschema，部署时每小时一次）
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/redis/go-redis/v9"

	ingestionv1 "github.com/bambu-ai/bambuflow/contracts/gen/go/bambuflow/ingestion/v1"
	"github.com/bambu-ai/bambuflow/libs/platform-go/logging"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/dbschema"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/fileshare"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/incremental"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/logs"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/publish"
	"github.com/bambu-ai/bambuflow/services/ingestion/connectors/internal/wiki"
)

// lister 读取一个样例源的全部文档；样例目录与 source_uri 前缀由环境变量给出。
type lister func(dir, baseURI string) ([]*publish.Document, error)

var commands = map[string]struct {
	dirEnv, uriEnv string
	list           lister
}{
	"sync-wiki":      {"BF_WIKI_DIR", "BF_WIKI_BASE_URI", listWiki},
	"sync-fileshare": {"BF_FILESHARE_DIR", "BF_FILESHARE_BASE_URI", listFileshare},
}

func main() {
	log := logging.New(os.Stdout, "connectors", slog.LevelInfo)
	if len(os.Args) < 2 {
		usage()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	cmd, listed := commands[os.Args[1]]
	switch {
	case os.Args[1] == "sync-logs":
		err = syncLogs(ctx, log)
	case os.Args[1] == "sync-dbschema":
		err = syncDBSchema(ctx, log)
	case listed:
		err = syncSource(ctx, log, cmd.dirEnv, cmd.uriEnv, cmd.list)
	default:
		usage()
	}
	if err != nil {
		log.Error("退出", slog.Any("err", err))
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法: connectors sync-wiki | sync-fileshare | sync-logs | sync-dbschema")
	os.Exit(2)
}

func env(name string) (string, error) {
	v := os.Getenv(name)
	if v == "" {
		return "", fmt.Errorf("未设置 %s", name)
	}
	return v, nil
}

func listWiki(dir, baseURI string) ([]*publish.Document, error) {
	pages, err := wiki.List(dir)
	if err != nil {
		return nil, err
	}
	out := make([]*publish.Document, 0, len(pages))
	for _, p := range pages {
		out = append(out, &publish.Document{
			Meta: p.Meta, SourceURI: baseURI + p.DocID, SourceType: "wiki", ContentType: "text/markdown",
			Text: p.Body, ContentHash: p.ContentHash,
		})
	}
	return out, nil
}

func listFileshare(dir, baseURI string) ([]*publish.Document, error) {
	files, err := fileshare.List(dir)
	if err != nil {
		return nil, err
	}
	out := make([]*publish.Document, 0, len(files))
	for _, f := range files {
		out = append(out, &publish.Document{
			Meta: f.Meta, SourceURI: baseURI + url.PathEscape(f.Name), SourceType: f.Format.SourceType,
			ContentType: f.Format.ContentType, FileName: f.Name, Raw: f.Raw, ContentHash: f.ContentHash,
		})
	}
	return out, nil
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
	return s3, bucket, redis.NewClient(&redis.Options{Addr: redisAddr}), nil
}

// syncLogs 是日志数据源的一轮同步（internal/logs）。配置文件给出主题、入库的服务与级别、服务归属的运维组与保留期。
func syncLogs(ctx context.Context, log *slog.Logger) error {
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
	log.Info("日志同步完成", slog.String("source", sourceID), slog.Int("read", st.Read), slog.Int("accepted", st.Accepted),
		slog.Int("duplicates", st.Duplicates), slog.Int("upserts", st.Upserts), slog.Int("acl_only", st.ACLOnly),
		slog.Int("expired", st.Expired))
	return nil
}

// syncDBSchema 是关系型数据库数据源的一轮同步（internal/dbschema）。元数据默认用只读账号读系统表；
// 接数据平台的元数据目录时只换 Catalog 的实现。
func syncDBSchema(ctx context.Context, log *slog.Logger) error {
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
	if st.DeletesPaused != "" {
		log.Warn("删除已暂停，等待确认", slog.String("reason", st.DeletesPaused), slog.String("source", sourceID))
	}
	log.Info("表结构同步完成", slog.String("source", sourceID), slog.Int("views", st.Views), slog.Int("upserts", st.Upserts),
		slog.Int("acl_only", st.ACLOnly), slog.Int("deletes", st.Deletes))
	return nil
}

func syncSource(ctx context.Context, log *slog.Logger, dirEnv, uriEnv string, list lister) error {
	vals, err := envs("BF_SOURCE_ID", dirEnv, uriEnv)
	if err != nil {
		return err
	}
	sourceID, dir, baseURI := vals[0], vals[1], vals[2]

	docs, err := list(dir, baseURI)
	if err != nil {
		return err
	}
	s3, bucket, rdb, err := connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	p := &publish.Publisher{S3: s3, Bucket: bucket, Redis: rdb, SourceID: sourceID}
	// 已发出的状态：doc_id → 内容摘要|权限摘要。只发新增与变化的文档（增量入库），两种样例源共用
	stateKey := "bf:connector:" + sourceID + ":state"
	state, err := rdb.HGetAll(ctx, stateKey).Result()
	if err != nil {
		return fmt.Errorf("读取同步状态: %w", err)
	}
	byID := map[string]*publish.Document{}
	current := map[string]string{}
	for _, d := range docs {
		byID[d.DocID] = d
		// 内容部分含源端字段摘要：只改了源端字段也按内容变化处理（入库管道详设第 7 节，元数据要跟着更新）。
		// 加入这一项后，已有状态里的摘要都对不上，各文档会重新处理一次，正好补齐历史文档的元数据
		current[d.DocID] = d.ContentHash + "+" + d.SourceFingerprint + "|" + d.ACLFingerprint
	}
	plan := incremental.Diff(state, current, os.Getenv("BF_CONFIRM_DELETES") == "1")
	now := time.Now()
	changes := map[ingestionv1.ChangeType][]string{
		ingestionv1.ChangeType_CHANGE_TYPE_UPSERT:   plan.Upserts,
		ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY: plan.AclOnly,
	}
	for _, change := range []ingestionv1.ChangeType{ingestionv1.ChangeType_CHANGE_TYPE_ACL_ONLY, ingestionv1.ChangeType_CHANGE_TYPE_UPSERT} {
		for _, id := range changes[change] {
			if err := p.Publish(ctx, byID[id], change, now); err != nil {
				return errors.Join(fmt.Errorf("文档 %s", id), err)
			}
			// 事件发出后才记状态：中途失败的文档下一轮会重发
			if err := rdb.HSet(ctx, stateKey, id, current[id]).Err(); err != nil {
				return err
			}
		}
	}
	for _, id := range plan.Deletes {
		if err := p.Delete(ctx, id, now); err != nil {
			return errors.Join(fmt.Errorf("删除 %s", id), err)
		}
		if err := rdb.HDel(ctx, stateKey, id).Err(); err != nil {
			return err
		}
	}
	if plan.DeletesPaused {
		// 删除保护：等管理员确认后用 BF_CONFIRM_DELETES=1 重跑（入库管道详设第 5 节）
		log.Warn("删除已暂停，等待确认", slog.String("reason", plan.Reason), slog.String("source", sourceID))
	}
	log.Info("同步完成", slog.String("source", sourceID), slog.Int("documents", len(docs)),
		slog.Int("upserts", len(plan.Upserts)), slog.Int("acl_only", len(plan.AclOnly)), slog.Int("deletes", len(plan.Deletes)))
	return nil
}
