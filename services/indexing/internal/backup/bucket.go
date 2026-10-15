package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Bucket 是备份所在的对象存储桶。生产上它跨地域复制到灾备站点（总体设计 11.5），灾备站点从复制过去的桶恢复。
type Bucket interface {
	Put(ctx context.Context, key string, body []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
	// Purge 删掉 key 的全部历史版本，只留最新的（桶开了版本控制时，覆盖写不会让旧内容消失）
	Purge(ctx context.Context, key string) error
}

// ErrNotFound 表示对象不存在。
var ErrNotFound = errors.New("对象不存在")

type s3Bucket struct {
	c      *minio.Client
	bucket string
}

// OpenFromEnv 按 BF_BACKUP_S3_ENDPOINT、BF_BACKUP_S3_ACCESS_KEY、BF_BACKUP_S3_SECRET_KEY、BF_BACKUP_BUCKET 连接备份桶，
// 桶不存在时创建。
func OpenFromEnv(ctx context.Context) (Bucket, error) {
	vals := make([]string, 4)
	for i, name := range []string{"BF_BACKUP_S3_ENDPOINT", "BF_BACKUP_S3_ACCESS_KEY", "BF_BACKUP_S3_SECRET_KEY", "BF_BACKUP_BUCKET"} {
		if vals[i] = os.Getenv(name); vals[i] == "" {
			return nil, fmt.Errorf("未设置 %s", name)
		}
	}
	u, err := url.Parse(vals[0])
	if err != nil {
		return nil, err
	}
	c, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(vals[1], vals[2], ""), Secure: u.Scheme == "https"})
	if err != nil {
		return nil, err
	}
	exists, err := c.BucketExists(ctx, vals[3])
	if err != nil {
		return nil, fmt.Errorf("检查备份桶: %w", err)
	}
	if !exists {
		if err := c.MakeBucket(ctx, vals[3], minio.MakeBucketOptions{}); err != nil {
			if exists, _ := c.BucketExists(ctx, vals[3]); !exists {
				return nil, fmt.Errorf("创建备份桶: %w", err)
			}
		}
	}
	return &s3Bucket{c: c, bucket: vals[3]}, nil
}

func (b *s3Bucket) Put(ctx context.Context, key string, body []byte) error {
	_, err := b.c.PutObject(ctx, b.bucket, key, bytes.NewReader(body), int64(len(body)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

func (b *s3Bucket) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := b.c.GetObject(ctx, b.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	body, err := io.ReadAll(obj)
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil, ErrNotFound
	}
	return body, err
}

func (b *s3Bucket) Delete(ctx context.Context, key string) error {
	return b.c.RemoveObject(ctx, b.bucket, key, minio.RemoveObjectOptions{})
}

func (b *s3Bucket) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range b.c.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

func (b *s3Bucket) Purge(ctx context.Context, key string) error {
	for obj := range b.c.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Prefix: key, WithVersions: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		if obj.Key != key || obj.IsLatest || obj.VersionID == "" || obj.VersionID == "null" {
			continue
		}
		if err := b.c.RemoveObject(ctx, b.bucket, key, minio.RemoveObjectOptions{VersionID: obj.VersionID}); err != nil {
			return err
		}
	}
	return nil
}
