// Package worm 是审计用的只追加对象存储（技术设计文档 8.3）：开启对象锁的桶，每个对象按合规模式保留，不可改删。
// gateway 的入口审计段与 governance 的变更审计写同一个桶，审计查询（/v1/admin/audit）从这里读。
package worm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Store 写入开启对象锁的桶，每个对象按合规模式保留 retention。
type Store struct {
	c         *minio.Client
	bucket    string
	retention time.Duration
}

// Open 连接对象存储并确认桶已开启对象锁；桶不存在时以开启对象锁的方式创建。
// 桶存在但没开对象锁时报错：审计存储达不到只追加就不启动（fail-closed）。
func Open(ctx context.Context, endpoint, accessKey, secretKey, bucket string, retention time.Duration) (*Store, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	c, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: u.Scheme == "https"})
	if err != nil {
		return nil, err
	}
	exists, err := c.BucketExists(ctx, bucket)
	if err != nil {
		return nil, fmt.Errorf("检查审计桶: %w", err)
	}
	if !exists {
		if err := c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{ObjectLocking: true}); err != nil {
			// 多个服务同时启动时可能被别人先建好
			if exists, _ := c.BucketExists(ctx, bucket); !exists {
				return nil, fmt.Errorf("创建审计桶: %w", err)
			}
		}
	}
	if enabled, _, _, _, err := c.GetObjectLockConfig(ctx, bucket); err != nil || enabled != "Enabled" {
		return nil, errors.Join(fmt.Errorf("审计桶 %s 没有开启对象锁", bucket), err)
	}
	return &Store{c: c, bucket: bucket, retention: retention}, nil
}

// Put 写入一个对象；同一个键不覆盖（对象锁下覆盖会生成新版本，调用方保证键唯一）。
func (s *Store) Put(ctx context.Context, key string, body []byte) error {
	_, err := s.c.PutObject(ctx, s.bucket, key, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{
		ContentType:     "application/x-ndjson",
		Mode:            minio.Compliance,
		RetainUntilDate: time.Now().Add(s.retention),
	})
	return err
}

// List 返回前缀下的全部对象键（审计查询用）。
func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	for obj := range s.c.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		keys = append(keys, obj.Key)
	}
	return keys, nil
}

// Get 读一个对象。
func (s *Store) Get(ctx context.Context, key string) ([]byte, error) {
	obj, err := s.c.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}
