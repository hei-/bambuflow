// Package pgmigrate 按序执行一个服务拥有的元数据库表结构变更（技术设计文档 12.8：升级前以 Job 执行）。
//
// 每个服务把迁移文件（NNNN_说明.sql）嵌入自己的二进制，按服务名分别记录进度；每个文件一个事务，
// 用会话级咨询锁防止同一服务的多个 Job 同时迁移。
package pgmigrate

import (
	"context"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

type step struct {
	version int
	name    string
	sql     string
}

// Steps 读取 dir 下的迁移文件并按序号排序；序号重复时报错。
func Steps(files fs.FS, dir string) ([]step, error) {
	entries, err := fs.ReadDir(files, dir)
	if err != nil {
		return nil, err
	}
	var out []step
	for _, e := range entries {
		v, err := strconv.Atoi(strings.SplitN(e.Name(), "_", 2)[0])
		if err != nil {
			return nil, fmt.Errorf("迁移文件名应以序号开头: %s", e.Name())
		}
		b, err := fs.ReadFile(files, path.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		out = append(out, step{v, e.Name(), string(b)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	for i := 1; i < len(out); i++ {
		if out[i].version == out[i-1].version {
			return nil, fmt.Errorf("迁移序号重复: %s 与 %s", out[i-1].name, out[i].name)
		}
	}
	return out, nil
}

// Up 执行 service 尚未执行过的迁移，返回本次执行的文件名。
func Up(ctx context.Context, conn *pgx.Conn, service string, files fs.FS, dir string) ([]string, error) {
	all, err := Steps(files, dir)
	if err != nil {
		return nil, err
	}
	// 所有服务的迁移共用一把锁、串行执行：各服务共用 public.schema_migrations，且迁移里有 CREATE EXTENSION
	// 等全库对象，并发执行 CREATE ... IF NOT EXISTS 会撞上 pg_type 唯一键（CI 中实际出现过）。迁移很快，串行没有代价。
	const lockKey = "pgmigrate"
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext($1))`, lockKey); err != nil {
		return nil, err
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext($1))`, lockKey)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS public.schema_migrations (
		service text NOT NULL, version int NOT NULL, name text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY (service, version))`); err != nil {
		return nil, err
	}
	var applied []string
	for _, s := range all {
		var done bool
		if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM public.schema_migrations WHERE service = $1 AND version = $2)`,
			service, s.version).Scan(&done); err != nil {
			return applied, err
		}
		if done {
			continue
		}
		err := pgx.BeginFunc(ctx, conn, func(tx pgx.Tx) error {
			if _, err := tx.Exec(ctx, s.sql); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.schema_migrations (service, version, name) VALUES ($1, $2, $3)`,
				service, s.version, s.name)
			return err
		})
		if err != nil {
			return applied, fmt.Errorf("%s: %w", s.name, err)
		}
		applied = append(applied, s.name)
	}
	return applied, nil
}
