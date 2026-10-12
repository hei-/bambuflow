// seed-dbschema 在开发 Postgres 里建关系型数据库数据源的演示库（bankdemo schema，见 demo.sql）和只读账号，
// 只用于开发与 CI（P3-I4）。可重复执行（make ingest 会连跑两次）。
//
// 只读账号 dbschema_reader 只用来读系统目录：没有任何表或视图的授权，连接默认只读事务。
// 查询账号 sql_query_login 是 orchestrator 的 sql_query 默认实现（P3-Q）用的登录账号：NOINHERIT，
// 本身读不到任何视图，只能在只读事务里 SET ROLE 成调用者映射到的报表角色（report_ops、report_finance）再查询。
package main

import (
	"context"
	_ "embed"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hei-/bambuflow/libs/platform-go/logging"
)

//go:embed demo.sql
var demo string

const (
	reader = "dbschema_reader"
	query  = "sql_query_login"
)

func main() {
	log := logging.New(os.Stdout, "seed-dbschema", slog.LevelInfo)
	dsn, password, queryPassword := os.Getenv("BF_PG_DSN"), os.Getenv("BF_DBSCHEMA_PASSWORD"), os.Getenv("BF_SQL_QUERY_PASSWORD")
	if dsn == "" || password == "" || queryPassword == "" {
		log.Error("需要 BF_PG_DSN、BF_DBSCHEMA_PASSWORD 与 BF_SQL_QUERY_PASSWORD")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := seed(ctx, dsn, password, queryPassword); err != nil {
		log.Error("建演示库失败", slog.Any("err", err))
		os.Exit(1)
	}
	log.Info("演示库与只读账号已就绪", slog.String("schema", "bankdemo"), slog.String("reader", reader))
}

func seed(ctx context.Context, dsn, password, queryPassword string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	// 不带参数的 Exec 走简单查询协议，一次执行整个脚本
	if _, err := conn.Exec(ctx, demo); err != nil {
		return err
	}
	if err := login(ctx, conn, reader, password, "LOGIN"); err != nil {
		return err
	}
	if err := login(ctx, conn, query, queryPassword, "LOGIN NOINHERIT"); err != nil {
		return err
	}
	for _, stmt := range []string{
		`SELECT format('GRANT report_ops, report_finance TO %I', $1::text)`,
	} {
		var s string
		if err := conn.QueryRow(ctx, stmt, query).Scan(&s); err != nil {
			return err
		}
		if _, err := conn.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// login 建或更新一个登录账号（口令每次按配置重设），连接默认只读事务。
func login(ctx context.Context, conn *pgx.Conn, role, password, attrs string) error {
	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
		return err
	}
	// 角色语句不能用参数：由服务端的 format 按标识符与字面量转义后拼出语句；attrs 是本文件里的常量
	tmpl := "ALTER ROLE %I " + attrs + " PASSWORD %L"
	if !exists {
		tmpl = "CREATE ROLE %I " + attrs + " PASSWORD %L"
	}
	var stmt string
	if err := conn.QueryRow(ctx, `SELECT format($1, $2::text, $3::text)`, tmpl, role, password).Scan(&stmt); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, stmt); err != nil {
		return err
	}
	if err := conn.QueryRow(ctx, `SELECT format('ALTER ROLE %I SET default_transaction_read_only = on', $1::text)`, role).Scan(&stmt); err != nil {
		return err
	}
	_, err := conn.Exec(ctx, stmt)
	return err
}
