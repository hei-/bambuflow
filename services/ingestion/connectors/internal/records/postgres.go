package records

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresRows 是 Rows 的默认实现：只读账号查白名单视图，连接以只读事务运行。列都按文本取（::text），
// 展示与摘要不受驱动的类型映射影响；权限列按 text[] 取。
type PostgresRows struct {
	Pool *pgxpool.Pool
}

// OpenPostgres 建立连接池（不立即连接）。
func OpenPostgres(ctx context.Context, dsn string) (*PostgresRows, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PostgresRows{Pool: pool}, nil
}

func (p *PostgresRows) Close() { p.Pool.Close() }

func (p *PostgresRows) Rows(ctx context.Context, t Table, limit int) ([]Row, error) {
	schema, view, _ := strings.Cut(t.View, ".")
	cols := make([]string, 0, len(t.Columns)+1)
	for _, c := range t.Columns {
		cols = append(cols, pgx.Identifier{c.Name}.Sanitize()+"::text")
	}
	if t.ACLColumn != "" {
		cols = append(cols, pgx.Identifier{t.ACLColumn}.Sanitize()+"::text[]")
	}
	// 标识符都经配置校验并转义；多取一行判断是否超过上限
	rows, err := p.Pool.Query(ctx, "SELECT "+strings.Join(cols, ", ")+" FROM "+pgx.Identifier{schema, view}.Sanitize()+
		" LIMIT $1", limit+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Row
	for rows.Next() {
		vals := make([]*string, len(t.Columns))
		dst := make([]any, 0, len(cols))
		for i := range vals {
			dst = append(dst, &vals[i])
		}
		var tokens []string
		if t.ACLColumn != "" {
			dst = append(dst, &tokens)
		}
		if err := rows.Scan(dst...); err != nil {
			return nil, err
		}
		r := Row{Values: map[string]*string{}, ACL: tokens}
		for i, c := range t.Columns {
			r.Values[c.Name] = vals[i]
		}
		out = append(out, r)
		if len(out) > limit {
			return nil, ErrTooManyRows
		}
	}
	return out, rows.Err()
}
