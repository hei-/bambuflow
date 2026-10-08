package dbschema

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresCatalog 是 Catalog 的默认实现：读 PostgreSQL 的系统目录（pg_class、pg_attribute、注释与 relacl），
// 只查白名单里的视图（含物化视图），不读任何行数据。连接以只读事务运行，账号配错成可写的也写不进去。
type PostgresCatalog struct {
	Pool *pgxpool.Pool
}

// OpenPostgres 建立连接池（不立即连接）。
func OpenPostgres(ctx context.Context, dsn string) (*PostgresCatalog, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &PostgresCatalog{Pool: pool}, nil
}

func (p *PostgresCatalog) Close() { p.Pool.Close() }

func (p *PostgresCatalog) Views(ctx context.Context, qualified []string) ([]View, error) {
	rows, err := p.Pool.Query(ctx, `
		SELECT n.nspname, c.relname, COALESCE(obj_description(c.oid, 'pg_class'), ''),
			a.attname, format_type(a.atttypid, a.atttypmod), COALESCE(col_description(c.oid, a.attnum), '')
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		WHERE c.relkind IN ('v', 'm') AND n.nspname || '.' || c.relname = ANY($1::text[])
		ORDER BY n.nspname, c.relname, a.attnum`, qualified)
	if err != nil {
		return nil, err
	}
	var out []View
	byName := map[string]int{}
	err = forEach(rows, func(r pgx.Rows) error {
		var schema, name, comment string
		var col Column
		if err := r.Scan(&schema, &name, &comment, &col.Name, &col.Type, &col.Comment); err != nil {
			return err
		}
		key := schema + "." + name
		i, ok := byName[key]
		if !ok {
			i = len(out)
			byName[key] = i
			out = append(out, View{Schema: schema, Name: name, Comment: comment})
		}
		out[i].Columns = append(out[i].Columns, col)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// SELECT 授权：relacl 展开后的被授权角色；授权给 PUBLIC 的 grantee 为 0
	grants, err := p.Pool.Query(ctx, `
		SELECT n.nspname || '.' || c.relname, CASE WHEN g.grantee = 0 THEN 'PUBLIC' ELSE g.grantee::regrole::text END
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(c.relacl) g
		WHERE c.relkind IN ('v', 'm') AND g.privilege_type = 'SELECT' AND n.nspname || '.' || c.relname = ANY($1::text[])
		ORDER BY 1, 2`, qualified)
	if err != nil {
		return nil, err
	}
	err = forEach(grants, func(r pgx.Rows) error {
		var key, role string
		if err := r.Scan(&key, &role); err != nil {
			return err
		}
		if i, ok := byName[key]; ok {
			out[i].Grantees = append(out[i].Grantees, role)
		}
		return nil
	})
	return out, err
}

func forEach(rows pgx.Rows, f func(pgx.Rows) error) error {
	defer rows.Close()
	for rows.Next() {
		if err := f(rows); err != nil {
			return err
		}
	}
	return rows.Err()
}
