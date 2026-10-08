// Package schema 是 indexing 拥有的索引表结构，由 indexing migrate 执行。
package schema

import "embed"

//go:embed sql/*.sql
var Files embed.FS
