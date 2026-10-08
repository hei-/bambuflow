// 开发与 CI 专用的工具（Vault 种子、服务桩），打进测试工具镜像，不进生产镜像。
module github.com/bambu-ai/bambuflow/tools/devtools

go 1.26.0

replace (
	github.com/bambu-ai/bambuflow/contracts/gen/go => ../../contracts/gen/go
	github.com/bambu-ai/bambuflow/libs/platform-go => ../../libs/platform-go
)

require (
	github.com/bambu-ai/bambuflow/libs/platform-go v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v5 v5.11.0
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)
