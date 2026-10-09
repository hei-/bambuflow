module github.com/bambu-ai/bambuflow/services/indexing

go 1.26.0

replace (
	github.com/bambu-ai/bambuflow/contracts/gen/go => ../../contracts/gen/go
	github.com/bambu-ai/bambuflow/libs/platform-go => ../../libs/platform-go
	github.com/bambu-ai/bambuflow/libs/textseg => ../../libs/textseg
)

require (
	github.com/bambu-ai/bambuflow/contracts/gen/go v0.0.0-00010101000000-000000000000
	github.com/bambu-ai/bambuflow/libs/platform-go v0.0.0-00010101000000-000000000000
	github.com/bambu-ai/bambuflow/libs/textseg v0.0.0-00010101000000-000000000000
	github.com/jackc/pgx/v5 v5.11.0
	github.com/pgvector/pgvector-go v0.4.1
	github.com/pgvector/pgvector-go/pgx v0.4.1
	github.com/redis/go-redis/v9 v9.23.0
	go.yaml.in/yaml/v3 v3.0.5
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-ego/gse v1.1.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/vcaesar/cedar v0.50.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260928230214-8a89bd6388cc // indirect
)
