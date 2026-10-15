# 本机只需要 git、make 和 docker 客户端：未设置 IN_TOOLBOX 时，每个目标都委托给工具容器
# （deploy/toolbox/Dockerfile，经 Docker 上下文 DOCKER_CONTEXT 运行，默认本机 Docker）；容器里和 CI 里 IN_TOOLBOX=1，
# 执行真正的命令。
IN_TOOLBOX ?=

# 本机不入库的配置（远端 Docker 上下文、软件源镜像、真实模型开关、外部 LLM 的地址与密钥），格式见 .env.local.example；
# CI 里没有这个文件。放在最前面，下面的默认值都可以被它覆盖
-include .env.local

# 契约兼容性的比较基线。CI 的合并请求里指向目标分支。
BREAKING_REF ?= origin/main

# M=<模块目录> 只处理一个模块，如 make test M=services/retrieval
M ?=

ifeq ($(IN_TOOLBOX),)

# 为空时用本机 Docker；要在远端主机上构建和运行，在 .env.local 里设 DOCKER_CONTEXT=<上下文名>
DOCKER_CONTEXT ?=
export DOCKER_CONTEXT
# compose 每个并发操作都会经 SSH 新开一条连接；远端 sshd 默认 MaxStartups 10，并发多了会被断开
# （Connection closed ... past MaxStartups）。限制并发，避免 make up 中途失败。
COMPOSE_PARALLEL_LIMIT ?= 4
export COMPOSE_PARALLEL_LIMIT
# 并行开发时每个工作树设不同的 BF_TOOLBOX_TAG（如 t2、t7），各用一份工具镜像与虚拟环境
BF_TOOLBOX_TAG ?= dev
export BF_TOOLBOX_TAG
TOOLBOX := docker compose -f deploy/compose/toolbox.yaml
export BF_NPM_REGISTRY

DELEGATED := check lint lint-go lint-py lint-ci lint-ts test test-go test-py test-ts deps-check terms-check helm-check alerts-check \
	contracts-lint contracts-breaking contracts-openapi contracts-fresh
.PHONY: $(DELEGATED) contracts contracts-gen run fix

# 每次运行都重新构建镜像：只有最后一层（源码）会变，依赖与缓存在远端命名卷里。
$(DELEGATED):
	$(TOOLBOX) run --rm --build toolbox make $@ M=$(M) BREAKING_REF=$(BREAKING_REF)
	@docker image prune -f --filter label=org.bambuflow.role=toolbox >/dev/null

contracts: contracts-gen
	$(TOOLBOX) run --rm --build toolbox make contracts-lint contracts-breaking contracts-openapi BREAKING_REF=$(BREAKING_REF)

contracts-gen:
	@$(MAKE) --no-print-directory run CMD='make contracts-gen' OUT='contracts/gen/go/bambuflow contracts/gen/python/src/bambuflow contracts/gen/ts'


# 在工具容器里更新锁文件并自动修复格式（uv lock、ruff、go mod tidy、gofmt），再把容器里有变动的文件全部拷回。
# 修复与检查分开：先 make fix 带回结果，再 make check 判断是否通过，不会因检查失败而丢掉修复。
fix:
	@set -e; c=bambuflow-fix-$$$$; \
	trap 'docker rm -f $$c >/dev/null 2>&1 || true' EXIT; \
	$(TOOLBOX) run --build --name $$c toolbox make fix-local; \
	docker cp $$c:/tmp/changed.txt - | tar -xO > /tmp/bambuflow-changed.$$$$; \
	while read -r p; do [ -n "$$p" ] || continue; mkdir -p "$$(dirname "$$p")"; docker cp "$$c:/src/$$p" "$$p"; echo "已拷回 $$p"; done < /tmp/bambuflow-changed.$$$$; \
	rm -f /tmp/bambuflow-changed.$$$$
	@docker image prune -f --filter label=org.bambuflow.role=toolbox >/dev/null

# 在工具容器里执行 CMD，再把 OUT 列出的路径（相对仓库根目录）拷回本地，覆盖本地同名路径。
# 命令失败时不拷回。CMD 里的 shell 变量要写成 $$var（否则会被 make 先展开）。
# 用于生成代码、更新 go.sum / uv.lock 等需要把结果带回来的命令，如
#   make run CMD='cd services/gateway && go get github.com/x/y@v1.2.3' OUT='services/gateway/go.mod services/gateway/go.sum'
run:
	@set -e; c=bambuflow-run-$$$$; \
	trap 'docker rm -f $$c >/dev/null 2>&1 || true' EXIT; \
	$(TOOLBOX) run --build --name $$c toolbox sh -c '$(CMD)'; \
	for p in $(OUT); do rm -rf "$$p"; mkdir -p "$$(dirname "$$p")"; docker cp "$$c:/src/$$p" "$$p"; echo "已拷回 $$p"; done
	@docker image prune -f --filter label=org.bambuflow.role=toolbox >/dev/null

else

# 工具按固定版本编译一次，放进 TOOLS_DIR，文件名带版本号（golangci-lint 等版本升级改这里）。
# 不用 go run：它每次都要在构建缓存里重新链接，冷缓存时 vacuum 一项就要编译 7 分钟以上；
# 带版本号的二进制可以整目录缓存（工具容器的命名卷、CI 的 actions/cache），旧版本留着也不会被误用。
TOOLS_DIR ?= $(HOME)/.cache/bambuflow-tools
GOLANGCI_LINT := $(TOOLS_DIR)/golangci-lint-v2.14.0
ACTIONLINT    := $(TOOLS_DIR)/actionlint-v1.7.12
BUF           := $(TOOLS_DIR)/buf-v1.73.0
VACUUM        := $(TOOLS_DIR)/vacuum-v0.30.6
HELM          := $(TOOLS_DIR)/helm-v4.3.0
KUBECONFORM   := $(TOOLS_DIR)/kubeconform-v0.7.0
UV_RUN        := uv run --locked --all-packages

# $(1) 二进制名，$(2) 包路径，$(3) 版本
define go-tool
$(TOOLS_DIR)/$(1)-$(3):
	GOBIN=$(TOOLS_DIR)/.install go install $(2)@$(3)
	mv $(TOOLS_DIR)/.install/$(1) $$@
endef
$(eval $(call go-tool,golangci-lint,github.com/golangci/golangci-lint/v2/cmd/golangci-lint,v2.14.0))
$(eval $(call go-tool,actionlint,github.com/rhysd/actionlint/cmd/actionlint,v1.7.12))
$(eval $(call go-tool,buf,github.com/bufbuild/buf/cmd/buf,v1.73.0))
$(eval $(call go-tool,vacuum,github.com/daveshanley/vacuum,v0.30.6))
$(eval $(call go-tool,helm,helm.sh/helm/v4/cmd/helm,v4.3.0))
$(eval $(call go-tool,kubeconform,github.com/yannh/kubeconform/cmd/kubeconform,v0.7.0))

GO_MODULES = $(if $(M),$(wildcard $(M)/go.mod),$(shell go list -m -f '{{.Dir}}/go.mod'))
GO_DIRS    = $(patsubst %/go.mod,%,$(GO_MODULES))
PY_DIRS    = $(if $(M),$(wildcard $(M)/pyproject.toml),$(wildcard contracts/gen/python/pyproject.toml services/*/pyproject.toml services/*/*/pyproject.toml libs/*/pyproject.toml plugins/*/*/pyproject.toml))
PY_PATHS   = $(patsubst %/pyproject.toml,%,$(PY_DIRS))

.PHONY: bootstrap check lint lint-go lint-py lint-ci lint-ts test test-go test-py test-ts console-deps deps-check terms-check helm-check alerts-check \
	contracts contracts-gen contracts-lint contracts-breaking contracts-openapi contracts-fresh \
	fix-local e2e-tests eval-perm-seed eval-perm-tests bench-seed-local

bootstrap:
	go mod download
	uv sync --locked --all-packages

# 提交前至少跑这一项
check: lint test deps-check terms-check helm-check alerts-check

lint: lint-go lint-py lint-ci lint-ts

lint-go: $(GOLANGCI_LINT)
	@set -e; for d in $(GO_DIRS); do echo "golangci-lint $$d"; (cd $$d && $(GOLANGCI_LINT) run ./...); done

lint-py:
	$(UV_RUN) ruff check .
	$(UV_RUN) ruff format --check .
	@# 还没有 Python 模块时 mypy 无文件可查会报错，所以只在有模块时运行
	$(UV_RUN) mypy $(PY_PATHS) tools/e2e tools/test-upstream

lint-ci: $(ACTIONLINT)
	$(ACTIONLINT)

test: test-go test-py test-ts

test-go:
	@set -e; for d in $(GO_DIRS); do (cd $$d && go test -race ./...); done

test-py:
	$(if $(PY_PATHS),$(UV_RUN) pytest $(PY_PATHS),@echo "pytest: 暂无 Python 模块")

# console（TypeScript）：依赖按 package-lock.json 装进工具容器的命名卷；M 指定了别的模块时跳过
TS_DIRS = $(if $(M),$(filter services/console,$(M)),services/console)

console-deps:
	@for d in $(TS_DIRS); do (cd $$d && npm ci --no-audit --no-fund --loglevel=error); done

lint-ts: console-deps
	@for d in $(TS_DIRS); do (cd $$d && npm run --silent lint && npm run --silent typecheck && npx prettier --check .); done

test-ts: console-deps
	@for d in $(TS_DIRS); do (cd $$d && npm test --silent); done

# make fix 在容器里调用：修复后把有变动的文件（相对仓库根目录）列到 /tmp/changed.txt。
# ruff check --fix 修不掉的问题留给 make check 报告，这里不让它中断，否则修好的部分也带不回去。
fix-local:
	uv lock
	$(UV_RUN) ruff format .
	-$(UV_RUN) ruff check --fix --quiet .
	@set -e; for d in $(GO_DIRS); do (cd $$d && go mod tidy); done
	gofmt -l -w $(GO_DIRS)
	cd services/console && npm install --no-audit --no-fund --loglevel=error && npx prettier --write --log-level warn . && npx eslint --fix . || true
	git status --porcelain --untracked-files=all | awk '$$1 != "D" {print $$NF}' > /tmp/changed.txt

# 端到端测试，在 compose 网络里的 e2e 容器中执行（make e2e 调用）
e2e-tests:
	@# 只装 e2e 自己的依赖（不装整个工作区的开发工具），远端访问 PyPI 慢，少装能省下大部分时间
	uv run --locked --no-dev --package bambuflow-e2e pytest tools/e2e/e2e_tests

# 权限评测集的两步（make eval-perm 调用，在 e2e 容器中执行）
eval-perm-seed:
	cd tools/e2e && uv run --locked --no-dev --package bambuflow-e2e python -m perm_eval.seed

# CI 用：相对 BASE 的改动影响评测集里的哪些租户（逗号分隔，空表示不影响回答；evaluation 详设 5.1）
affected-tenants:
	@git diff --name-only $(BASE)...HEAD | uv run -q --locked --no-dev --package bambuflow-evaluation \
		python -m bambuflow_evaluation affected --golden golden/$(GOLDEN)

# make helm-smoke 用：在 e2e 容器里以测试用户 alice 登录，输出访问令牌
helm-smoke-token:
	cd tools/e2e && uv run -q --locked --no-dev --package bambuflow-e2e python -m e2e_tests.smoke_token

bench-seed-local:
	cd tools/e2e && uv run --locked --no-dev --package bambuflow-e2e python -m retrieval_bench.seed

eval-perm-tests:
	uv run --locked --no-dev --package bambuflow-e2e pytest -s tools/e2e/perm_eval

deps-check:
	go run ./tools/deps-check

# Helm chart：生产 values 必须通过 lint 与 schema 校验并能渲染；含测试 realm 与开发口令的反例必须被拒绝。
# 告警规则（deploy/observability/rules）：语法与表达式检查，再跑规则单元测试
# promtool 随工具镜像提供（deploy/toolbox/Dockerfile，与 compose 的 Prometheus 同一版本）
alerts-check:
	promtool check rules deploy/observability/rules/*.yaml
	promtool test rules deploy/observability/rules/tests/*.yaml
	promtool check config --syntax-only deploy/observability/prometheus/prometheus.yml

HELM_CHART := deploy/helm/bambuflow
# 离线入库平面的 chart，与在线平面分开部署
HELM_INGEST_CHART := deploy/helm/bambuflow-ingest
# 渲染结果再按 Kubernetes 的 schema 校验字段（Vault Secrets Operator 的自定义资源没有公开 schema，跳过）。
# kubeconform 的 schema 缓存在工具缓存卷里：每次从 GitHub 现拉在远端主机上常因网络失败
KUBECONFORM_ARGS := -strict -summary -kubernetes-version 1.33.0 -skip VaultConnection,VaultAuth,VaultStaticSecret -cache $(TOOLS_DIR)/kubeconform-cache
helm-check: $(HELM) $(KUBECONFORM)
	mkdir -p $(TOOLS_DIR)/kubeconform-cache
	$(HELM) lint --strict $(HELM_CHART) -f $(HELM_CHART)/values-prod.yaml
	$(HELM) template ci $(HELM_CHART) -f $(HELM_CHART)/values-prod.yaml | \
		$(KUBECONFORM) $(KUBECONFORM_ARGS)
	$(HELM) lint --strict $(HELM_INGEST_CHART) -f $(HELM_INGEST_CHART)/values-prod.yaml
	$(HELM) template ci $(HELM_INGEST_CHART) -f $(HELM_INGEST_CHART)/values-prod.yaml | \
		$(KUBECONFORM) $(KUBECONFORM_ARGS)
	@out=$$($(HELM) lint --strict $(HELM_CHART) -f deploy/helm/testdata/values-test-realm.yaml 2>&1) && \
		{ echo "$$out"; echo "含测试 realm 的 values 应当校验失败"; exit 1; } || \
		for f in /gateway/platformIssuer /vault/role /orchestrator/llmUrl /orchestrator/llmModel /events/redisAddr; do \
			echo "$$out" | grep -q "at '$$f'" || { echo "$$out"; echo "反例的 $$f 应被拒绝"; exit 1; }; \
		done

terms-check:
	go run ./tools/terms-check

# 动了 contracts/ 就跑这一项：重新生成代码，再做 lint、兼容性和 OpenAPI 命名检查。
contracts: contracts-gen contracts-lint contracts-breaking contracts-openapi

# 不用 buf 的 --clean：它会删掉整个输出目录，连同 gen/go/go.mod、gen/python/pyproject.toml。
contracts-gen: $(BUF) console-deps
	rm -rf contracts/gen/go/bambuflow contracts/gen/python/src/bambuflow contracts/gen/ts
	cd contracts && $(BUF) generate
	@# console 用的对外接口类型（OpenAPI → TypeScript）
	mkdir -p contracts/gen/ts
	cd services/console && npx openapi-typescript ../../contracts/openapi/v1/openapi.yaml -o ../../contracts/gen/ts/v1.d.ts

contracts-lint: $(BUF)
	cd contracts && $(BUF) lint && $(BUF) format --diff --exit-code

# 基线上还没有 contracts/ 时（首次引入）没有可比较的对象，跳过。
contracts-breaking: $(BUF)
	@if git cat-file -e $(BREAKING_REF):contracts/buf.yaml 2>/dev/null; then \
		cd contracts && $(BUF) breaking --against '../.git#ref=$(BREAKING_REF),subdir=contracts'; \
	else echo "contracts-breaking: $(BREAKING_REF) 上还没有 contracts/，跳过"; fi

# 正式规格必须全绿；反例规格必须恰好报出 4 处错误，证明命名规则本身有效。
contracts-openapi: $(VACUUM)
	cd contracts/openapi && $(VACUUM) lint -q -r ruleset.yaml -d --fail-severity warn v1/openapi.yaml
	@cd contracts/openapi && out=$$($(VACUUM) lint -r ruleset.yaml -d --fail-severity warn testdata/bad.yaml 2>&1); \
		echo "$$out" | grep -q "Failed with 4 errors" || { echo "$$out"; echo "反例规格应恰好报 4 处错误"; exit 1; }

# CI 用：提交的生成代码必须与契约一致。
contracts-fresh: contracts-gen
	@test -z "$$(git status --porcelain -- contracts/gen)" || { git status --porcelain -- contracts/gen; echo "生成代码与契约不一致，请运行 make contracts 并提交"; exit 1; }

endif

# ---- Compose 环境：在本机和 CI 里都直接调用 docker compose ----
# 运行环境所在的 Docker 上下文，默认与工具容器相同；真实模型可放到 GPU 主机（.env.local 里设 BF_STACK_CONTEXT）
BF_STACK_CONTEXT := $(or $(BF_STACK_CONTEXT),$(DOCKER_CONTEXT))
STACK_DOCKER := docker $(if $(BF_STACK_CONTEXT),--context $(BF_STACK_CONTEXT))
COMPOSE := $(STACK_DOCKER) compose -f deploy/compose/compose.yaml $(if $(filter 1,$(BF_MODELS_GPU)),-f deploy/compose/models-gpu.yaml)
BF_LLM_API_KEY ?=
export BF_LLM_URL BF_LLM_MODEL BF_LLM_API_KEY BF_LLM_EXTRA_BODY BF_NLI_DEFAULT BF_K3S_DOCKER_MIRROR
# 软件源镜像（构建镜像时用）：默认官方源，网络访问不了官方源时在 .env.local 里改
BF_GOPROXY := $(or $(BF_GOPROXY),https://proxy.golang.org,direct)
export BF_GOPROXY BF_PIP_INDEX_URL BF_DEBIAN_MIRROR BF_NPM_REGISTRY
# 本机端口（.env.local 里可改）：本机 Docker 演示时 BF_KC_PORT=8080，浏览器才能按令牌签发方 http://keycloak:8080 登录
export BF_KC_PORT BF_CONSOLE_PORT
# 逗号分隔，如 PROFILE=core,ingest
PROFILE ?= core
UP_WAIT ?= 300
# BF_MODELS=1：所有命令改用真实模型（models profile），嵌入与 NLI 地址指向开发用模型服务。
# 换模型后已有索引的嵌入模型不一致，需要先 make down VOLUMES=1 再重新入库。
ifeq ($(BF_MODELS),1)
override PROFILE := $(PROFILE),models
export BF_EMBEDDER_URL := http://models:8000
export BF_NLI_URL := http://models:8000
export BF_RERANKER_URL := http://models:8000
# 线上校验用 BF_NLI_DEFAULT（默认 110M），评测判定用更大的 330M（evaluation 详设 6.1）
export BF_EVAL_NLI_MODEL := nli-erlangshen-330m
# 首次启动要下载约 6 GB 权重
UP_WAIT := 3600
endif
COMPOSE_PROFILES_ARGS = $(foreach p,$(subst $(comma), ,$(PROFILE)),--profile $(p))
comma := ,

.PHONY: up down e2e eval-perm eval bench-seed eval-retrieval tunnel ingest reconcile demo helm-smoke helm-smoke-down dr-drill

up:
	$(if $(filter models,$(subst $(comma), ,$(PROFILE))),@$(STACK_DOCKER) volume create bambuflow-model-cache >/dev/null)
	$(COMPOSE) $(COMPOSE_PROFILES_ARGS) up -d --build --wait --wait-timeout $(UP_WAIT)
	@$(STACK_DOCKER) image prune -f --filter label=org.bambuflow.role >/dev/null

# 停掉全部 profile 的容器；数据卷保留，加 VOLUMES=1 一并删除
down:
	$(COMPOSE) --profile '*' down $(if $(VOLUMES),--volumes)

# 全量入库样例源并发布（P0 不引入 Temporal，依次运行；需先 make up PROFILE=core,ingest）：Wiki、共享盘、
# 可疑指令测试页（P3-I2）、日志（先写测试日志，P3-I3）、数据库表结构（先建演示库，P3-I4），再处理与发布。可重复运行。
# 三个 profile 都要启用：一次性任务依赖 core 的嵌入服务和 ingest 的 indexing。
ingest:
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm --build connectors
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm connectors-fileshare
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm connectors-inject
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm logs-seed
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm connectors-logs
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm dbschema-seed
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm connectors-dbschema
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm --build processing
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm indexing-publish

# 每日对账（connectors reconcile-*，部署时每天一次）：与已入库的文档比对后同步一轮，结果在 console 的数据源同步页
reconcile:
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm connectors reconcile-wiki
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm connectors-fileshare reconcile-fileshare

# 演示环境：起全部服务、入库样例数据、跑一次对账，然后按 docs/demo.md 在浏览器里演示
demo:
	$(MAKE) --no-print-directory up PROFILE=core,ingest
	$(MAKE) --no-print-directory ingest
	$(MAKE) --no-print-directory reconcile
	@echo "演示环境已就绪：console http://localhost:$(or $(BF_CONSOLE_PORT),27300)（远端 Docker 先 make tunnel）。"
	@echo "浏览器所在机器的 hosts 里要有 \"127.0.0.1 keycloak\"，账号与演示路线见 docs/demo.md。"

e2e:
	@$(STACK_DOCKER) volume create bambuflow-toolbox_uv-cache >/dev/null
	$(COMPOSE) --profile test run --rm --build e2e

# 灾备演练（总体设计 11.5，indexing backup / restore）：备份一次 → 在同一 PostgreSQL 的 bambuflow_dr 库（充当灾备站点）里
# 恢复最新快照 → 核对线上版本的文档数、块数与正文和向量的摘要两边一致 → 起一个指向灾备库的 retrieval 并等它就绪。
# 需先 make up PROFILE=core,ingest 与 make ingest；bambuflow_dr 每次重建。
DR_CHECK := SELECT count(DISTINCT vd.doc_id) || ' 份文档 ' || count(*) || ' 块 ' || md5(string_agg(c.chunk_id || c.text || c.embedding::text, '' ORDER BY c.chunk_id, c.doc_version)) \
	FROM idx.aliases a JOIN idx.version_docs vd ON vd.version = a.version \
	JOIN idx.chunks c ON c.doc_id = vd.doc_id AND c.doc_version = vd.doc_version WHERE a.alias = 'prod'
dr-drill:
	$(COMPOSE) --profile core --profile ingest --profile ingest-job run --rm indexing-backup
	$(COMPOSE) exec -T postgres psql -U bambuflow -d bambuflow -v ON_ERROR_STOP=1 -c 'DROP DATABASE IF EXISTS bambuflow_dr' -c 'CREATE DATABASE bambuflow_dr'
	$(COMPOSE) --profile test run --rm indexing-dr migrate
	$(COMPOSE) --profile test run --rm indexing-dr
	@src="$$($(COMPOSE) exec -T postgres psql -U bambuflow -d bambuflow -Atc "$(DR_CHECK)")"; \
	dr="$$($(COMPOSE) exec -T postgres psql -U bambuflow -d bambuflow_dr -Atc "$(DR_CHECK)")"; \
	echo "线上：$$src"; echo "灾备：$$dr"; \
	test -n "$$src" && test "$$src" = "$$dr" || { echo "灾备库与线上不一致"; exit 1; }
	$(COMPOSE) --profile test up -d --wait retrieval-dr
	$(COMPOSE) --profile test rm -sf retrieval-dr
	@echo "灾备演练通过：快照恢复后与线上一致，retrieval 指向灾备库可以就绪"

# 权限评测集 P0 版（tools/e2e/perm_eval）：写入合成文档 → 入库并发布 → 与权限表的暴力计算逐条比对，越权必须为 0；
# 另起三个故障实例验证 fail-closed。需先 make up PROFILE=core,ingest 并 make ingest 过一次。动了权限相关代码必跑。
eval-perm:
	@$(STACK_DOCKER) volume create bambuflow-toolbox_uv-cache >/dev/null
	$(COMPOSE) --profile test up -d retrieval-nopolicy retrieval-nodb retrieval-nogrants
	$(COMPOSE) --profile test run --rm --build e2e make eval-perm-seed
	$(MAKE) --no-print-directory ingest
	$(COMPOSE) --profile test run --rm e2e make eval-perm-tests

# 评测（services/evaluation）：同一评测配置连跑三次，报告拷回 golden/$(GOLDEN)/reports/（report.md、report.json）。
# 需先 make up 与 make ingest。退出码 0 通过、1 不通过、2 无法判定；不论结论如何都拷回报告。
# WRITE_BASELINE=1 时写 baseline.md、baseline.json 作为之后运行的比较基线（基线随评测集提交）。
GOLDEN ?= wiki-demo
# 模拟服务与真实模型的指标不可比，各自一份基线：baseline（CI 回归）与 baseline-real（BF_MODELS=1）
EVAL_BASELINE ?= $(if $(filter 1,$(BF_MODELS)),baseline-real,baseline)
eval:
	@set -e; c=bambuflow-eval-$$$$; \
	trap '$(STACK_DOCKER) rm -f $$c >/dev/null 2>&1 || true' EXIT; \
	rc=0; $(COMPOSE) --profile test run --build --name $$c evaluation run --golden golden/$(GOLDEN) --repeat 3 \
		$(if $(TENANTS),--tenants $(TENANTS)) \
		--baseline $(EVAL_BASELINE) $(if $(WRITE_BASELINE),--write-baseline) >/dev/null || rc=$$?; \
	mkdir -p golden/$(GOLDEN)/reports; $(STACK_DOCKER) cp $$c:/out/. golden/$(GOLDEN)/reports/; \
	echo "报告已拷回 golden/$(GOLDEN)/reports/"; exit $$rc

# 只检索评测（公开评测集子集 golden/t2retrieval）：先 make bench-seed 再 make ingest 发布，然后 make eval-retrieval。
# 需要真实嵌入模型才有意义（BF_MODELS=1）；WRITE_BASELINE=1 时重写基线。
bench-seed:
	@$(STACK_DOCKER) volume create bambuflow-toolbox_uv-cache >/dev/null
	$(COMPOSE) --profile test run --rm --build e2e make bench-seed-local

eval-retrieval:
	@set -e; c=bambuflow-evalr-$$$$; \
	trap '$(STACK_DOCKER) rm -f $$c >/dev/null 2>&1 || true' EXIT; \
	rc=0; $(COMPOSE) --profile test run --build --name $$c evaluation retrieval --set golden/t2retrieval \
		--baseline $(or $(RETRIEVAL_BASELINE),baseline) $(if $(RERANK),--rerank) $(if $(WRITE_BASELINE),--write-baseline) \
		>/dev/null || rc=$$?; \
	mkdir -p golden/t2retrieval/reports; $(STACK_DOCKER) cp $$c:/out/. golden/t2retrieval/reports/; \
	echo "报告已拷回 golden/t2retrieval/reports/"; exit $$rc

# Helm 冒烟测试：在栈所在主机起单节点 k3s，推送生产镜像（runtime 阶段）到本栈的镜像仓库，装 chart 后经 gateway 问答一次
# （tools/helm-smoke/smoke.sh）。需先 make up 并 make ingest；测完 make helm-smoke-down 释放 k3s。
HELM_SMOKE_CHARTS := vault-0.34.1 vault-secrets-operator-1.6.0
HELM_SMOKE_REGISTRY := localhost:$(or $(BF_REGISTRY_PORT),27500)/bambuflow
helm-smoke:
	@mkdir -p tools/helm-smoke/charts; for c in $(HELM_SMOKE_CHARTS); do \
		[ -f tools/helm-smoke/charts/$$c.tgz ] || curl -sfL -o tools/helm-smoke/charts/$$c.tgz https://helm.releases.hashicorp.com/$$c.tgz; done
	$(COMPOSE) --profile k8s up -d --wait registry k3s
	$(STACK_DOCKER) build -q --target runtime -f services/gateway/Dockerfile --build-arg GOPROXY=$(BF_GOPROXY) -t $(HELM_SMOKE_REGISTRY)/gateway:smoke .
	$(STACK_DOCKER) build -q --target runtime -f services/retrieval/Dockerfile --build-arg GOPROXY=$(BF_GOPROXY) -t $(HELM_SMOKE_REGISTRY)/retrieval:smoke .
	$(STACK_DOCKER) build -q -f services/qa/orchestrator/Dockerfile -t $(HELM_SMOKE_REGISTRY)/orchestrator:smoke .
	$(STACK_DOCKER) build -q --target runtime -f services/governance/Dockerfile --build-arg GOPROXY=$(BF_GOPROXY) -t $(HELM_SMOKE_REGISTRY)/governance:smoke .
	$(STACK_DOCKER) build -q --target runtime -f services/qa/batch/Dockerfile --build-arg GOPROXY=$(BF_GOPROXY) -t $(HELM_SMOKE_REGISTRY)/batch:smoke .
	$(STACK_DOCKER) build -q -f services/evaluation/Dockerfile -t $(HELM_SMOKE_REGISTRY)/evaluation:smoke .
	@# chart 里每个组件的镜像都要在这里构建并推送（webhook 用 governance 镜像）
	for s in gateway retrieval orchestrator governance batch evaluation; do $(STACK_DOCKER) push -q $(HELM_SMOKE_REGISTRY)/$$s:smoke; done
	$(COMPOSE) --profile k8s-run run --rm --build helm-smoke install
	@# 令牌在安装完成后再取（有效期短），只经环境变量传给检查步骤，不打印
	@$(STACK_DOCKER) volume create bambuflow-toolbox_uv-cache >/dev/null
	@set -e; SMOKE_TOKEN=$$($(COMPOSE) --profile test run --rm --build -T e2e make -s helm-smoke-token | tail -1); \
		[ -n "$$SMOKE_TOKEN" ] || { echo "取不到测试令牌"; exit 1; }; \
		SMOKE_TOKEN=$$SMOKE_TOKEN $(COMPOSE) --profile k8s-run run --rm helm-smoke check

# 只删 k3s 与镜像仓库的容器和卷，不动栈里其他服务的数据
helm-smoke-down:
	$(COMPOSE) --profile k8s rm -sf registry k3s
	$(STACK_DOCKER) volume rm -f bambuflow_k3s-data bambuflow_k3s-config bambuflow_registry-data

# 把远端 127.0.0.1 上的端口转到本机；令牌的签发方是 http://keycloak:8080，浏览器访问前在 /etc/hosts 加 "127.0.0.1 keycloak"。
tunnel:
	@host=$$(docker context inspect $(or $(BF_STACK_CONTEXT),default) --format '{{.Endpoints.docker.Host}}'); \
	case "$$host" in ssh://*) ;; *) echo "当前 Docker 上下文不是 ssh 远端，不需要隧道"; exit 0;; esac; \
	hp=$${host#ssh://}; port=$${hp##*:}; [ "$$port" = "$$hp" ] && port=22; hp=$${hp%:*}; \
	echo "Keycloak http://keycloak:8080（本机 8080） · Jaeger http://localhost:27686 · Vault http://localhost:27200 · PG localhost:27432 · Redis localhost:27379 · S3 http://localhost:27900 · gateway http://localhost:27088 · console http://localhost:27300 · Prometheus http://localhost:27090 · Grafana http://localhost:27030"; \
	ssh -N -p $$port -L 8080:127.0.0.1:27080 -L 27686:127.0.0.1:27686 -L 27200:127.0.0.1:27200 -L 27432:127.0.0.1:27432 -L 27379:127.0.0.1:27379 -L 27900:127.0.0.1:27900 -L 27088:127.0.0.1:27088 -L 27300:127.0.0.1:27300 -L 27090:127.0.0.1:27090 -L 27030:127.0.0.1:27030 $$hp
