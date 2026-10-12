#!/bin/sh
# Helm 冒烟测试（make helm-smoke），在 compose 网络里的 helm-smoke 容器中运行，目标是同一网络里的单节点 k3s。
#   smoke.sh install  配置 Vault 的 Kubernetes 认证与密钥 → 装 Vault Agent Injector、Vault Secrets Operator → 装 chart
#   smoke.sh check    检查密钥同步、审计卷、HPA、PDB，经 gateway 问答一次、提交一个批次并等它完成，验证 NetworkPolicy 拦截其他命名空间
# 依赖服务（Postgres、Vault、对象存储、Keycloak、模型）用 compose 栈里的；只用于开发环境。
set -eu
NS=bambuflow
REL=smoke
export KUBECONFIG=/tmp/kubeconfig

step() { printf '\n== %s\n' "$*"; }
fail() { echo "失败: $*" >&2; exit 1; }

# k3s 写的 kubeconfig 指向 127.0.0.1，从这个容器访问要换成服务名
i=0
until [ -s /k3s/k3s.yaml ] && sed 's#https://127.0.0.1:6443#https://k3s:6443#' /k3s/k3s.yaml >"$KUBECONFIG" &&
	kubectl get --raw /readyz >/dev/null 2>&1; do
	i=$((i + 1)); [ "$i" -lt 90 ] || fail "k3s 没有就绪"; sleep 2
done

vault_api() { # 方法 路径 [JSON]
	curl -sSf -H "X-Vault-Token: $VAULT_TOKEN" -X "$1" "$VAULT_ADDR/v1/$2" ${3:+-d "$3"}
}

install() {
	step "CoreDNS 解析 compose 服务名"
	# k3s 的 CoreDNS 转发不到 compose 网络的内置 DNS，按当前 IP 用 hosts 插件写死；compose 服务重建换了 IP 时重跑 install
	zones="" entries=""
	for h in keycloak postgres redis vault rustfs jaeger embedder models llm nli openldap k3s; do
		ip=$(getent hosts "$h" | awk '{print $1; exit}') || true
		[ -n "$ip" ] || continue
		zones="$zones $h"
		entries="$entries        $ip $h
"
	done
	kubectl apply -f - <<YAML
apiVersion: v1
kind: ConfigMap
metadata: {name: coredns-custom, namespace: kube-system}
data:
  compose.server: |
    ${zones# } {
      hosts {
$entries        fallthrough
      }
    }
YAML
	kubectl -n kube-system rollout restart deploy/coredns
	kubectl -n kube-system rollout status deploy/coredns --timeout=120s

	step "Vault：Kubernetes 认证、策略、角色、各服务的密钥"
	kubectl -n kube-system create serviceaccount vault-reviewer --dry-run=client -o yaml | kubectl apply -f -
	kubectl create clusterrolebinding vault-reviewer --clusterrole=system:auth-delegator \
		--serviceaccount=kube-system:vault-reviewer --dry-run=client -o yaml | kubectl apply -f -
	jwt=$(kubectl -n kube-system create token vault-reviewer --duration=24h)
	ca=$(kubectl config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' | base64 -d)
	vault_api GET sys/auth | jq -e '."kubernetes/"' >/dev/null ||
		vault_api POST sys/auth/kubernetes '{"type":"kubernetes"}' >/dev/null
	vault_api POST auth/kubernetes/config "$(jq -n --arg ca "$ca" --arg jwt "$jwt" \
		'{kubernetes_host: "https://k3s:6443", kubernetes_ca_cert: $ca, token_reviewer_jwt: $jwt}')" >/dev/null
	vault_api PUT sys/policies/acl/bambuflow-online "$(jq -n \
		--arg p 'path "secret/data/bambuflow/*" { capabilities = ["read"] }
path "secret/data/governance/*" { capabilities = ["read"] }
path "transit/decrypt/bambuflow-pii" { capabilities = ["update"] }
path "transit/datakey/plaintext/bambuflow-answers" { capabilities = ["update"] }
path "transit/decrypt/bambuflow-answers" { capabilities = ["update"] }' '{policy: $p}')" >/dev/null
	vault_api POST auth/kubernetes/role/bambuflow-online "$(jq -n --arg rel "$REL" --arg ns "$NS" \
		'{bound_service_account_names: [$rel + "-retrieval", $rel + "-governance", $rel + "-orchestrator", $rel + "-vault-sync"],
		  bound_service_account_namespaces: [$ns], policies: ["bambuflow-online"], ttl: "1h"}')" >/dev/null
	# webhook 单独一个角色：只能读写订阅的签名密钥
	vault_api PUT sys/policies/acl/bambuflow-webhook "$(jq -n \
		--arg p 'path "secret/data/webhooks/*" { capabilities = ["create", "update", "read"] }' '{policy: $p}')" >/dev/null
	vault_api POST auth/kubernetes/role/bambuflow-webhook "$(jq -n --arg rel "$REL" --arg ns "$NS" \
		'{bound_service_account_names: [$rel + "-webhook"],
		  bound_service_account_namespaces: [$ns], policies: ["bambuflow-webhook"], ttl: "1h"}')" >/dev/null
	# gateway 单独一个角色：只能读写 Principal 签名密钥（定期轮换并写回）
	vault_api PUT sys/policies/acl/bambuflow-gateway "$(jq -n \
		--arg p 'path "secret/data/gateway/principal-signing" { capabilities = ["create", "update", "read"] }' '{policy: $p}')" >/dev/null
	vault_api POST auth/kubernetes/role/bambuflow-gateway "$(jq -n --arg rel "$REL" --arg ns "$NS" \
		'{bound_service_account_names: [$rel + "-gateway"],
		  bound_service_account_namespaces: [$ns], policies: ["bambuflow-gateway"], ttl: "1h"}')" >/dev/null
	dsn="postgres://bambuflow:$BF_PG_PASSWORD@postgres:5432/bambuflow?sslmode=disable"
	vault_api POST secret/data/bambuflow/gateway "$(jq -n --arg dsn "$dsn" --arg ak "$BF_S3_ACCESS_KEY" --arg sk "$BF_S3_SECRET_KEY" \
		'{data: {BF_PG_DSN: $dsn, BF_AUDIT_S3_ACCESS_KEY: $ak, BF_AUDIT_S3_SECRET_KEY: $sk}}')" >/dev/null
	vault_api POST secret/data/bambuflow/retrieval "$(jq -n --arg dsn "$dsn" '{data: {BF_PG_DSN: $dsn}}')" >/dev/null
	# governance 的管理接口把变更审计写进审计桶
	vault_api POST secret/data/bambuflow/governance "$(jq -n --arg dsn "$dsn" --arg ak "$BF_S3_ACCESS_KEY" --arg sk "$BF_S3_SECRET_KEY" \
		'{data: {BF_PG_DSN: $dsn, BF_AUDIT_S3_ACCESS_KEY: $ak, BF_AUDIT_S3_SECRET_KEY: $sk}}')" >/dev/null
	vault_api POST secret/data/bambuflow/identity-sync "$(jq -n --arg dsn "$dsn" --arg pw "$BF_LDAP_ADMIN_PASSWORD" \
		'{data: {BF_PG_DSN: $dsn, LDAP_BIND_PASSWORD: $pw}}')" >/dev/null
	vault_api POST secret/data/bambuflow/batch "$(jq -n --arg dsn "$dsn" '{data: {BF_PG_DSN: $dsn}}')" >/dev/null
	vault_api POST secret/data/bambuflow/webhook "$(jq -n --arg dsn "$dsn" '{data: {BF_PG_DSN: $dsn}}')" >/dev/null
	# 在线抽样评审的表由 compose 栈的 evaluation-migrate 建好
	vault_api POST secret/data/bambuflow/evaluation "$(jq -n --arg dsn "$dsn" '{data: {BF_PG_DSN: $dsn}}')" >/dev/null
	# 回答记录的表结构由 compose 栈的 orchestrator-migrate 建好
	vault_api POST secret/data/bambuflow/orchestrator "$(jq -n --arg dsn "$dsn" \
		'{data: {OTEL_SERVICE_NAME: "orchestrator", BF_PG_DSN: $dsn}}')" >/dev/null
	# 模拟 LLM 不校验密钥
	vault_api POST secret/data/bambuflow/orchestrator-llm '{"data": {"api_key": "unused-by-mock-llm"}}' >/dev/null

	step "Vault Agent Injector、Vault Secrets Operator"
	helm upgrade --install vault /smoke/charts/vault-[0-9]*.tgz -n vault --create-namespace \
		--set server.enabled=false --set injector.externalVaultAddr="$VAULT_ADDR" --wait --timeout 10m
	helm upgrade --install vso /smoke/charts/vault-secrets-operator-*.tgz -n vault-secrets-operator --create-namespace \
		--wait --timeout 10m

	step "安装 chart"
	for ns in "$NS" smoke other; do
		kubectl create namespace "$ns" --dry-run=client -o yaml | kubectl apply -f -
	done
	# 常驻的客户端 Pod：k3s 的网络策略控制器按 Pod IP 维护放行名单，新建的 Pod 要过一会儿才进名单，
	# 现建现用的临时 Pod 会被误拒
	for ns in smoke other; do
		kubectl -n "$ns" get pod client >/dev/null 2>&1 ||
			kubectl -n "$ns" run client --image=curlimages/curl:8.16.0 --restart=Never --command -- sleep infinity
	done
	kubectl -n smoke wait --for=condition=Ready pod/client --timeout=300s
	kubectl -n other wait --for=condition=Ready pod/client --timeout=300s
	helm upgrade --install "$REL" /chart -n "$NS" -f /smoke/values-smoke.yaml --skip-schema-validation \
		--set retrieval.embedderUrl="$BF_EMBEDDER_URL" --set retrieval.rerankerUrl="$BF_RERANKER_URL" \
		--set orchestrator.nliUrl="$BF_NLI_URL" --wait --timeout 15m ||
		{ kubectl -n "$NS" get pods; kubectl -n "$NS" describe pods | tail -60; fail "chart 没有在 15 分钟内就绪"; }
	kubectl -n "$NS" get pods -o wide
}

# 从指定命名空间的客户端 Pod 访问 gateway；令牌经标准输入传入，不出现在命令行与 Pod 定义里
ask() { # 命名空间
	echo "$SMOKE_TOKEN" | kubectl -n "$1" exec -i client -- sh -c \
		'read -r t; curl -s -m 20 -o /tmp/out -w "%{http_code}" -H "Authorization: Bearer $t" -H "Bambuflow-Tenant: credit" \
		  -H "Content-Type: application/json" -d "{\"question\":\"个人消费贷款的额度上限是多少？\"}" \
		  http://'"$REL"'-gateway.'"$NS"':8080/v1/answers; echo; head -c 400 /tmp/out' 2>&1 || true
}

# 提交一个只有一条问题的批次，输出响应体
batch() { # 命名空间
	echo "$SMOKE_TOKEN" | kubectl -n "$1" exec -i client -- sh -c \
		'read -r t; printf "%s\n" "{\"custom_id\":\"q1\",\"question\":\"个人消费贷款的额度上限是多少？\"}" >/tmp/batch.jsonl
		 curl -s -m 20 -H "Authorization: Bearer $t" -H "Bambuflow-Tenant: credit" \
		  -F mode=answer -F "input_file=@/tmp/batch.jsonl;type=application/x-ndjson" \
		  http://'"$REL"'-gateway.'"$NS"':8080/v1/batches' 2>&1 || true
}

batch_status() { # 命名空间 批次 ID
	echo "$SMOKE_TOKEN" | kubectl -n "$1" exec -i client -- sh -c \
		'read -r t; curl -s -m 10 -H "Authorization: Bearer $t" -H "Bambuflow-Tenant: credit" \
		  http://'"$REL"'-gateway.'"$NS"':8080/v1/batches/'"$2" 2>&1 || true
}

check() {
	[ -n "${SMOKE_TOKEN:-}" ] || fail "未设置 SMOKE_TOKEN"
	step "Vault Secrets Operator 同步的 Secret"
	for c in gateway retrieval orchestrator orchestrator-llm governance batch evaluation webhook; do
		kubectl -n "$NS" get secret "$REL-$c" >/dev/null || fail "Secret $REL-$c 不存在"
	done
	step "gateway 的审计卷与 Vault Agent"
	kubectl -n "$NS" get pvc -o wide
	[ "$(kubectl -n "$NS" get pvc "audit-$REL-gateway-0" -o jsonpath='{.status.phase}')" = Bound ] || fail "审计卷没有绑定"
	kubectl -n "$NS" get pod "$REL-gateway-0" -o jsonpath='{range .spec.containers[*]}{.name}{" "}{end}' | grep -q vault-agent ||
		fail "gateway 没有注入 Vault Agent"
	pod=$(kubectl -n "$NS" get pod -l app.kubernetes.io/name=webhook -o jsonpath='{.items[0].metadata.name}')
	kubectl -n "$NS" get pod "$pod" -o jsonpath='{range .spec.containers[*]}{.name}{" "}{end}' | grep -q vault-agent ||
		fail "webhook 没有注入 Vault Agent"
	step "HPA 与 PDB"
	kubectl -n "$NS" get hpa,pdb
	step "从入口命名空间（smoke）经 gateway 问答"
	out=$(ask smoke); echo "$out"
	echo "$out" | head -1 | grep -q '^200$' || fail "问答没有返回 200"
	echo "$out" | grep -q '"status":"answered"' || fail "问答没有回答"
	step "经 gateway 提交一个批次，等 qa/batch 执行完（gateway → batch → gateway 换发 → orchestrator）"
	batch_id=$(batch smoke | sed -n 's/.*"batch_id":"\(bat_[^"]*\)".*/\1/p')
	[ -n "$batch_id" ] || fail "批次没有受理"
	i=0
	until batch_status smoke "$batch_id" | grep -q '"status":"completed"'; do
		i=$((i + 1)); [ "$i" -lt 60 ] || { batch_status smoke "$batch_id"; fail "批次没有在 2 分钟内完成"; }; sleep 2
	done
	step "其他命名空间访问 gateway 应被 NetworkPolicy 拦截"
	out=$(ask other); echo "$out"
	echo "$out" | head -1 | grep -q '^000$' || fail "NetworkPolicy 没有拦截其他命名空间"
	step "审计送出（gateway 日志里不应有送出失败）"
	sleep 70 # 默认每分钟送一轮
	if kubectl -n "$NS" logs "$REL-gateway-0" -c gateway | grep -q '审计送出失败'; then
		kubectl -n "$NS" logs "$REL-gateway-0" -c gateway | grep '审计送出失败' | tail -3
		fail "审计送出失败"
	fi
	echo; echo "Helm 冒烟测试通过"
}

case "${1:-}" in
install) install ;;
check) check ;;
*) fail "用法: smoke.sh install | check" ;;
esac
