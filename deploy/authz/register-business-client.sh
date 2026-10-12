#!/usr/bin/env bash
# 登记可代用户调用的业务系统（gateway 详设 3.1、3.4）：在平台授权服务（Keycloak）里建该业务系统的客户端，
# 它换得的令牌受众为本平台、带 act（RFC 8693，值为客户端自己），并允许它拿用户的公司 IdP 令牌做令牌交换。
#
# 用法（在能访问 Keycloak 管理接口的运维机上，需要 curl 与 jq；管理员口令从标准输入读，不出现在命令行里）：
#   KC_URL=https://authz.corp.example ./register-business-client.sh <client_id> < 口令文件
# 可选：KC_REALM（默认 bambuflow）、KC_IDP（上游身份源的别名，默认 corp-idp）、KC_ADMIN_USER（默认 admin）。
# 重复执行不报错：已有的客户端与授权都保留。
#
# 之后还要：
# - 客户端密钥由 Keycloak 生成，本脚本不输出；在管理控制台该客户端的“凭据”页取出，交给业务系统存进它自己的密钥管理；
# - 业务系统调用 gateway 时出示的 mTLS 客户端证书，CN 须与 client_id 一致；
# - 在租户配置的 bound_clients 里登记这个 client_id（经 governance 的变更单审批）。
#
# 业务系统换令牌（RFC 8693，外部令牌换内部令牌）：
#   POST <KC_URL>/realms/<realm>/protocol/openid-connect/token
#     grant_type=urn:ietf:params:oauth:grant-type:token-exchange
#     subject_token=<用户的公司 IdP 访问令牌>  subject_token_type=urn:ietf:params:oauth:token-type:access_token
#     subject_issuer=<KC_IDP>  client_id=<client_id>  client_secret=<密钥>
set -euo pipefail

client=${1:?用法: register-business-client.sh <client_id>}
if [[ ! $client =~ ^[a-z][a-z0-9-]{1,62}$ ]]; then
	echo "client_id 只能由小写字母、数字和连字符组成，以字母开头" >&2
	exit 2
fi
kc=${KC_URL:?需要 KC_URL}
realm=${KC_REALM:-bambuflow}
idp=${KC_IDP:-corp-idp}
IFS= read -r password

token=$(curl -fsS "$kc/realms/master/protocol/openid-connect/token" \
	--data-urlencode grant_type=password --data-urlencode client_id=admin-cli \
	--data-urlencode "username=${KC_ADMIN_USER:-admin}" --data-urlencode "password=$password" | jq -er .access_token)
unset password

api() {
	local method=$1 path=$2
	shift 2
	curl -fsS -X "$method" -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
		"$kc/admin/realms/$realm/$path" "$@"
}

# ① 客户端：只做令牌交换，不开浏览器登录、密码登录与服务账号（服务账号的令牌没有用户身份，gateway 一律拒绝）
id=$(api GET "clients?clientId=$client" | jq -r '.[0].id // empty')
if [[ -z $id ]]; then
	api POST clients -d "$(jq -n --arg c "$client" '{
		clientId: $c, name: $c, enabled: true, publicClient: false, clientAuthenticatorType: "client-secret",
		standardFlowEnabled: false, implicitFlowEnabled: false, directAccessGrantsEnabled: false, serviceAccountsEnabled: false,
		attributes: {"access.token.signed.response.alg": "ES256"},
		protocolMappers: [
			{name: "platform-audience", protocol: "openid-connect", protocolMapper: "oidc-audience-mapper",
			 config: {"included.custom.audience": "bambuflow-platform", "access.token.claim": "true", "id.token.claim": "false"}},
			{name: "act", protocol: "openid-connect", protocolMapper: "oidc-hardcoded-claim-mapper",
			 config: {"claim.name": "act", "claim.value": ({sub: $c} | tojson), "jsonType.label": "JSON",
			          "access.token.claim": "true", "id.token.claim": "false", "userinfo.token.claim": "false"}}
		]}')"
	id=$(api GET "clients?clientId=$client" | jq -er '.[0].id')
	echo "已建客户端 $client"
else
	echo "客户端 $client 已存在，保持不变"
fi

# ② 令牌交换授权：上游身份源的 token-exchange 权限里加一条只含该客户端的策略（多条策略任一满足即放行）
perm=$(api PUT "identity-provider/instances/$idp/management/permissions" -d '{"enabled": true}' | jq -er '.scopePermissions["token-exchange"]')
rm_id=$(api GET "clients?clientId=realm-management" | jq -er '.[0].id')
authz="clients/$rm_id/authz/resource-server"
name="token-exchange:$client"
policy=$(api GET "$authz/policy?name=$name" | jq -r --arg n "$name" '.[] | select(.name == $n) | .id')
if [[ -z $policy ]]; then
	policy=$(api POST "$authz/policy/client" -d "$(jq -n --arg n "$name" --arg id "$id" \
		'{name: $n, clients: [$id], logic: "POSITIVE", decisionStrategy: "UNANIMOUS"}')" | jq -er .id)
fi
current=$(api GET "$authz/policy/$perm/associatedPolicies" | jq -c '[.[].id]')
api GET "$authz/permission/scope/$perm" | jq --argjson cur "$current" --arg p "$policy" \
	'. + {policies: ($cur + [$p] | unique), decisionStrategy: "AFFIRMATIVE"}' |
	api PUT "$authz/permission/scope/$perm" -d @- >/dev/null
echo "已允许 $client 用 $idp 的用户令牌做令牌交换"
