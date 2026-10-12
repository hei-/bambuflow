# 平台授权服务（Keycloak，技术设计文档 ADR-13、gateway 详设 3.4）。
# platform：生产镜像，只含平台 realm 模板（config/authz/）；上游 IdP 地址与凭证由部署时的环境变量填入。
# dev：在 platform 基础上加入测试用上游 realm（tools/test-upstream/），只用于开发与 CI。
FROM quay.io/keycloak/keycloak:26.8.0 AS platform
LABEL org.bambuflow.role=authz
# token-exchange（v1）支持外部令牌换内部令牌：业务系统拿用户的公司 IdP 令牌换平台令牌（gateway 详设 3.1）；
# 哪些客户端可以换由上游身份源上的细粒度权限决定，所以同时启用 admin-fine-grained-authz:v1（deploy/authz/register-business-client.sh）
ENV KC_HEALTH_ENABLED=true \
    KC_HTTP_ENABLED=true \
    KC_FEATURES=token-exchange,admin-fine-grained-authz:v1
COPY config/authz/bambuflow-realm.json /opt/keycloak/data/import/

FROM platform AS dev
COPY tools/test-upstream/corp-idp-realm.json /opt/keycloak/data/import/
