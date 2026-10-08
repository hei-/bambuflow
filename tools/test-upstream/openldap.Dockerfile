# 测试用公司目录：OpenLDAP + 生成的测试用户与群组。只用于开发与 CI，生产 chart 不包含它。
# osixia/openldap 已停止更新（最后版本 1.5.0），对测试上游够用；若需更新再换镜像。
FROM python:3.12-slim-bookworm AS seed
ARG TEST_USER_PASSWORD
COPY tools/test-upstream/gen_ldif.py /gen_ldif.py
RUN test -n "$TEST_USER_PASSWORD" && TEST_USER_PASSWORD="$TEST_USER_PASSWORD" python /gen_ldif.py > /seed.ldif

FROM osixia/openldap:1.5.0
LABEL org.bambuflow.role=test-upstream
COPY --from=seed /seed.ldif /container/service/slapd/assets/config/bootstrap/ldif/custom/50-seed.ldif
