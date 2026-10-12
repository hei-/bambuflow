"""生成测试用公司目录（OpenLDAP）的种子数据，只用于开发与 CI。

用户属性的含义（平台身份同步按部署配置做字段映射，见 gateway 详设 3.5）：
departmentNumber 部门、title 岗位、employeeType 公司密级等级、l 地域。
"""

import base64
import hashlib
import os
import sys

BASE = "dc=corp,dc=example"

# 测试账号统一口令，由构建参数传入（compose 中的 BF_TEST_USER_PASSWORD），只存在于这套测试目录里。
TEST_PASSWORD = os.environ["TEST_USER_PASSWORD"]

USERS = [
    # uid, 姓, 名, 部门, 岗位, 密级等级, 地域
    ("alice", "王", "丽", "信贷部", "客户经理", "C2", "华东"),
    ("bob", "李", "博", "风险管理部", "风险经理", "C3", "华北"),
    # 风险管理部的另一名员工，不在 risk 租户的试点群组里：试点阶段只能检索、不能问答
    ("ruth", "容", "露", "风险管理部", "风险分析员", "C2", "华北"),
    # credit-l2 的第二个成员：groupOfNames 至少要有一个成员，端到端测试移出 alice 时群组不能变空
    ("carl", "陈", "楷", "信贷部", "客户经理", "C2", "华东"),
    # 属于 250 个群组：验证 Principal 群组超过 200 个时只放引用（gateway 详设 3.2）
    ("eve", "赵", "伊", "运营部", "运营专员", "C1", "华南"),
    # 管理接口的测试账号（tools/test-upstream/admin-roles.yaml 把下面的群组映射成平台角色）：
    # 平台运维两人（双人复核）、信息安全、两个部门的数据属主、
    # 平台管理员两人（配置变更的作者与审批人）、审计岗
    ("olga", "欧", "佳", "平台部", "平台运维", "C2", "华东"),
    ("oscar", "欧", "森", "平台部", "平台运维", "C2", "华东"),
    ("ivan", "伊", "凡", "信息安全部", "信息安全工程师", "C2", "华东"),
    ("dana", "戴", "娜", "信贷部", "数据管理员", "C2", "华东"),
    ("rick", "任", "柯", "风险管理部", "数据管理员", "C3", "华北"),
    ("paul", "潘", "鹏", "平台部", "平台管理员", "C2", "华东"),
    ("pete", "裴", "涛", "平台部", "平台管理员", "C2", "华东"),
    ("audra", "敖", "拉", "审计部", "审计员", "C2", "华东"),
    # 信贷租户的负责人（tenants/credit/tenant.yaml 的 owner_groups）：
    # 只读本租户的配置、门禁与质量趋势，不是租户成员
    ("owen", "欧", "文", "信贷部", "业务负责人", "C2", "华东"),
    # 演示用的全权限账号：全部平台角色，又是 credit 租户的成员与负责人，一个账号能打开所有菜单。
    # 只在开发与演示的测试目录里有，生产不设这种账号（governance 详设 9.1 职责分离）；
    # 发起人不能审批自己的操作仍然生效，审批要换另一个账号
    ("super", "苏", "柏", "信贷部", "演示管理员", "C3", "华东"),
    # 运维值班（P3-I）：只属于 ops 租户。可疑指令、日志、数据库表结构三个测试数据源
    # 只对运维群组可见，评测集与权限评测集的提问身份都不在这些群组里，新增数据不影响它们的结果
    ("sam", "沈", "默", "运维部", "运维工程师", "C2", "华东"),
]

GROUPS = {
    "credit-l2": ["alice", "carl", "super"],
    "risk-dept": ["bob", "ruth"],
    # risk 租户（第二个业务系统，试点阶段）的试点群组：问答只对它开放（evaluation 详设 3.1）
    "risk-pilot": ["bob"],
    # credit 租户的审计岗（tenants/credit/tenant.yaml 的 auditor_groups）：
    # 回放用例以 carl 查看 alice 的回答
    "credit-audit": ["carl"],
    "all-staff": ["alice", "bob", "carl", "eve", "sam"],
    "sre-oncall": ["sam"],
    # 日志数据源里 core-batch 服务的归属组（P3-I3）：sam 不在里面；groupOfNames 至少要有一个成员
    "sre-core": ["olga"],
    # 平台角色对应的群组（admin-roles.yaml）；这些账号不属于任何租户，查不了数据
    "platform-ops": ["olga", "oscar", "super"],
    "infosec": ["ivan", "super"],
    "data-owners": ["dana", "rick", "super"],
    "platform-admins": ["paul", "pete", "super"],
    "platform-audit": ["audra", "super"],
    "credit-owners": ["owen", "super"],
    **{f"project-{i:03d}": ["eve"] for i in range(1, 251)},
}


def attr(name: str, value: str) -> str:
    # LDIF 里非 ASCII 的值必须 base64 编码
    if value.isascii():
        return f"{name}: {value}"
    return f"{name}:: {base64.b64encode(value.encode()).decode()}"


def ssha(password: str, salt: bytes) -> str:
    digest = hashlib.sha1(password.encode() + salt).digest()  # noqa: S324 LDAP 的 {SSHA} 格式规定用 SHA-1
    return "{SSHA}" + base64.b64encode(digest + salt).decode()


def main() -> None:
    out = [
        f"dn: ou=people,{BASE}",
        "objectClass: organizationalUnit",
        "ou: people",
        "",
        f"dn: ou=groups,{BASE}",
        "objectClass: organizationalUnit",
        "ou: groups",
        "",
    ]
    for uid, sn, given, dept, title, grade, region in USERS:
        out += [
            f"dn: uid={uid},ou=people,{BASE}",
            "objectClass: inetOrgPerson",
            f"uid: {uid}",
            attr("cn", sn + given),
            attr("sn", sn),
            attr("givenName", given),
            f"mail: {uid}@corp.example",
            attr("departmentNumber", dept),
            attr("title", title),
            f"employeeType: {grade}",
            attr("l", region),
            # 固定盐，使生成结果可重复
            f"userPassword: {ssha(TEST_PASSWORD, uid.encode().ljust(8, b'.'))}",
            "",
        ]
    for cn, members in GROUPS.items():
        out += [f"dn: cn={cn},ou=groups,{BASE}", "objectClass: groupOfNames", f"cn: {cn}"]
        out += [f"member: uid={m},ou=people,{BASE}" for m in members]
        out.append("")
    sys.stdout.write("\n".join(out))


if __name__ == "__main__":
    main()
