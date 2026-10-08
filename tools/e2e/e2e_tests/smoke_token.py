"""输出测试用户 alice 的访问令牌，供 make helm-smoke 经 gateway 问答（只在开发环境用）。"""

from .test_gateway import token

if __name__ == "__main__":
    print(token("alice"))
