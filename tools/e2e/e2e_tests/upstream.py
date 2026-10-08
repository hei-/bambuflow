"""读取测试上游（tools/test-upstream）里的样例生成器：干扰页面与共享盘样例。

它们不是 Python 包，按文件路径加载。
"""

import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Protocol, cast

UPSTREAM = Path(__file__).resolve().parents[2] / "test-upstream"
FILESHARE_DIR = UPSTREAM / "fileshare"
CLEARANCE = {"public": 1, "internal": 2, "confidential": 3, "restricted": 4}


class Page(Protocol):
    id: str
    markdown: str


class FileDoc(Protocol):
    file: str
    id: str
    classification: str
    acl: tuple[str, ...]
    kind: str


def _load(name: str) -> ModuleType:
    path = UPSTREAM / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(str(path))
    mod = importlib.util.module_from_spec(spec)
    # dataclass 定义时要从 sys.modules 找到所在模块，按路径加载时须先登记
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def distractors() -> list[Page]:
    return cast(list[Page], _load("gen_distractors").pages())


def fileshare_docs() -> list[FileDoc]:
    """共享盘样例（tools/test-upstream/gen_fileshare.py 的 DOCS）：文件名、doc_id、ACL 与密级。"""
    return list(cast(tuple[FileDoc, ...], _load("gen_fileshare").DOCS))


def visible_fileshare(tokens: set[str], clearance: int) -> set[str]:
    """与检索的权限规则相同：ACL 与调用方的 token 有交集，且密级不超过调用方。"""
    return {
        d.id
        for d in fileshare_docs()
        if set(d.acl) & tokens and CLEARANCE[d.classification] <= clearance
    }


def _field(markdown: str, name: str) -> str:
    m = re.search(rf"^{name}: (.*)$", markdown, re.MULTILINE)
    if m is None:
        raise ValueError(f"干扰页面缺少 {name}")
    return m.group(1)


def visible_distractors(tokens: set[str], clearance: int) -> set[str]:
    """按页面的 acl 与密级判定可见的干扰页面：token 有交集且密级不超（与检索的权限规则相同）。"""
    out = set()
    for p in distractors():
        acl = set(re.findall(r"[a-z]+:[\w-]+", _field(p.markdown, "acl")))
        if acl & tokens and CLEARANCE[_field(p.markdown, "classification")] <= clearance:
            out.add(p.id)
    return out
