import json

import httpx
import pytest
from bambuflow_processing import pii

ID = "110101199003077777"
PHONE = "13812345678"


def test_mask_keeps_length_and_positions() -> None:
    text = (
        f"客户 {ID}，手机 {PHONE}，邮箱 zhang.san@corp.example，"
        "卡号 4111111111111111，流水号 1234567890123456"
    )
    masked, found = pii.mask(text)
    assert len(masked) == len(text)
    assert "110101********7777" in masked and "138****5678" in masked
    assert "z********@corp.example" in masked and "4111********1111" in masked
    # 不过 Luhn 校验的长数字不是卡号
    assert "1234567890123456" in masked
    for f in found:
        assert text[f.start : f.end] == f.original and masked[f.start : f.end] == f.masked


class FakeKeys:
    key_id = "bambuflow-pii"

    def __init__(self) -> None:
        self.calls = 0

    def data_key(self) -> tuple[bytes, str]:
        self.calls += 1
        return b"k" * 32, "vault:v1:fake"


def test_seal_roundtrip_and_one_key_per_document() -> None:
    keys = FakeKeys()
    doc = pii.DocumentKey(keys)
    masked, found = pii.mask(f"手机 {PHONE}")
    a = doc.seal("c1", found)
    b = doc.seal("c2", found)
    assert keys.calls == 1 and a.wrapped_key == "vault:v1:fake" and a.key_id == "bambuflow-pii"
    assert a.ciphertext != b.ciphertext
    spans = pii.unseal(b"k" * 32, "c1", a.ciphertext)
    assert spans == [(3, 14, PHONE)]
    assert PHONE not in json.dumps(masked)


class FakeNer:
    def __init__(self, entities: list[pii.Entity]) -> None:
        self.found = entities

    def entities(self, text: str) -> list[pii.Entity]:
        return self.found


def test_ner_entities_are_masked_after_rules() -> None:
    text = "客户张三丰，住上海市浦东新区，手机 13812345678。"
    ner = FakeNer(
        [
            pii.Entity(2, 5, "person"),
            pii.Entity(7, 14, "address"),
            # 与规则识别的手机号重叠：不重复掩码
            pii.Entity(18, 29, "person"),
        ]
    )
    masked, found = pii.mask(text, ner)
    assert masked == "客户张**，住上******，手机 138****5678。"
    assert len(masked) == len(text) and [f.original for f in found] == [
        "张三丰",
        "上海市浦东新区",
        "13812345678",
    ]


def test_http_ner_rejects_out_of_range_spans() -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"entities": [{"start": 0, "end": 99, "label": "person"}]})

    ner = pii.HttpNerModel("http://ner", httpx.Client(transport=httpx.MockTransport(handler)))
    with pytest.raises(pii.NerError):
        ner.entities("张三")

    def ok(req: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "entities": [
                    {"start": 0, "end": 2, "label": "person"},
                    {"start": 0, "end": 1, "label": "org"},
                ]
            },
        )

    ner = pii.HttpNerModel("http://ner", httpx.Client(transport=httpx.MockTransport(ok)))
    assert ner.entities("张三") == [pii.Entity(0, 2, "person")]
