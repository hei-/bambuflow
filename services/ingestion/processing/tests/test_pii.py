import json

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
