import pytest
from cryptography.hazmat.primitives.asymmetric import ec

from bambuflow_evaluation.golden import Asker
from bambuflow_evaluation.signer import Signer, serve_jwks
from bambuflow_platform.principal import (
    EVAL_ISSUER,
    JWKS,
    InvalidPrincipalError,
    Verifier,
)


def test_eval_identity_is_trusted_only_by_eval_replicas() -> None:
    signer = Signer(ec.generate_private_key(ec.SECP256R1()))
    server = serve_jwks(signer, "127.0.0.1:0")
    try:
        url = f"http://127.0.0.1:{server.server_address[1]}/internal/jwks.json"
        token = signer.sign(Asker("信贷 L2", "eval-credit", "credit", "credit", 2, ("credit-l2",)))
        # 评测副本：只信任 evaluation 签发方
        p = Verifier(JWKS(url), issuer=EVAL_ISSUER).verify(token)
        assert p.sub == "eval-credit"
        assert p.tenant == "credit"
        assert p.actor == "evaluation"
        # 线上实例：只信任 gateway，评测身份无效
        with pytest.raises(InvalidPrincipalError):
            Verifier(JWKS(url)).verify(token)
    finally:
        server.shutdown()
