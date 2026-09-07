import hashlib
import hmac
import json
import secrets
import time
from typing import Any


def generate_secret_key(length: int = 32) -> str:
    return secrets.token_hex(length)


def canonical_json(payload: dict[str, Any]) -> str:
    """Deterministic JSON serialization used for both signing and the outbound body.

    Signing and the transmitted bytes must be computed from the identical
    string, otherwise the receiver's signature verification would fail due to
    key-ordering / whitespace differences introduced by re-serialization.
    """
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def compute_signature(raw_json_string: str, secret: str, timestamp: int) -> str:
    canonical_payload = f"{timestamp}.{raw_json_string}"
    return hmac.new(
        key=secret.encode("utf-8"),
        msg=canonical_payload.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()


def generate_webhook_headers(payload: dict[str, Any], secret: str) -> dict[str, str]:
    raw_json_string = canonical_json(payload)
    timestamp = int(time.time())
    signature = compute_signature(raw_json_string, secret, timestamp)
    return {
        "Content-Type": "application/json",
        "X-Webhook-Timestamp": str(timestamp),
        "X-Webhook-Signature": f"t={timestamp},v1={signature}",
    }


def verify_signature(
    raw_json_string: str,
    secret: str,
    timestamp: str,
    signature: str,
    tolerance_seconds: int = 300,
) -> bool:
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return False

    if abs(time.time() - ts) > tolerance_seconds:
        return False

    expected = compute_signature(raw_json_string, secret, ts)
    return hmac.compare_digest(expected, signature)
