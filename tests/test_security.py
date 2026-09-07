import hashlib
import hmac

from app.security import canonical_json, compute_signature, generate_webhook_headers, verify_signature


def test_compute_signature_matches_fixed_vector():
    raw = '{"amount":100,"currency":"USD"}'
    secret = "super-secret"
    timestamp = 1_700_000_000

    expected = hmac.new(
        key=secret.encode("utf-8"),
        msg=f"{timestamp}.{raw}".encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    assert compute_signature(raw, secret, timestamp) == expected


def test_canonical_json_is_deterministic_regardless_of_key_order():
    a = canonical_json({"b": 2, "a": 1})
    b = canonical_json({"a": 1, "b": 2})
    assert a == b == '{"a":1,"b":2}'


def test_generate_webhook_headers_contains_expected_fields():
    headers = generate_webhook_headers({"x": 1}, "secret")
    assert headers["Content-Type"] == "application/json"
    assert "X-Webhook-Timestamp" in headers
    signature_header = headers["X-Webhook-Signature"]
    assert signature_header.startswith("t=")
    assert ",v1=" in signature_header


def test_verify_signature_accepts_valid_signature():
    payload = {"order_id": "abc123"}
    secret = "whsec_test"
    headers = generate_webhook_headers(payload, secret)
    timestamp = headers["X-Webhook-Timestamp"]
    signature = headers["X-Webhook-Signature"].split(",v1=")[1]

    assert verify_signature(canonical_json(payload), secret, timestamp, signature) is True


def test_verify_signature_rejects_tampered_payload():
    payload = {"order_id": "abc123"}
    secret = "whsec_test"
    headers = generate_webhook_headers(payload, secret)
    timestamp = headers["X-Webhook-Timestamp"]
    signature = headers["X-Webhook-Signature"].split(",v1=")[1]

    tampered_payload = canonical_json({"order_id": "tampered"})
    assert verify_signature(tampered_payload, secret, timestamp, signature) is False


def test_verify_signature_rejects_wrong_secret():
    payload = {"order_id": "abc123"}
    headers = generate_webhook_headers(payload, "correct-secret")
    timestamp = headers["X-Webhook-Timestamp"]
    signature = headers["X-Webhook-Signature"].split(",v1=")[1]

    assert (
        verify_signature(canonical_json(payload), "wrong-secret", timestamp, signature) is False
    )


def test_verify_signature_rejects_expired_timestamp():
    payload = {"order_id": "abc123"}
    secret = "whsec_test"
    raw = canonical_json(payload)
    old_timestamp = 1_000_000_000  # far in the past
    signature = compute_signature(raw, secret, old_timestamp)

    assert verify_signature(raw, secret, str(old_timestamp), signature) is False
