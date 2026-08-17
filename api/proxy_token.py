"""
Encrypts proxy payloads (upstream url + optional referer) into an opaque
token so the real upstream URL is never exposed in query strings /
network tab / shared links / server logs.

This mirrors the anime-api project's `utils/signedProxy.ts` exactly, so
tokens follow the same format and security properties:

Token format: base64url(iv) + "." + base64url(authTag) + "." +
base64url(ciphertext), where ciphertext = AES-256-GCM(JSON(payload)).

AES-256-GCM is authenticated encryption — it gives BOTH confidentiality
(the payload, including the URL, is unreadable without the key) AND
integrity (the auth tag detects any tampering) in one step; a failed/absent
auth tag on decrypt IS the "invalid/forged token" case, no separate
signature check needed.

Set PROXY_SIGNING_SECRET in the environment — it's fed through scrypt to
derive a 32-byte AES-256 key (so the raw env var doesn't have to be an
exact-length hex/base64 key itself). Falls back to a dev-only default so
local/dev setups keep working without extra config — always set a real
secret in production, since anyone with the fallback secret could both
decrypt and forge tokens.
"""

import base64
import json
import os
import time
from typing import Optional, TypedDict

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt

SECRET = os.environ.get("PROXY_SIGNING_SECRET", "dev-only-insecure-secret-change-me")
DEFAULT_TTL_SECONDS = int(os.environ.get("PROXY_TOKEN_TTL_SECONDS", "21600"))  # 6h

IV_LENGTH = 12  # 96-bit IV, the recommended/standard size for GCM
TAG_LENGTH = 16  # GCM auth tag is always 16 bytes
KEY_SALT = b"clixarena-proxy-token-v1"  # fixed salt: same derivation as the TS side


class ProxyPayload(TypedDict, total=False):
    url: str
    ref: Optional[str]
    exp: int  # unix seconds


# Derived once per process and memoized — scrypt is deliberately slow, so
# paying that cost on every token issued/verified would be a needless
# bottleneck on a hot proxy path. Only depends on SECRET, static per process.
_cached_key: Optional[bytes] = None


def _get_key() -> bytes:
    global _cached_key
    if _cached_key is None:
        # Matches Node's crypto.scryptSync(SECRET, KEY_SALT, 32) defaults:
        # N=16384, r=8, p=1.
        kdf = Scrypt(salt=KEY_SALT, length=32, n=16384, r=8, p=1)
        _cached_key = kdf.derive(SECRET.encode("utf-8"))
    return _cached_key


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def create_proxy_token(url: str, ref: Optional[str] = None, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> str:
    payload: ProxyPayload = {"url": url, "exp": int(time.time()) + ttl_seconds}
    if ref:
        payload["ref"] = ref
    plaintext = json.dumps(payload, separators=(",", ":")).encode("utf-8")

    iv = os.urandom(IV_LENGTH)
    aesgcm = AESGCM(_get_key())
    # cryptography's AESGCM appends the 16-byte auth tag to the ciphertext.
    ct_with_tag = aesgcm.encrypt(iv, plaintext, None)
    ciphertext, auth_tag = ct_with_tag[:-TAG_LENGTH], ct_with_tag[-TAG_LENGTH:]

    return f"{_b64url_encode(iv)}.{_b64url_encode(auth_tag)}.{_b64url_encode(ciphertext)}"


def verify_proxy_token(token: str) -> Optional[dict]:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    iv_part, tag_part, ciphertext_part = parts

    try:
        iv = _b64url_decode(iv_part)
        auth_tag = _b64url_decode(tag_part)
        ciphertext = _b64url_decode(ciphertext_part)
    except Exception:
        return None  # malformed base64url — not a token we issued

    if len(iv) != IV_LENGTH or len(auth_tag) != TAG_LENGTH:
        return None

    try:
        aesgcm = AESGCM(_get_key())
        # GCM auth-tag verification happens inside decrypt(); it raises if the
        # ciphertext or tag was tampered with, forged, or encrypted under a
        # different key.
        plaintext = aesgcm.decrypt(iv, ciphertext + auth_tag, None)
        payload = json.loads(plaintext.decode("utf-8"))
    except Exception:
        return None  # bad key/tag/ciphertext — tampered, forged, or expired-format token

    if not payload.get("url") or not isinstance(payload["url"], str):
        return None
    if not payload.get("exp") or int(time.time()) > payload["exp"]:
        return None  # expired

    return {"url": payload["url"], "ref": payload.get("ref")}
