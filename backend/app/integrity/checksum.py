import hashlib
import hmac
from typing import Union


def compute_sha256(data: Union[bytes, bytearray, memoryview]) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def verify_checksum(calculated: str, expected: str) -> bool:
    if not calculated or not expected:
        return False
    return hmac.compare_digest(calculated.strip().lower(), expected.strip().lower())
