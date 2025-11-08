import base64
import datetime
import json
from typing import Optional


def encode_json_extra(value: object) -> str:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode('ascii')
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif hasattr(value, "to_dict"):
        return value.to_dict()
    else:
        raise ValueError(f"Unrecognised type to encode: {value}")


def encode_json(value: object) -> str:
    return json.dumps(value, default=encode_json_extra)


def encode_optional_json(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    return encode_json(value)


def decode_json(data: str) -> object:
    return json.loads(data)


def decode_json_dict(data: str) -> dict:
    result = decode_json(data)
    if not isinstance(result, dict):
        raise ValueError(f"Expected dictionary, but decoded {type(result)}")
    return result


def decode_optional_json(data: Optional[str]) -> Optional[object]:
    if data is None:
        return None
    return decode_json(data)
