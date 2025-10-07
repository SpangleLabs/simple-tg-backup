import base64
import datetime


def encode_json_extra(value: object) -> str:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode('ascii')
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    elif hasattr(value, "to_dict"):
        return value.to_dict()
    else:
        raise ValueError(f"Unrecognised type to encode: {value}")
