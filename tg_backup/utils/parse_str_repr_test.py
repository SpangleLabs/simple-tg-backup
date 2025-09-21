import datetime
from types import NoneType

import pytest

from tg_backup.utils.parse_str_repr import StrReprObj


def test_parse_obj_with_string() -> None:
    str_repr = "Class(val='string')"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "val" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["val"], str)
    assert str_obj.values_dict["val"] == "string"


def test_parse_obj_with_int() -> None:
    str_repr = "Class(val=123)"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "val" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["val"], int)
    assert str_obj.values_dict["val"] == 123


def test_parse_obj_with_none() -> None:
    str_repr = "Class(val=None)"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "val" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["val"], NoneType)
    assert str_obj.values_dict["val"] is None


def test_parse_obj_with_empty_list() -> None:
    str_repr = "Class(val=[])"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "val" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["val"], list)
    assert len(str_obj.values_dict["val"]) == 0
    assert str_obj.values_dict["val"] == []


def test_parse_obj_with_bytes() -> None:
    str_repr = r"Class(val=b'\xba\x06@\x02p&g\xba')"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "val" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["val"], bytes)
    assert str_obj.values_dict["val"] == b"\xba\x06@\x02p&g\xba"


def test_parse_obj_with_bytes_with_quote() -> None:
    str_repr = "Class(val=b\"\\xba\\x06@'\\x02p&g\\xba\")"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "val" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["val"], bytes)
    assert str_obj.values_dict["val"] == b"\xba\x06@'\x02p&g\xba"


def test_parse_obj_with_date() -> None:
    str_repr = "Class(date=datetime.datetime(2025, 9, 21, 23, 59, 12, tzinfo=datetime.timezone.utc))"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "date" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["date"], datetime.datetime)
    assert str_obj.values_dict["date"] == datetime.datetime(2025, 9, 21, 23, 59, 12, tzinfo=datetime.timezone.utc)


def test_parse_obj_with_date_without_seconds() -> None:
    str_repr = "Class(date=datetime.datetime(2025, 9, 21, 23, 59, tzinfo=datetime.timezone.utc))"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "date" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["date"], datetime.datetime)
    assert str_obj.values_dict["date"] == datetime.datetime(2025, 9, 21, 23, 59, 0, tzinfo=datetime.timezone.utc)


def test_parse_obj_with_list_of_stuff() -> None:
    str_repr = "Class(vals=[1, None, 'hey', [2], Nested(foo='bar')])"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "vals" in str_obj.values_dict
    assert len(str_obj.get("vals")) == 5
    assert str_obj.get("vals")[0] == 1
    assert str_obj.get("vals")[1] is None
    assert str_obj.get("vals")[2] == "hey"
    assert str_obj.get("vals")[3] == [2]
    nested = str_obj.get("vals")[4]
    assert isinstance(nested, StrReprObj)
    assert nested.class_name == "Nested"
    assert len(nested.values_dict) == 1
    assert nested.get("foo") == "bar"


def test_parse_obj_with_underscore_key() -> None:
    str_repr = "Class(key_name=\"string\")"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "key_name" in str_obj.values_dict
    assert isinstance(str_obj.values_dict["key_name"], str)
    assert str_obj.values_dict["key_name"] == "string"


def test_parse_obj_with_nested_class() -> None:
    str_repr = "Class(obj=Nested(key=\"string\"))"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 1
    assert "obj" in str_obj.values_dict
    assert isinstance(str_obj.get("obj"), StrReprObj)
    assert str_obj.get("obj").class_name == "Nested"
    assert len(str_obj.get("obj").values_dict) == 1
    assert "key" in str_obj.get("obj").values_dict
    assert str_obj.get("obj").get("key") == "string"


def test_parse_obj_with_list_of_nested_classes() -> None:
    str_repr = "Class(objs=[First(id=123), Second(id=456), Third(deeper=Deep(real='deep'))])"

    str_obj = StrReprObj.parse_str_repr(str_repr)
    str_obj_str = str_obj.to_str()

    assert str_obj_str == str_repr


def test_parse_obj_with_many_values() -> None:
    str_repr = "Class(str_val=\"string\", int_val=123, null_val=None, list_val=[])"

    str_obj = StrReprObj.parse_str_repr(str_repr)

    assert str_obj.class_name == "Class"
    assert len(str_obj.values_dict) == 4
    assert str_obj.values_dict["str_val"] == "string"
    assert str_obj.values_dict["int_val"] == 123
    assert str_obj.values_dict["null_val"] is None
    assert str_obj.values_dict["list_val"] == []


def test_parse_obj_with_many_values_to_string() -> None:
    str_repr = "Class(a_str_val='string', b_int_val=123, c_null_val=None, d_list_val=[])"

    str_obj = StrReprObj.parse_str_repr(str_repr)
    back_to_str = str_obj.to_str()

    assert back_to_str == str_repr


def test_parse_obj_ordered_keys_to_string() -> None:
    str_repr = "Class(d_str_val='string', b_int_val=123, c_null_val=None, a_list_val=[])"

    str_obj = StrReprObj.parse_str_repr(str_repr)
    back_to_str = str_obj.to_str()

    assert back_to_str == str_repr