import ast
import datetime
from typing import Union

import pyparsing as pp
from pyparsing import ParseResults

StrReprValueType = Union[int, str, float, datetime.datetime, "StrReprObj"]


class StrReprObj:
    def __init__(
            self,
            class_name: str,
            values_dict: dict[str, StrReprValueType],
            key_order: list[str],
    ) -> None:
        self.class_name = class_name
        self.values_dict = values_dict
        self.key_order = key_order

    def has(self, key: str) -> bool:
        return key in self.values_dict

    def get(self, key: str) -> StrReprValueType:
        return self.values_dict[key]

    def __str__(self) -> str:
        return self.to_str()

    def to_str(self) -> str:
        result = f"{self.class_name}("
        kv_list = [
            f"{key}={StrReprObj.value_to_string(self.values_dict[key])}"
            for key in self.key_order
        ]
        result += ", ".join(kv_list)
        result += ")"
        return result

    @staticmethod
    def value_to_string(val: StrReprValueType) -> str:
        if val is None:
            return "None"
        if isinstance(val, StrReprObj):
            return val.to_str()
        elif isinstance(val, datetime.datetime):
            return repr(val)
        elif isinstance(val, str):
            if "'" in val:
                return f"\"{val}\""
            return f"'{val}'"
        elif isinstance(val, list):
            sub_items = [StrReprObj.value_to_string(v) for v in val]
            return "[" + ", ".join(sub_items) + "]"
        return str(val)

    def to_dict(self) -> dict:
        result = {
            "_": self.class_name,
        }
        for key, value in self.values_dict.items():
            if isinstance(value, StrReprObj):
                result[key] = value.to_dict()
            elif isinstance(value, datetime.datetime):
                result[key] = value.isoformat()
            elif isinstance(value, list):
                vals = []
                for item in value:
                    if isinstance(item, StrReprObj):
                        vals.append(item.to_dict())
                    elif isinstance(item, datetime.datetime):
                        vals.append(item.isoformat())
                    else:
                        vals.append(item)
                result[key] = vals
            else:
                result[key] = value
        return result

    @classmethod
    def parse_str_repr(cls, str_repr: str) -> "StrReprObj":
        parser = str_repr_parser()
        parsed = parser.parse_string(str_repr)
        return parsed[0]

    @classmethod
    def from_parsed_token(cls, parsed: pp.ParseResults) -> "StrReprObj":
        class_name = parsed["class_name"]
        if "key_value_list" in parsed:
            class_kv_list = parsed["key_value_list"][0]
        else:
            class_kv_list = []
        class_dict = {}
        key_order = []
        for kv_pair in class_kv_list:
            if kv_pair == ", ":
                continue
            key = kv_pair["key"]
            key_order.append(key)
            value = kv_pair["value"][0]
            if isinstance(value, ParseResults):
                if "class" in value:
                    value = value["class"]
                elif "list" in value:
                    value = value["list"]
            class_dict[key] = value
        return cls(class_name, class_dict, key_order)


def _parse_bytes(x: pp.ParseResults) -> bytes:
    bytes_str = x[0][0]
    if "'" in bytes_str and "\\'" not in bytes_str:
        bytes_str = "b\"" + bytes_str + "\""
    else:
        bytes_str = "b'" + bytes_str + "'"
    return ast.literal_eval(bytes_str)


def str_repr_parser() -> pp.ParserElement:
    val_int_expr = pp.common.signed_integer.set_name("value_integer")
    val_float_expr = pp.common.fnumber.set_name("value_float")
    val_none_expr = pp.Literal("None").set_parse_action(lambda x: [None]).set_name("value_none")
    val_bool_expr = pp.Or([pp.Literal("True"), pp.Literal("False")]).set_parse_action(lambda x: [x[0] == "True"]).set_name("value_bool")
    val_bytes_expr = pp.Group(pp.Literal("b").suppress() + pp.quoted_string).set_parse_action(_parse_bytes).set_name("value_bytes")
    val_str_expr = pp.quoted_string.set_parse_action(pp.remove_quotes).set_name("value_string")
    val_datetime_expr = pp.Group(
        pp.Suppress("datetime.datetime(")
        + pp.common.integer.set_results_name("year").set_parse_action(lambda x: int(x[0])) + ", "
        + pp.common.integer.set_results_name("month").set_parse_action(lambda x: int(x[0])) + ", "
        + pp.common.integer.set_results_name("day").set_parse_action(lambda x: int(x[0])) + ", "
        + pp.common.integer.set_results_name("hour").set_parse_action(lambda x: int(x[0])) + ", "
        + pp.common.integer.set_results_name("minute").set_parse_action(lambda x: int(x[0])) + ", "
        + pp.common.fnumber.set_results_name("second").set_parse_action(lambda x: int(x[0])) + ", "
        + pp.Suppress("tzinfo=datetime.timezone.utc)")
    ).set_parse_action(lambda x: datetime.datetime(x[0].year, x[0].month, x[0].day, x[0].hour, x[0].minute, x[0].second, tzinfo=datetime.timezone.utc)).set_name("value_datetime")

    class_expr = pp.Forward().set_results_name("class").set_parse_action(lambda x: StrReprObj.from_parsed_token(x[0]))
    val_class_expr = pp.Group(class_expr).set_name("value_class").set_parse_action(lambda x: x[0])
    list_expr = pp.Forward().set_results_name("list")
    val_list_expr = pp.Group(list_expr).set_parse_action(lambda x: x[0]).set_name("value_list")

    val_expr = pp.Or([val_int_expr, val_float_expr, val_none_expr, val_bool_expr, val_bytes_expr, val_str_expr, val_datetime_expr, val_class_expr, val_list_expr]).set_results_name("value")
    list_expr <<= pp.Group(pp.Suppress("[") + pp.Opt(val_expr + pp.ZeroOrMore(pp.Suppress(",") + val_expr)) + pp.Suppress("]")).set_parse_action(lambda x: x.as_list())

    key_expr = pp.Word(pp.alphanums + "_").set_results_name("key")
    key_val_expr = pp.Group(key_expr + pp.Suppress("=") + val_expr).set_results_name("key_value_pair", list_all_matches=True)

    key_val_list_expr = pp.Group(key_val_expr + pp.ZeroOrMore(pp.Suppress(", ") + key_val_expr)).set_results_name("key_value_list", list_all_matches=True)

    class_name_expr = pp.Word(pp.alphanums+".").set_results_name("class_name")
    class_expr <<= pp.Group(class_name_expr + pp.Suppress("(") + pp.Opt(key_val_list_expr) + pp.Suppress(")"))

    str_repr_expr = class_expr + pp.StringEnd()
    return str_repr_expr
