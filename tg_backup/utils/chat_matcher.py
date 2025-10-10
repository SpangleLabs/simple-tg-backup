import dataclasses
import enum
from abc import ABC, abstractmethod
from typing import Optional, Callable

import pyparsing as pp

from tg_backup.utils.dialog_type import DialogType


@dataclasses.dataclass
class ChatData:
    chat_id: int
    chat_type: DialogType
    username: Optional[str]
    title: Optional[str]
    member_count: Optional[int]

    def to_dict(self) -> dict:
        return {
            "chat_id": self.chat_id,
            "chat_type": self.chat_type.value,
            "username": self.username,
            "title": self.title,
            "member_count": self.member_count,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ChatData":
        return cls(
            data["chat_id"],
            DialogType.from_str(data["chat_type"]),
            data["username"],
            data["title"],
            data["member_count"],
        )


class DelimFunctor(enum.Enum):
    EQUAL = "equal"
    NOT_EQUAL = "not_equal"

    def compare_func(self) -> Callable[[any, any], bool]:
        return {
            DelimFunctor.EQUAL: lambda a, b: a == b,
            DelimFunctor.NOT_EQUAL: lambda a, b: a != b,
        }[self]

    def compare(self, chat_value: any, query_value: any) -> bool:
        return self.compare_func()(chat_value, query_value)


class FieldGetter(enum.Enum):
    CHAT_ID = enum.auto()
    CHAT_TYPE = enum.auto()
    USERNAME = enum.auto()
    TITLE = enum.auto()
    MEMBER_COUNT = enum.auto()

    def get_func(self) -> Callable[[ChatData], any]:
        return {
            FieldGetter.CHAT_ID: lambda chat_data: chat_data.chat_id,
            FieldGetter.CHAT_TYPE: lambda chat_data: chat_data.chat_type,
            FieldGetter.USERNAME: lambda chat_data: chat_data.username,
            FieldGetter.TITLE: lambda chat_data: chat_data.title,
            FieldGetter.MEMBER_COUNT: lambda chat_data: chat_data.member_count,
        }[self]

    def get_value(self, chat: ChatData) -> any:
        return self.get_func()(chat)


class ChatMatcher(ABC):

    @abstractmethod
    def matches_chat(self, chat: ChatData) -> bool:
        raise NotImplementedError()


class ChatFieldMatcher(ChatMatcher):
    def __init__(self, field_getter: FieldGetter, delim_func: DelimFunctor, compare_val: any) -> None:
        self.field_getter = field_getter
        self.delim_func = delim_func
        self.compare_val = compare_val
        self.match_query = lambda chat: delim_func.compare(field_getter.get_value(chat), compare_val)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(field={self.field_getter}, delim={self.delim_func}, value={self.compare_val})"

    def matches_chat(self, chat: ChatData) -> bool:
        return self.match_query(chat)


class AndMatcher(ChatMatcher):
    def __init__(self, first: ChatMatcher, second: ChatMatcher) -> None:
        self.sub_matchers = [first, second]
        if isinstance(first, AndMatcher):
            self.sub_matchers = [*first.sub_matchers, *self.sub_matchers[1:]]
        if isinstance(second, AndMatcher):
            self.sub_matchers = [*self.sub_matchers[-1:], *second.sub_matchers]

    def matches_chat(self, chat: ChatData) -> bool:
        return all(m.matches_chat(chat) for m in self.sub_matchers)


class OrMatcher(ChatMatcher):
    def __init__(self, first: ChatMatcher, second: ChatMatcher) -> None:
        self.sub_matchers = [first, second]
        if isinstance(first, OrMatcher):
            self.sub_matchers = [*first.sub_matchers, *self.sub_matchers[1:]]
        if isinstance(second, OrMatcher):
            self.sub_matchers = [*self.sub_matchers[-1:], *second.sub_matchers]

    def matches_chat(self, chat: ChatData) -> bool:
        return any(m.matches_chat(chat) for m in self.sub_matchers)


class NotMatcher(ChatMatcher):
    def __init__(self, sub_matcher: ChatMatcher) -> None:
        self.sub_matcher = sub_matcher

    def matches_chat(self, chat: ChatData) -> bool:
        return not self.sub_matcher.matches_chat(chat)


def parse_query(query: str) -> ChatMatcher:
    pass

def matcher_parser() -> pp.ParserElement:
    # Parsers for types of values
    val_int_expr = pp.common.signed_integer.set_name("value_integer").set_results_name("value")
    val_quoted_str_expr = pp.quoted_string.set_parse_action(pp.remove_quotes).set_name("value_quoted_str").set_results_name("value")
    val_unquoted_word_expr = pp.Word(pp.printables).set_name("value_unquoted_word").set_results_name("value")
    val_str_expr = pp.MatchFirst([val_quoted_str_expr, val_unquoted_word_expr]).set_name("value_str").set_results_name("value")
    val_any_expr = pp.MatchFirst([val_int_expr, val_str_expr]).set_name("value_any").set_results_name("value")

    # key/value delimiter expression
    key_val_eq_expr = pp.MatchFirst([pp.Literal("="), pp.Literal(":")]).set_parse_action(lambda _: DelimFunctor.EQUAL)
    key_val_neq_expr = pp.MatchFirst([pp.Literal("!="), pp.Literal("!:")]).set_parse_action(lambda _: DelimFunctor.NOT_EQUAL)
    key_val_delim_expr = pp.MatchFirst([key_val_neq_expr, key_val_eq_expr]).set_results_name("key_val_delim")

    # Parse specifying the chat ID
    chat_id_key_expr = pp.MatchFirst([pp.CaselessLiteral("chat_id"), pp.CaselessLiteral("id")]).set_parse_action(lambda _: FieldGetter.CHAT_ID).set_results_name("key_expr")
    chat_id_expr = pp.Group(chat_id_key_expr + key_val_delim_expr + val_any_expr).set_parse_action(lambda x: ChatFieldMatcher(x[0].key_expr, x[0].key_val_delim, x[0].value))

    # Parse specifying chat type
    chat_type_key_expr = pp.MatchFirst([pp.CaselessLiteral("chat_type"), pp.CaselessLiteral("type")]).set_parse_action(lambda _: FieldGetter.CHAT_TYPE).set_results_name("key_expr")
    val_chat_type_expr = pp.MatchFirst([pp.CaselessLiteral("user"), pp.CaselessLiteral("group"), pp.CaselessLiteral("channel")]).set_name("value_chat_type").set_results_name("value").set_parse_action(lambda x: DialogType.from_str(x[0].upper()))
    chat_type_expr = pp.Group(chat_type_key_expr + key_val_delim_expr + val_chat_type_expr).set_parse_action(lambda x: ChatFieldMatcher(x[0].key_expr, x[0].key_val_delim, x[0].value))

    # Parse specifying username
    chat_username_key_expr = pp.CaselessLiteral("username").set_parse_action(lambda _: FieldGetter.USERNAME).set_results_name("key_expr")
    chat_username_expr = pp.Group(chat_username_key_expr + key_val_delim_expr + val_str_expr).set_parse_action(lambda x: ChatFieldMatcher(x[0].key_expr, x[0].key_val_delim, x[0].value))

    # Parse specifying title
    chat_title_key_expr = pp.MatchFirst([pp.CaselessLiteral("title"), pp.CaselessLiteral("name")]).set_parse_action(lambda _: FieldGetter.TITLE).set_results_name("key_expr")
    chat_title_expr = pp.Group(chat_title_key_expr + key_val_delim_expr + val_str_expr).set_parse_action(lambda x: ChatFieldMatcher(x[0].key_expr, x[0].key_val_delim, x[0].value))

    # Parse specifying member count
    chat_member_count_key_expr = pp.MatchFirst([pp.CaselessLiteral("member_count"), pp.CaselessLiteral("user_count"), pp.CaselessLiteral("users"), pp.CaselessLiteral("size")]).set_parse_action(lambda _: FieldGetter.MEMBER_COUNT).set_results_name("key_expr")
    chat_member_count_expr = pp.Group(chat_member_count_key_expr + key_val_delim_expr + val_int_expr).set_parse_action(lambda x: ChatFieldMatcher(x[0].key_expr, x[0].key_val_delim, x[0].value))

    # Parser for all field matchers
    field_matcher_expr = pp.MatchFirst([chat_id_expr, chat_type_expr, chat_username_expr, chat_title_expr, chat_member_count_expr]).set_parse_action(lambda x: x[0])

    # Setup a forward for the parser for sub-matcher expressions
    matcher_expr = pp.Forward()

    # "NOT" parser
    not_prefix_expr = pp.MatchFirst([pp.CaselessKeyword("not"), pp.Literal("!")]).suppress()
    not_matcher_expr = pp.Group(not_prefix_expr + matcher_expr.set_results_name("sub_expr")).set_parse_action(lambda x: NotMatcher(x[0].sub_expr))

    # "AND" parser
    and_delim_expr = pp.MatchFirst([pp.CaselessKeyword("and"), pp.Literal("&&"), pp.Literal("&")]).set_parse_action(lambda _: AndMatcher).set_results_name("combine_delim")
    # and_matcher_expr = pp.Group(matcher_expr.set_results_name("first_expr") + and_delim_expr + matcher_expr.set_results_name("second_expr")).set_parse_action(lambda x: AndMatcher(x[0].first_expr, x[0].second_expr))

    # "OR" parser
    or_delim_expr = pp.MatchFirst([pp.CaselessKeyword("or"), pp.Literal("|"), pp.Literal("||")]).set_parse_action(lambda _: OrMatcher).set_results_name("combine_delim")
    # or_matcher_expr = pp.Group(matcher_expr + or_delim_expr + matcher_expr).set_parse_action(lambda x: OrMatcher(x[0], x[1]))

    combine_delim_expr = pp.MatchFirst([and_delim_expr, or_delim_expr]).set_parse_action(lambda x: x[0])

    # brackets expression
    brackets_expr = pp.Group(pp.Literal("(").suppress() + matcher_expr + pp.Literal(")").suppress()).set_parse_action(lambda x: x[0])

    # Define the parser for sub-matcher expressions
    elem_expr = pp.MatchFirst([brackets_expr, field_matcher_expr]).set_parse_action(lambda x: x[0])
    notable_elem_expr = pp.MatchFirst([not_matcher_expr, elem_expr]).set_parse_action(lambda x: x[0])

    def parse_combined(results: pp.ParseResults) -> ChatMatcher:
        results_list = results.as_list()[0]
        matcher = results_list.pop(0)
        while results_list:
            combine_delim = results_list.pop(0)
            next_matcher = results_list.pop(0)
            matcher = combine_delim(matcher, next_matcher)
        return matcher

    matcher_expr <<= pp.Group(notable_elem_expr + pp.ZeroOrMore(combine_delim_expr + notable_elem_expr)).set_parse_action(parse_combined)

    return pp.Group(matcher_expr + pp.StringEnd().suppress()).set_parse_action(lambda x: x[0])
