import pytest
from charset_normalizer.utils import is_arabic_isolated_form

from tg_backup.utils.chat_matcher import matcher_parser, ChatFieldMatcher, FieldGetter, DelimFunctor, ChatType, \
    NotMatcher, AndMatcher, OrMatcher


def test_parse_chat_id() -> None:
    match_str = "chat_id=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == 123

def test_parse_chat_id_short() -> None:
    match_str = "id=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == 123

def test_parse_chat_id_spaces() -> None:
    match_str = "id = 123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == 123

def test_parse_chat_id_uppercase_key() -> None:
    match_str = "CHAT_ID=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == 123

def test_parse_chat_id_negative() -> None:
    match_str = "id=-123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == -123

def test_parse_chat_id_not_equal() -> None:
    match_str = "id!=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.NOT_EQUAL
    assert matcher.compare_val == 123

def test_parse_chat_type() -> None:
    match_str = "chat_type=USER"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_TYPE
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == ChatType.USER

def test_parse_chat_type_short() -> None:
    match_str = "type=USER"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_TYPE
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == ChatType.USER

def test_parse_chat_type_lowercase() -> None:
    match_str = "chat_type=user"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_TYPE
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == ChatType.USER

def test_parse_chat_type_group() -> None:
    match_str = "chat_type=GROUP"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_TYPE
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == ChatType.GROUP


def test_parse_chat_type_channel() -> None:
    match_str = "chat_type=CHANNEL"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_TYPE
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == ChatType.CHANNEL


def test_parse_username() -> None:
    match_str = "username=durov"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.USERNAME
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == "durov"


def test_parse_title() -> None:
    match_str = "title=Telegram"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.TITLE
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == "Telegram"


def test_parse_title_short() -> None:
    match_str = "name=Durov"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.TITLE
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == "Durov"


def test_parse_not_chat_id() -> None:
    match_str = "not id=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, NotMatcher)
    assert isinstance(matcher.sub_matcher, ChatFieldMatcher)
    assert matcher.sub_matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.sub_matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.sub_matcher.compare_val == 123

def test_parse_not_chat_id_short() -> None:
    match_str = "!id=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, NotMatcher)
    assert isinstance(matcher.sub_matcher, ChatFieldMatcher)
    assert matcher.sub_matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.sub_matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.sub_matcher.compare_val == 123

def test_parse_and() -> None:
    match_str = "chat_type=USER and title=Durov"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, AndMatcher)
    assert len(matcher.sub_matchers) == 2
    first, second = matcher.sub_matchers
    assert isinstance(first, ChatFieldMatcher)
    assert first.field_getter == FieldGetter.CHAT_TYPE
    assert first.delim_func == DelimFunctor.EQUAL
    assert first.compare_val == ChatType.USER
    assert isinstance(second, ChatFieldMatcher)
    assert second.field_getter == FieldGetter.TITLE
    assert second.delim_func == DelimFunctor.EQUAL
    assert second.compare_val == "Durov"


# noinspection DuplicatedCode
def test_parse_and_3() -> None:
    match_str = "chat_type=USER and title=Durov and id=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, AndMatcher)
    assert len(matcher.sub_matchers) == 3
    first, second, third = matcher.sub_matchers
    assert isinstance(first, ChatFieldMatcher)
    assert first.field_getter == FieldGetter.CHAT_TYPE
    assert first.delim_func == DelimFunctor.EQUAL
    assert first.compare_val == ChatType.USER
    assert isinstance(second, ChatFieldMatcher)
    assert second.field_getter == FieldGetter.TITLE
    assert second.delim_func == DelimFunctor.EQUAL
    assert second.compare_val == "Durov"
    assert isinstance(third, ChatFieldMatcher)
    assert third.field_getter == FieldGetter.CHAT_ID
    assert third.delim_func == DelimFunctor.EQUAL
    assert third.compare_val == 123


def test_parse_and_not() -> None:
    match_str = "chat_type=USER and not title=Durov"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, AndMatcher)
    assert len(matcher.sub_matchers) == 2
    first, second = matcher.sub_matchers
    assert isinstance(first, ChatFieldMatcher)
    assert first.field_getter == FieldGetter.CHAT_TYPE
    assert first.delim_func == DelimFunctor.EQUAL
    assert first.compare_val == ChatType.USER
    assert isinstance(second, NotMatcher)
    assert isinstance(second.sub_matcher, ChatFieldMatcher)
    assert second.sub_matcher.field_getter == FieldGetter.TITLE
    assert second.sub_matcher.delim_func == DelimFunctor.EQUAL
    assert second.sub_matcher.compare_val == "Durov"


def test_parse_or() -> None:
    match_str = "chat_type=USER or title=Durov"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, OrMatcher)
    assert len(matcher.sub_matchers) == 2
    first, second = matcher.sub_matchers
    assert isinstance(first, ChatFieldMatcher)
    assert first.field_getter == FieldGetter.CHAT_TYPE
    assert first.delim_func == DelimFunctor.EQUAL
    assert first.compare_val == ChatType.USER
    assert isinstance(second, ChatFieldMatcher)
    assert second.field_getter == FieldGetter.TITLE
    assert second.delim_func == DelimFunctor.EQUAL
    assert second.compare_val == "Durov"

# noinspection DuplicatedCode
def test_parse_or_3() -> None:
    match_str = "chat_type=USER or title=Durov or id=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, OrMatcher)
    assert len(matcher.sub_matchers) == 3
    first, second, third = matcher.sub_matchers
    assert isinstance(first, ChatFieldMatcher)
    assert first.field_getter == FieldGetter.CHAT_TYPE
    assert first.delim_func == DelimFunctor.EQUAL
    assert first.compare_val == ChatType.USER
    assert isinstance(second, ChatFieldMatcher)
    assert second.field_getter == FieldGetter.TITLE
    assert second.delim_func == DelimFunctor.EQUAL
    assert second.compare_val == "Durov"
    assert isinstance(third, ChatFieldMatcher)
    assert third.field_getter == FieldGetter.CHAT_ID
    assert third.delim_func == DelimFunctor.EQUAL
    assert third.compare_val == 123


# noinspection DuplicatedCode
def test_parse_and_or() -> None:
    match_str = "chat_type=USER and title=Durov or id=123"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, OrMatcher)
    assert len(matcher.sub_matchers) == 2
    and_match, third = matcher.sub_matchers
    assert isinstance(and_match, AndMatcher)
    first, second = and_match.sub_matchers
    assert first.field_getter == FieldGetter.CHAT_TYPE
    assert first.delim_func == DelimFunctor.EQUAL
    assert first.compare_val == ChatType.USER
    assert isinstance(second, ChatFieldMatcher)
    assert second.field_getter == FieldGetter.TITLE
    assert second.delim_func == DelimFunctor.EQUAL
    assert second.compare_val == "Durov"
    assert isinstance(third, ChatFieldMatcher)
    assert third.field_getter == FieldGetter.CHAT_ID
    assert third.delim_func == DelimFunctor.EQUAL
    assert third.compare_val == 123


def test_parse_brackets() -> None:
    match_str = "(chat_id=123)"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == 123

def test_parse_nested_brackets() -> None:
    match_str = "((((chat_id=123))))"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, ChatFieldMatcher)
    assert matcher.field_getter == FieldGetter.CHAT_ID
    assert matcher.delim_func == DelimFunctor.EQUAL
    assert matcher.compare_val == 123

def test_parse_not_brackets() -> None:
    match_str = "not (chat_type=USER and chat_id=123)"
    parser = matcher_parser()

    matcher = parser.parse_string(match_str)[0]

    assert isinstance(matcher, NotMatcher)
    assert isinstance(matcher.sub_matcher, AndMatcher)
    assert len(matcher.sub_matcher.sub_matchers) == 2
    first, second = matcher.sub_matcher.sub_matchers
    assert isinstance(first, ChatFieldMatcher)
    assert first.field_getter == FieldGetter.CHAT_TYPE
    assert first.delim_func == DelimFunctor.EQUAL
    assert first.compare_val == ChatType.USER
    assert isinstance(second, ChatFieldMatcher)
    assert second.field_getter == FieldGetter.CHAT_ID
    assert second.delim_func == DelimFunctor.EQUAL
    assert second.compare_val == 123
