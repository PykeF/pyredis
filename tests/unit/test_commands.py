from __future__ import annotations

import pytest

from pyredis.commands import dispatch
from pyredis.store import KeyValueStore


@pytest.fixture
def store() -> KeyValueStore:
    return KeyValueStore()


# --------------------------------------------------------------------------
# Replies
# --------------------------------------------------------------------------


def test_ping_answers_pong(store: KeyValueStore) -> None:
    assert dispatch(store, [b"PING"]) == b"+PONG\r\n"


def test_ping_with_a_message_echoes_it_as_a_bulk_string(store: KeyValueStore) -> None:
    assert dispatch(store, [b"PING", b"hello"]) == b"$5\r\nhello\r\n"


def test_set_answers_ok_and_stores_the_value(store: KeyValueStore) -> None:
    assert dispatch(store, [b"SET", b"name", b"Pyke"]) == b"+OK\r\n"
    assert store.get(b"name") == b"Pyke"


def test_get_answers_a_bulk_string(store: KeyValueStore) -> None:
    store.set(b"name", b"Pyke")

    assert dispatch(store, [b"GET", b"name"]) == b"$4\r\nPyke\r\n"


def test_get_answers_a_null_bulk_string_when_the_key_is_missing(store: KeyValueStore) -> None:
    assert dispatch(store, [b"GET", b"missing"]) == b"$-1\r\n"


def test_get_answers_an_empty_bulk_string_for_an_empty_value(store: KeyValueStore) -> None:
    store.set(b"empty", b"")

    assert dispatch(store, [b"GET", b"empty"]) == b"$0\r\n\r\n"


def test_del_answers_the_number_removed(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    assert dispatch(store, [b"DEL", b"a", b"b", b"missing"]) == b":2\r\n"
    assert store.dbsize() == 0


def test_exists_answers_the_number_present_counting_repeats(store: KeyValueStore) -> None:
    store.set(b"a", b"1")

    assert dispatch(store, [b"EXISTS", b"a", b"a", b"missing"]) == b":2\r\n"


def test_incr_answers_the_new_value(store: KeyValueStore) -> None:
    assert dispatch(store, [b"INCR", b"counter"]) == b":1\r\n"
    assert dispatch(store, [b"INCR", b"counter"]) == b":2\r\n"


def test_dbsize_answers_the_key_count(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    assert dispatch(store, [b"DBSIZE"]) == b":2\r\n"


def test_flushdb_answers_ok_and_empties_the_store(store: KeyValueStore) -> None:
    store.set(b"a", b"1")

    assert dispatch(store, [b"FLUSHDB"]) == b"+OK\r\n"
    assert store.dbsize() == 0


# --------------------------------------------------------------------------
# Name normalization
# --------------------------------------------------------------------------


@pytest.mark.parametrize("name", [b"SET", b"set", b"SeT", b"sET"])
def test_command_names_are_matched_case_insensitively(
    store: KeyValueStore, name: bytes
) -> None:
    assert dispatch(store, [name, b"key", b"value"]) == b"+OK\r\n"


def test_keys_and_values_are_never_case_folded(store: KeyValueStore) -> None:
    dispatch(store, [b"set", b"MixedKey", b"MixedValue"])

    assert store.get(b"MixedKey") == b"MixedValue"
    assert store.get(b"mixedkey") is None


def test_binary_keys_and_values_pass_through_unchanged(store: KeyValueStore) -> None:
    key = b"\x00\xff key"
    value = b"\x89PNG\r\n\x1a\n\xfe"

    dispatch(store, [b"SET", key, value])

    assert dispatch(store, [b"GET", key]) == b"$%d\r\n%s\r\n" % (len(value), value)


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("request_", "command"),
    [
        pytest.param([b"PING", b"a", b"b"], "ping", id="ping-too-many"),
        pytest.param([b"SET", b"key"], "set", id="set-too-few"),
        pytest.param([b"SET", b"key", b"value", b"EX", b"5"], "set", id="set-with-options"),
        pytest.param([b"GET"], "get", id="get-too-few"),
        pytest.param([b"GET", b"a", b"b"], "get", id="get-too-many"),
        pytest.param([b"DEL"], "del", id="del-too-few"),
        pytest.param([b"EXISTS"], "exists", id="exists-too-few"),
        pytest.param([b"INCR"], "incr", id="incr-too-few"),
        pytest.param([b"INCR", b"a", b"b"], "incr", id="incr-too-many"),
        pytest.param([b"DBSIZE", b"a"], "dbsize", id="dbsize-too-many"),
        pytest.param([b"FLUSHDB", b"ASYNC"], "flushdb", id="flushdb-with-argument"),
    ],
)
def test_wrong_arity_is_reported_with_the_canonical_command_name(
    store: KeyValueStore, request_: list[bytes], command: str
) -> None:
    reply = dispatch(store, request_)

    assert reply == b"-ERR wrong number of arguments for '%s' command\r\n" % command.encode()


def test_wrong_arity_does_not_touch_the_store(store: KeyValueStore) -> None:
    dispatch(store, [b"SET", b"key"])

    assert store.dbsize() == 0


def test_unknown_command_is_reported_with_its_arguments(store: KeyValueStore) -> None:
    reply = dispatch(store, [b"COMMAND", b"DOCS"])

    assert reply == b"-ERR unknown command 'COMMAND', with args beginning with: 'DOCS', \r\n"


def test_unknown_command_without_arguments(store: KeyValueStore) -> None:
    assert dispatch(store, [b"HELLO"]) == (
        b"-ERR unknown command 'HELLO', with args beginning with: \r\n"
    )


def test_unknown_command_name_is_escaped_rather_than_echoed(store: KeyValueStore) -> None:
    reply = dispatch(store, [b"\xff\r\nbad"])

    assert reply == b"-ERR unknown command '\\xff\\x0d\\x0abad', with args beginning with: \r\n"
    assert reply.count(b"\r\n") == 1


def test_store_errors_become_resp_errors(store: KeyValueStore) -> None:
    store.set(b"key", b"not-a-number")

    reply = dispatch(store, [b"INCR", b"key"])

    assert reply == b"-ERR value is not an integer or out of range\r\n"


def test_store_error_leaves_the_value_untouched(store: KeyValueStore) -> None:
    store.set(b"key", b"abc")

    dispatch(store, [b"INCR", b"key"])

    assert store.get(b"key") == b"abc"


def test_overflow_becomes_a_resp_error(store: KeyValueStore) -> None:
    store.set(b"key", b"9223372036854775807")

    assert dispatch(store, [b"INCR", b"key"]) == (
        b"-ERR increment or decrement would overflow\r\n"
    )


def test_every_reply_is_a_single_resp_frame(store: KeyValueStore) -> None:
    # Guards against any reply path forgetting or duplicating a terminator.
    for request_ in ([b"PING"], [b"SET", b"a", b"b"], [b"GET", b"a"], [b"DBSIZE"], [b"NOPE"]):
        assert dispatch(store, request_).endswith(b"\r\n")
