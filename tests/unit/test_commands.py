from __future__ import annotations

import pytest

from pyredis.aof import NO_JOURNAL, PersistenceError, scan
from pyredis.commands import dispatch
from pyredis.store import (
    ENTRY_OVERHEAD_BYTES,
    KeyValueStore,
    MaxmemoryPolicy,
)


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
        pytest.param(
            [b"SET", b"key", b"value", b"EX", b"5", b"PX", b"9"], "set", id="set-too-many"
        ),
        pytest.param([b"EXPIRE", b"key"], "expire", id="expire-too-few"),
        pytest.param([b"EXPIRE", b"key", b"1", b"NX"], "expire", id="expire-too-many"),
        pytest.param([b"TTL"], "ttl", id="ttl-too-few"),
        pytest.param([b"TTL", b"a", b"b"], "ttl", id="ttl-too-many"),
        pytest.param([b"PERSIST"], "persist", id="persist-too-few"),
        pytest.param([b"PERSIST", b"a", b"b"], "persist", id="persist-too-many"),
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


# --------------------------------------------------------------------------
# Expiration commands
# --------------------------------------------------------------------------


class FakeClock:
    """A clock the tests move by hand, so no test ever waits in real time."""

    def __init__(self, now: int = 1_700_000_000_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def timed(clock: FakeClock) -> KeyValueStore:
    return KeyValueStore(clock=clock)


def test_set_with_ex_installs_a_deadline(timed: KeyValueStore, clock: FakeClock) -> None:
    assert dispatch(timed, [b"SET", b"key", b"value", b"EX", b"2"]) == b"+OK\r\n"

    clock.advance(1999)
    assert dispatch(timed, [b"GET", b"key"]) == b"$5\r\nvalue\r\n"
    clock.advance(1)
    assert dispatch(timed, [b"GET", b"key"]) == b"$-1\r\n"


def test_set_with_px_installs_a_deadline(timed: KeyValueStore, clock: FakeClock) -> None:
    assert dispatch(timed, [b"SET", b"key", b"value", b"PX", b"150"]) == b"+OK\r\n"

    clock.advance(149)
    assert dispatch(timed, [b"GET", b"key"]) == b"$5\r\nvalue\r\n"
    clock.advance(1)
    assert dispatch(timed, [b"GET", b"key"]) == b"$-1\r\n"


@pytest.mark.parametrize("option", [b"EX", b"ex", b"Ex", b"eX"])
def test_the_expiration_option_is_case_insensitive(
    timed: KeyValueStore, option: bytes
) -> None:
    assert dispatch(timed, [b"SET", b"key", b"value", option, b"5"]) == b"+OK\r\n"
    assert dispatch(timed, [b"TTL", b"key"]) == b":5\r\n"


def test_set_options_do_not_disturb_binary_keys_and_values(timed: KeyValueStore) -> None:
    key = b"\x00\xff key"
    value = b"\r\n\x00binary"

    dispatch(timed, [b"SET", key, value, b"PX", b"5000"])

    assert dispatch(timed, [b"GET", key]) == b"$%d\r\n%s\r\n" % (len(value), value)


@pytest.mark.parametrize(
    "request_",
    [
        pytest.param([b"SET", b"k", b"v", b"FOO", b"5"], id="unknown-option"),
        pytest.param([b"SET", b"k", b"v", b"EX"], id="missing-amount"),
        pytest.param([b"SET", b"k", b"v", b"5"], id="amount-without-keyword"),
    ],
)
def test_bad_set_option_syntax_is_a_syntax_error(
    timed: KeyValueStore, request_: list[bytes]
) -> None:
    assert dispatch(timed, request_) == b"-ERR syntax error\r\n"


@pytest.mark.parametrize("amount", [b"abc", b"1.5", b"+1", b"01", b"", b"\xff"])
def test_a_malformed_duration_is_reported_as_a_bad_integer(
    timed: KeyValueStore, amount: bytes
) -> None:
    reply = dispatch(timed, [b"SET", b"k", b"v", b"EX", amount])

    assert reply == b"-ERR value is not an integer or out of range\r\n"


@pytest.mark.parametrize("amount", [b"0", b"-1"])
def test_set_rejects_a_non_positive_expire_time(
    timed: KeyValueStore, amount: bytes
) -> None:
    reply = dispatch(timed, [b"SET", b"k", b"v", b"EX", amount])

    assert reply == b"-ERR invalid expire time in 'set' command\r\n"


def test_set_rejects_an_overflowing_expire_time(timed: KeyValueStore) -> None:
    reply = dispatch(timed, [b"SET", b"k", b"v", b"EX", b"9223372036854775807"])

    assert reply == b"-ERR invalid expire time in 'set' command\r\n"


def test_a_rejected_set_option_leaves_the_keyspace_untouched(
    timed: KeyValueStore,
) -> None:
    dispatch(timed, [b"SET", b"k", b"v", b"EX", b"0"])

    assert dispatch(timed, [b"DBSIZE"]) == b":0\r\n"


def test_expire_returns_one_when_applied(timed: KeyValueStore, clock: FakeClock) -> None:
    dispatch(timed, [b"SET", b"key", b"value"])

    assert dispatch(timed, [b"EXPIRE", b"key", b"1"]) == b":1\r\n"

    clock.advance(1000)
    assert dispatch(timed, [b"GET", b"key"]) == b"$-1\r\n"


def test_expire_returns_zero_for_a_missing_key(timed: KeyValueStore) -> None:
    assert dispatch(timed, [b"EXPIRE", b"missing", b"1"]) == b":0\r\n"


def test_expire_with_a_non_positive_time_deletes_the_key(timed: KeyValueStore) -> None:
    dispatch(timed, [b"SET", b"key", b"value"])

    assert dispatch(timed, [b"EXPIRE", b"key", b"0"]) == b":1\r\n"
    assert dispatch(timed, [b"GET", b"key"]) == b"$-1\r\n"


def test_expire_rejects_a_malformed_duration(timed: KeyValueStore) -> None:
    dispatch(timed, [b"SET", b"key", b"value"])

    assert dispatch(timed, [b"EXPIRE", b"key", b"soon"]) == (
        b"-ERR value is not an integer or out of range\r\n"
    )


def test_expire_rejects_an_overflowing_expire_time(timed: KeyValueStore) -> None:
    dispatch(timed, [b"SET", b"key", b"value"])

    assert dispatch(timed, [b"EXPIRE", b"key", b"9223372036854775807"]) == (
        b"-ERR invalid expire time in 'expire' command\r\n"
    )


def test_ttl_reports_minus_two_for_a_missing_key(timed: KeyValueStore) -> None:
    assert dispatch(timed, [b"TTL", b"missing"]) == b":-2\r\n"


def test_ttl_reports_minus_one_without_a_deadline(timed: KeyValueStore) -> None:
    dispatch(timed, [b"SET", b"key", b"value"])

    assert dispatch(timed, [b"TTL", b"key"]) == b":-1\r\n"


def test_ttl_reports_minus_two_at_the_exact_boundary(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    dispatch(timed, [b"SET", b"key", b"value", b"PX", b"1000"])
    clock.advance(1000)

    assert dispatch(timed, [b"TTL", b"key"]) == b":-2\r\n"


@pytest.mark.parametrize(
    ("remaining_ms", "expected"),
    [
        pytest.param(2000, b":2\r\n", id="two-seconds"),
        pytest.param(1500, b":2\r\n", id="rounds-half-up"),
        pytest.param(1499, b":1\r\n", id="rounds-down"),
        pytest.param(500, b":1\r\n", id="half-second-rounds-up"),
        pytest.param(499, b":0\r\n", id="under-half-a-second"),
        pytest.param(1, b":0\r\n", id="one-millisecond-left"),
    ],
)
def test_ttl_rounds_to_the_nearest_second(
    timed: KeyValueStore, remaining_ms: int, expected: bytes
) -> None:
    dispatch(timed, [b"SET", b"key", b"value", b"PX", str(remaining_ms).encode()])

    assert dispatch(timed, [b"TTL", b"key"]) == expected


def test_ttl_counts_down(timed: KeyValueStore, clock: FakeClock) -> None:
    dispatch(timed, [b"SET", b"key", b"value", b"EX", b"10"])

    clock.advance(4000)

    assert dispatch(timed, [b"TTL", b"key"]) == b":6\r\n"


def test_persist_returns_one_and_clears_the_deadline(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    dispatch(timed, [b"SET", b"key", b"value", b"EX", b"1"])

    assert dispatch(timed, [b"PERSIST", b"key"]) == b":1\r\n"
    assert dispatch(timed, [b"TTL", b"key"]) == b":-1\r\n"

    clock.advance(10_000)
    assert dispatch(timed, [b"GET", b"key"]) == b"$5\r\nvalue\r\n"


def test_persist_returns_zero_without_a_deadline(timed: KeyValueStore) -> None:
    dispatch(timed, [b"SET", b"key", b"value"])

    assert dispatch(timed, [b"PERSIST", b"key"]) == b":0\r\n"


def test_persist_returns_zero_for_a_missing_key(timed: KeyValueStore) -> None:
    assert dispatch(timed, [b"PERSIST", b"missing"]) == b":0\r\n"


def test_plain_set_clears_a_deadline(timed: KeyValueStore, clock: FakeClock) -> None:
    dispatch(timed, [b"SET", b"key", b"value", b"EX", b"1"])

    dispatch(timed, [b"SET", b"key", b"fresh"])

    assert dispatch(timed, [b"TTL", b"key"]) == b":-1\r\n"
    clock.advance(10_000)
    assert dispatch(timed, [b"GET", b"key"]) == b"$5\r\nfresh\r\n"


def test_incr_keeps_a_deadline(timed: KeyValueStore) -> None:
    dispatch(timed, [b"SET", b"counter", b"1", b"EX", b"10"])

    assert dispatch(timed, [b"INCR", b"counter"]) == b":2\r\n"
    assert dispatch(timed, [b"TTL", b"counter"]) == b":10\r\n"


def test_dbsize_ignores_expired_keys(timed: KeyValueStore, clock: FakeClock) -> None:
    dispatch(timed, [b"SET", b"permanent", b"1"])
    dispatch(timed, [b"SET", b"fleeting", b"2", b"PX", b"100"])

    assert dispatch(timed, [b"DBSIZE"]) == b":2\r\n"

    clock.advance(100)
    assert dispatch(timed, [b"DBSIZE"]) == b":1\r\n"


@pytest.mark.parametrize("name", [b"PTTL", b"PEXPIRE", b"EXPIREAT", b"PEXPIREAT", b"GETEX"])
def test_unsupported_expiration_commands_are_unknown(
    timed: KeyValueStore, name: bytes
) -> None:
    reply = dispatch(timed, [name, b"key", b"1"])

    assert reply.startswith(b"-ERR unknown command '%s'" % name)


# --------------------------------------------------------------------------
# What gets journalled
# --------------------------------------------------------------------------


class FakeJournal:
    """Records what would have been persisted, and can be made to fail.

    `fail` starts the journal in the failed state, as it would be after an
    earlier write or an everysec fsync failed. `fail_on_append` models the
    write that *causes* the failure: healthy when the command is admitted,
    broken by the time the record is handed over.
    """

    def __init__(self, *, fail: bool = False, fail_on_append: bool = False) -> None:
        self.records: list[list[bytes]] = []
        self.failed = fail
        self._fail_on_append = fail_on_append

    def append(self, record: bytes) -> None:
        if self.failed or self._fail_on_append:
            self.failed = True
            raise PersistenceError("journal has failed")
        self.records.append(scan(record).records[0].parts)

    def sync(self) -> None:
        return None

    def close(self) -> None:
        return None


@pytest.fixture
def journal() -> FakeJournal:
    return FakeJournal()


def test_set_is_journalled(timed: KeyValueStore, journal: FakeJournal) -> None:
    dispatch(timed, [b"SET", b"k", b"v"], journal)

    assert journal.records == [[b"SET", b"k", b"v"]]


def test_set_with_ex_is_journalled_as_an_absolute_deadline(
    timed: KeyValueStore, clock: FakeClock, journal: FakeJournal
) -> None:
    dispatch(timed, [b"SET", b"k", b"v", b"EX", b"60"], journal)

    assert journal.records == [
        [b"SET", b"k", b"v", b"PXAT", str(clock.now + 60_000).encode()]
    ]


def test_set_with_px_is_journalled_as_an_absolute_deadline(
    timed: KeyValueStore, clock: FakeClock, journal: FakeJournal
) -> None:
    dispatch(timed, [b"SET", b"k", b"v", b"PX", b"1500"], journal)

    assert journal.records == [
        [b"SET", b"k", b"v", b"PXAT", str(clock.now + 1500).encode()]
    ]


def test_incr_is_journalled_as_the_resulting_value(
    timed: KeyValueStore, journal: FakeJournal
) -> None:
    dispatch(timed, [b"INCR", b"counter"], journal)
    dispatch(timed, [b"INCR", b"counter"], journal)

    assert journal.records == [
        [b"SET", b"counter", b"1"],
        [b"SET", b"counter", b"2"],
    ]


def test_incr_keeps_the_deadline_in_its_record(
    timed: KeyValueStore, clock: FakeClock, journal: FakeJournal
) -> None:
    dispatch(timed, [b"SET", b"counter", b"1", b"EX", b"60"], journal)
    journal.records.clear()

    dispatch(timed, [b"INCR", b"counter"], journal)

    assert journal.records == [
        [b"SET", b"counter", b"2", b"PXAT", str(clock.now + 60_000).encode()]
    ]


def test_expire_is_journalled_as_pexpireat(
    timed: KeyValueStore, clock: FakeClock, journal: FakeJournal
) -> None:
    dispatch(timed, [b"SET", b"k", b"v"], journal)
    journal.records.clear()

    dispatch(timed, [b"EXPIRE", b"k", b"30"], journal)

    assert journal.records == [[b"PEXPIREAT", b"k", str(clock.now + 30_000).encode()]]


def test_expire_with_a_non_positive_time_is_journalled_as_a_delete(
    timed: KeyValueStore, journal: FakeJournal
) -> None:
    dispatch(timed, [b"SET", b"k", b"v"], journal)
    journal.records.clear()

    dispatch(timed, [b"EXPIRE", b"k", b"0"], journal)

    assert journal.records == [[b"DEL", b"k"]]


def test_del_and_persist_and_flushdb_are_journalled(
    timed: KeyValueStore, journal: FakeJournal
) -> None:
    dispatch(timed, [b"SET", b"k", b"v", b"EX", b"60"], journal)
    journal.records.clear()

    dispatch(timed, [b"PERSIST", b"k"], journal)
    dispatch(timed, [b"DEL", b"k"], journal)
    dispatch(timed, [b"FLUSHDB"], journal)

    assert journal.records == [[b"PERSIST", b"k"], [b"DEL", b"k"], [b"FLUSHDB"]]


@pytest.mark.parametrize(
    "request_",
    [
        pytest.param([b"GET", b"k"], id="get"),
        pytest.param([b"EXISTS", b"k"], id="exists"),
        pytest.param([b"TTL", b"k"], id="ttl"),
        pytest.param([b"DBSIZE"], id="dbsize"),
        pytest.param([b"PING"], id="ping"),
    ],
)
def test_reads_are_never_journalled(
    timed: KeyValueStore, journal: FakeJournal, request_: list[bytes]
) -> None:
    dispatch(timed, request_, journal)

    assert journal.records == []


@pytest.mark.parametrize(
    "request_",
    [
        pytest.param([b"SET", b"k"], id="wrong-arity"),
        pytest.param([b"SET", b"k", b"v", b"FOO", b"1"], id="syntax-error"),
        pytest.param([b"SET", b"k", b"v", b"EX", b"0"], id="invalid-expire-time"),
        pytest.param([b"SET", b"k", b"v", b"EX", b"abc"], id="malformed-duration"),
        pytest.param([b"DEL", b"missing"], id="delete-of-a-missing-key"),
        pytest.param([b"PERSIST", b"missing"], id="persist-without-a-deadline"),
        pytest.param([b"EXPIRE", b"missing", b"5"], id="expire-of-a-missing-key"),
        pytest.param([b"NOPE", b"k"], id="unknown-command"),
    ],
)
def test_commands_that_change_nothing_are_never_journalled(
    timed: KeyValueStore, journal: FakeJournal, request_: list[bytes]
) -> None:
    dispatch(timed, request_, journal)

    assert journal.records == []


def test_a_failed_incr_is_not_journalled(
    timed: KeyValueStore, journal: FakeJournal
) -> None:
    dispatch(timed, [b"SET", b"k", b"abc"], journal)
    journal.records.clear()

    dispatch(timed, [b"INCR", b"k"], journal)

    assert journal.records == []


# --------------------------------------------------------------------------
# Persistence failure
# --------------------------------------------------------------------------


PERSISTENCE_ERROR = b"-ERR persistence failure\r\n"


def test_the_command_that_hits_the_failure_is_told(timed: KeyValueStore) -> None:
    breaking = FakeJournal(fail_on_append=True)

    assert dispatch(timed, [b"SET", b"k", b"v"], breaking) == PERSISTENCE_ERROR


def test_the_mutation_that_failed_may_remain_visible_in_memory(
    timed: KeyValueStore,
) -> None:
    # No rollback is attempted: the write happened, it just is not durable.
    breaking = FakeJournal(fail_on_append=True)

    dispatch(timed, [b"SET", b"k", b"v"], breaking)

    assert timed.get(b"k") == b"v"


def test_the_failing_write_puts_the_journal_into_the_failed_state(
    timed: KeyValueStore,
) -> None:
    breaking = FakeJournal(fail_on_append=True)

    assert dispatch(timed, [b"SET", b"k", b"v"], breaking) == PERSISTENCE_ERROR
    assert breaking.failed is True
    # The next mutation is turned away before it can reach the store.
    assert dispatch(timed, [b"SET", b"other", b"value"], breaking) == PERSISTENCE_ERROR
    assert timed.get(b"other") is None


@pytest.mark.parametrize(
    "request_",
    [
        pytest.param([b"SET", b"other", b"value"], id="set"),
        pytest.param([b"DEL", b"k"], id="del"),
        pytest.param([b"INCR", b"counter"], id="incr"),
        pytest.param([b"EXPIRE", b"k", b"60"], id="expire"),
        pytest.param([b"PERSIST", b"k"], id="persist"),
        pytest.param([b"FLUSHDB"], id="flushdb"),
    ],
)
def test_later_mutations_are_refused_before_they_touch_the_store(
    timed: KeyValueStore, request_: list[bytes]
) -> None:
    failing = FakeJournal(fail=True)
    timed.set(b"k", b"original", ttl_ms=60_000)
    timed.set(b"counter", b"5")
    before = dict(timed._data)

    assert dispatch(timed, request_, failing) == PERSISTENCE_ERROR
    assert timed._data == before, "the store was mutated despite a failed journal"


def test_reads_keep_working_after_a_persistence_failure(timed: KeyValueStore) -> None:
    failing = FakeJournal(fail=True)
    timed.set(b"k", b"v", ttl_ms=60_000)

    assert dispatch(timed, [b"GET", b"k"], failing) == b"$1\r\nv\r\n"
    assert dispatch(timed, [b"EXISTS", b"k"], failing) == b":1\r\n"
    assert dispatch(timed, [b"TTL", b"k"], failing) == b":60\r\n"
    assert dispatch(timed, [b"DBSIZE"], failing) == b":1\r\n"
    assert dispatch(timed, [b"PING"], failing) == b"+PONG\r\n"


def test_a_journal_that_fails_mid_session_stops_accepting_writes(
    timed: KeyValueStore, journal: FakeJournal
) -> None:
    assert dispatch(timed, [b"SET", b"a", b"1"], journal) == b"+OK\r\n"

    journal.failed = True  # as an everysec fsync failure would leave it

    assert dispatch(timed, [b"SET", b"b", b"2"], journal) == PERSISTENCE_ERROR
    assert timed.get(b"b") is None
    assert timed.get(b"a") == b"1"


def test_persistence_failures_do_not_exist_when_the_aof_is_disabled(
    timed: KeyValueStore,
) -> None:
    for request_ in ([b"SET", b"k", b"v"], [b"INCR", b"c"], [b"DEL", b"k"], [b"FLUSHDB"]):
        assert dispatch(timed, request_) != PERSISTENCE_ERROR

    assert NO_JOURNAL.failed is False


# --------------------------------------------------------------------------
# Memory limits and eviction
# --------------------------------------------------------------------------


OOM_ERROR = b"-OOM command not allowed when used memory > 'maxmemory'.\r\n"


def entry_cost(key: bytes, value: bytes) -> int:
    return len(key) + len(value) + ENTRY_OVERHEAD_BYTES


def test_a_refused_write_answers_with_the_oom_code(clock: FakeClock) -> None:
    store = KeyValueStore(clock=clock, maxmemory=entry_cost(b"first_", b"value"))
    dispatch(store, [b"SET", b"first_", b"value"])

    assert dispatch(store, [b"SET", b"second", b"value"]) == OOM_ERROR


def test_a_refused_write_changes_nothing_and_journals_nothing(
    clock: FakeClock, journal: FakeJournal
) -> None:
    store = KeyValueStore(clock=clock, maxmemory=entry_cost(b"first_", b"value"))
    dispatch(store, [b"SET", b"first_", b"value"], journal)
    journal.records.clear()

    assert dispatch(store, [b"SET", b"second", b"value"], journal) == OOM_ERROR
    assert journal.records == []
    assert dispatch(store, [b"DBSIZE"], journal) == b":1\r\n"


def test_incr_can_be_refused_for_memory(clock: FakeClock) -> None:
    store = KeyValueStore(clock=clock, maxmemory=entry_cost(b"count_", b"9"))
    dispatch(store, [b"SET", b"count_", b"9"])

    assert dispatch(store, [b"INCR", b"count_"]) == OOM_ERROR
    assert dispatch(store, [b"GET", b"count_"]) == b"$1\r\n9\r\n"


def test_expire_is_never_refused_for_memory(clock: FakeClock) -> None:
    store = KeyValueStore(clock=clock, maxmemory=entry_cost(b"first_", b"value"))
    dispatch(store, [b"SET", b"first_", b"value"])

    assert dispatch(store, [b"EXPIRE", b"first_", b"60"]) == b":1\r\n"
    assert dispatch(store, [b"TTL", b"first_"]) == b":60\r\n"


def test_reads_still_work_at_the_memory_limit(clock: FakeClock) -> None:
    store = KeyValueStore(clock=clock, maxmemory=entry_cost(b"first_", b"value"))
    dispatch(store, [b"SET", b"first_", b"value"])

    assert dispatch(store, [b"GET", b"first_"]) == b"$5\r\nvalue\r\n"
    assert dispatch(store, [b"EXISTS", b"first_"]) == b":1\r\n"
    assert dispatch(store, [b"PING"]) == b"+PONG\r\n"


def test_an_eviction_is_journalled_as_a_delete_before_the_write(
    clock: FakeClock, journal: FakeJournal
) -> None:
    store = KeyValueStore(
        clock=clock,
        maxmemory=2 * entry_cost(b"first_", b"value"),
        policy=MaxmemoryPolicy.ALLKEYS_LRU,
    )
    dispatch(store, [b"SET", b"first_", b"value"], journal)
    dispatch(store, [b"SET", b"second", b"value"], journal)
    journal.records.clear()

    dispatch(store, [b"SET", b"third_", b"value"], journal)

    assert journal.records == [
        [b"DEL", b"first_"],
        [b"SET", b"third_", b"value"],
    ]


def test_an_incr_that_evicts_journals_the_eviction(
    clock: FakeClock, journal: FakeJournal
) -> None:
    store = KeyValueStore(
        clock=clock,
        maxmemory=2 * entry_cost(b"count_", b"9"),
        policy=MaxmemoryPolicy.ALLKEYS_LRU,
    )
    dispatch(store, [b"SET", b"other_", b"9"], journal)
    dispatch(store, [b"SET", b"count_", b"9"], journal)
    journal.records.clear()

    dispatch(store, [b"INCR", b"count_"], journal)

    assert journal.records == [
        [b"DEL", b"other_"],
        [b"SET", b"count_", b"10"],
    ]


def test_a_persistence_failure_during_eviction_keeps_the_p4_behaviour(
    clock: FakeClock,
) -> None:
    breaking = FakeJournal(fail_on_append=True)
    store = KeyValueStore(
        clock=clock,
        maxmemory=2 * entry_cost(b"first_", b"value"),
        policy=MaxmemoryPolicy.ALLKEYS_LRU,
    )
    store.set(b"first_", b"value")
    store.set(b"second", b"value")

    # The eviction DEL is the append that fails.
    assert dispatch(store, [b"SET", b"third_", b"value"], breaking) == (
        b"-ERR persistence failure\r\n"
    )
    assert breaking.failed is True
    # No rollback, exactly as in P4: the eviction and the write both stand.
    assert store.get(b"first_") is None
    assert store.get(b"third_") == b"value"
    # And nothing further may mutate, while reads carry on.
    assert dispatch(store, [b"SET", b"fourth", b"value"], breaking) == (
        b"-ERR persistence failure\r\n"
    )
    assert dispatch(store, [b"GET", b"third_"], breaking) == b"$5\r\nvalue\r\n"
