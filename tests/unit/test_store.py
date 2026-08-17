from __future__ import annotations

import pytest

from pyredis.store import (
    INT64_MAX,
    INT64_MIN,
    IntegerOverflowError,
    KeyValueStore,
    NotAnIntegerError,
    StoreError,
)

#: Deliberately not valid UTF-8, and full of bytes a text-based store would mangle.
BINARY_KEY = b"\x00\xff\xfe key\x00"
BINARY_VALUE = b"\x89PNG\r\n\x1a\n\x00\xff\xfe\x80"


@pytest.fixture
def store() -> KeyValueStore:
    return KeyValueStore()


# --------------------------------------------------------------------------
# SET / GET
# --------------------------------------------------------------------------


def test_get_returns_the_value_that_was_set(store: KeyValueStore) -> None:
    store.set(b"key", b"value")

    assert store.get(b"key") == b"value"


def test_get_returns_none_for_a_missing_key(store: KeyValueStore) -> None:
    assert store.get(b"missing") is None


def test_set_overwrites_an_existing_value(store: KeyValueStore) -> None:
    store.set(b"key", b"first")
    store.set(b"key", b"second")

    assert store.get(b"key") == b"second"


def test_overwrite_does_not_change_the_key_count(store: KeyValueStore) -> None:
    store.set(b"key", b"first")
    store.set(b"key", b"second")

    assert store.dbsize() == 1


def test_empty_value_is_stored_and_is_distinct_from_a_missing_key(
    store: KeyValueStore,
) -> None:
    store.set(b"key", b"")

    assert store.get(b"key") == b""
    assert store.get(b"key") is not None
    assert store.exists(b"key") == 1


def test_empty_key_is_usable(store: KeyValueStore) -> None:
    store.set(b"", b"value")

    assert store.get(b"") == b"value"
    assert store.dbsize() == 1


def test_binary_keys_and_values_round_trip_unchanged(store: KeyValueStore) -> None:
    store.set(BINARY_KEY, BINARY_VALUE)

    assert store.get(BINARY_KEY) == BINARY_VALUE


def test_keys_differing_only_by_a_trailing_nul_are_distinct(store: KeyValueStore) -> None:
    store.set(b"key", b"plain")
    store.set(b"key\x00", b"with-nul")

    assert store.get(b"key") == b"plain"
    assert store.get(b"key\x00") == b"with-nul"
    assert store.dbsize() == 2


def test_keys_that_differ_only_by_unicode_normalization_stay_distinct(
    store: KeyValueStore,
) -> None:
    # U+00E9 (NFC) and 'e' + U+0301 (NFD) render identically and compare equal
    # after normalization, but they are different byte strings and so must be
    # different keys. Spelled as byte literals so the source encoding cannot
    # quietly normalize one into the other.
    nfc = b"\xc3\xa9"
    nfd = b"e\xcc\x81"

    store.set(nfc, b"composed")
    store.set(nfd, b"decomposed")

    assert store.get(nfc) == b"composed"
    assert store.get(nfd) == b"decomposed"
    assert store.dbsize() == 2


# --------------------------------------------------------------------------
# DEL
# --------------------------------------------------------------------------


def test_delete_removes_the_key_and_reports_one(store: KeyValueStore) -> None:
    store.set(b"key", b"value")

    assert store.delete(b"key") == 1
    assert store.get(b"key") is None
    assert store.dbsize() == 0


def test_delete_reports_zero_for_a_missing_key(store: KeyValueStore) -> None:
    assert store.delete(b"missing") == 0


def test_delete_counts_only_the_keys_that_existed(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    assert store.delete(b"a", b"missing", b"b") == 2
    assert store.dbsize() == 0


def test_delete_counts_a_repeated_key_once(store: KeyValueStore) -> None:
    store.set(b"key", b"value")

    assert store.delete(b"key", b"key") == 1


def test_delete_with_no_keys_returns_zero(store: KeyValueStore) -> None:
    store.set(b"key", b"value")

    assert store.delete() == 0
    assert store.dbsize() == 1


def test_delete_leaves_other_keys_alone(store: KeyValueStore) -> None:
    store.set(b"keep", b"value")
    store.set(b"drop", b"value")

    store.delete(b"drop")

    assert store.get(b"keep") == b"value"


# --------------------------------------------------------------------------
# EXISTS
# --------------------------------------------------------------------------


def test_exists_reports_one_for_a_present_key(store: KeyValueStore) -> None:
    store.set(b"key", b"value")

    assert store.exists(b"key") == 1


def test_exists_reports_zero_for_a_missing_key(store: KeyValueStore) -> None:
    assert store.exists(b"missing") == 0


def test_exists_sums_across_keys(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    assert store.exists(b"a", b"b", b"missing") == 2


def test_exists_counts_a_repeated_key_every_time(store: KeyValueStore) -> None:
    store.set(b"key", b"value")

    assert store.exists(b"key", b"key", b"key") == 3


def test_exists_with_no_keys_returns_zero(store: KeyValueStore) -> None:
    assert store.exists() == 0


# --------------------------------------------------------------------------
# INCR
# --------------------------------------------------------------------------


def test_incr_creates_a_missing_key_holding_one(store: KeyValueStore) -> None:
    assert store.incr(b"counter") == 1
    assert store.get(b"counter") == b"1"


@pytest.mark.parametrize(
    ("stored", "expected"),
    [
        (b"0", 1),
        (b"1", 2),
        (b"9", 10),
        (b"-1", 0),
        (b"-5", -4),
        (b"100", 101),
        (str(INT64_MIN).encode(), INT64_MIN + 1),
        (str(INT64_MAX - 1).encode(), INT64_MAX),
    ],
)
def test_incr_increments_valid_integers(
    store: KeyValueStore, stored: bytes, expected: int
) -> None:
    store.set(b"counter", stored)

    assert store.incr(b"counter") == expected
    assert store.get(b"counter") == str(expected).encode()


def test_incr_writes_back_canonical_decimal_bytes(store: KeyValueStore) -> None:
    store.set(b"counter", b"-1")

    store.incr(b"counter")

    assert store.get(b"counter") == b"0"


def test_repeated_incr_accumulates(store: KeyValueStore) -> None:
    for expected in range(1, 6):
        assert store.incr(b"counter") == expected

    assert store.get(b"counter") == b"5"


def test_incr_at_the_signed_64_bit_minimum_succeeds(store: KeyValueStore) -> None:
    store.set(b"counter", str(INT64_MIN).encode())

    assert store.incr(b"counter") == INT64_MIN + 1


def test_incr_at_the_signed_64_bit_maximum_overflows(store: KeyValueStore) -> None:
    at_max = str(INT64_MAX).encode()
    store.set(b"counter", at_max)

    with pytest.raises(IntegerOverflowError, match="increment or decrement would overflow"):
        store.incr(b"counter")

    assert store.get(b"counter") == at_max


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b" ", id="blank"),
        pytest.param(b" 1", id="leading-space"),
        pytest.param(b"1 ", id="trailing-space"),
        pytest.param(b"\t1", id="leading-tab"),
        pytest.param(b"1\n", id="trailing-newline"),
        pytest.param(b"+1", id="explicit-plus"),
        pytest.param(b"01", id="leading-zero"),
        pytest.param(b"007", id="multiple-leading-zeros"),
        pytest.param(b"-0", id="negative-zero"),
        pytest.param(b"-01", id="negative-leading-zero"),
        pytest.param(b"--1", id="double-minus"),
        pytest.param(b"-", id="lone-minus"),
        pytest.param(b"3.0", id="decimal-point"),
        pytest.param(b"1e3", id="scientific"),
        pytest.param(b"0x10", id="hexadecimal"),
        pytest.param(b"1_0", id="underscore-separator"),
        pytest.param(b"abc", id="letters"),
        pytest.param(b"\xff", id="non-ascii-byte"),
        pytest.param(b"\xd9\xa1", id="arabic-indic-digit-one"),
        pytest.param(b"\xef\xbc\x91", id="fullwidth-digit-one"),
        pytest.param(b"1\x002", id="embedded-nul"),
    ],
)
def test_incr_rejects_values_that_are_not_canonical_integers(
    store: KeyValueStore, stored: bytes
) -> None:
    store.set(b"counter", stored)

    with pytest.raises(NotAnIntegerError, match="value is not an integer or out of range"):
        store.incr(b"counter")

    assert store.get(b"counter") == stored


@pytest.mark.parametrize(
    "stored",
    [
        pytest.param(str(INT64_MAX + 1).encode(), id="one-above-max"),
        pytest.param(str(INT64_MIN - 1).encode(), id="one-below-min"),
        pytest.param(b"99999999999999999999999999", id="far-above-max"),
    ],
)
def test_incr_rejects_values_outside_the_signed_64_bit_range(
    store: KeyValueStore, stored: bytes
) -> None:
    store.set(b"counter", stored)

    with pytest.raises(NotAnIntegerError):
        store.incr(b"counter")

    assert store.get(b"counter") == stored


def test_a_key_created_by_incr_is_an_ordinary_key(store: KeyValueStore) -> None:
    store.incr(b"counter")

    assert store.exists(b"counter") == 1
    assert store.dbsize() == 1
    assert store.delete(b"counter") == 1


def test_incr_errors_share_a_base_class() -> None:
    assert issubclass(NotAnIntegerError, StoreError)
    assert issubclass(IntegerOverflowError, StoreError)


# --------------------------------------------------------------------------
# DBSIZE / FLUSHDB
# --------------------------------------------------------------------------


def test_dbsize_is_zero_for_a_fresh_store(store: KeyValueStore) -> None:
    assert store.dbsize() == 0


def test_dbsize_counts_distinct_keys(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    assert store.dbsize() == 2


def test_dbsize_drops_after_delete(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")
    store.delete(b"a")

    assert store.dbsize() == 1


def test_flushdb_removes_every_key(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    store.flushdb()

    assert store.dbsize() == 0
    assert store.get(b"a") is None
    assert store.exists(b"a", b"b") == 0


def test_flushdb_on_an_empty_store_is_a_no_op(store: KeyValueStore) -> None:
    store.flushdb()

    assert store.dbsize() == 0


def test_store_is_usable_after_flushdb(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.flushdb()
    store.set(b"a", b"2")

    assert store.get(b"a") == b"2"
    assert store.dbsize() == 1


# --------------------------------------------------------------------------
# Isolation
# --------------------------------------------------------------------------


def test_separate_stores_share_no_state() -> None:
    first = KeyValueStore()
    second = KeyValueStore()

    first.set(b"key", b"value")

    assert second.get(b"key") is None
    assert second.dbsize() == 0
