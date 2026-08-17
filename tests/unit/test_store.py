from __future__ import annotations

import pytest

from pyredis.store import (
    INT64_MAX,
    INT64_MIN,
    IntegerOverflowError,
    InvalidExpireError,
    KeyValueStore,
    NotAnIntegerError,
    StoreError,
    TtlResult,
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


# --------------------------------------------------------------------------
# Expiration
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


def test_a_key_with_a_ttl_is_readable_before_its_deadline(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(999)

    assert timed.get(b"key") == b"value"
    assert timed.exists(b"key") == 1


def test_a_key_expires_exactly_at_its_deadline(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    # The boundary is inclusive: now == deadline means gone.
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.get(b"key") is None
    assert timed.exists(b"key") == 0
    assert timed.ttl(b"key") == (False, None)


def test_a_long_expired_key_is_missing(timed: KeyValueStore, clock: FakeClock) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(10_000_000)

    assert timed.get(b"key") is None
    assert timed.dbsize() == 0


def test_reading_an_expired_key_reclaims_it(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(1000)
    timed.get(b"key")

    assert timed.dbsize() == 0
    assert b"key" not in timed._expires


def test_binary_keys_can_carry_a_ttl(timed: KeyValueStore, clock: FakeClock) -> None:
    timed.set(BINARY_KEY, BINARY_VALUE, ttl_ms=1000)

    assert timed.get(BINARY_KEY) == BINARY_VALUE
    clock.advance(1000)
    assert timed.get(BINARY_KEY) is None


@pytest.mark.parametrize("ttl_ms", [0, -1, -1000])
def test_set_rejects_a_non_positive_ttl(timed: KeyValueStore, ttl_ms: int) -> None:
    with pytest.raises(InvalidExpireError):
        timed.set(b"key", b"value", ttl_ms=ttl_ms)


def test_set_rejects_a_deadline_beyond_the_signed_64_bit_range(
    timed: KeyValueStore,
) -> None:
    with pytest.raises(InvalidExpireError):
        timed.set(b"key", b"value", ttl_ms=INT64_MAX)


def test_a_rejected_ttl_leaves_the_key_untouched(timed: KeyValueStore) -> None:
    timed.set(b"key", b"original")

    with pytest.raises(InvalidExpireError):
        timed.set(b"key", b"replacement", ttl_ms=0)

    assert timed.get(b"key") == b"original"


# --------------------------------------------------------------------------
# EXPIRE / TTL / PERSIST
# --------------------------------------------------------------------------


def test_expire_sets_a_deadline_on_an_existing_key(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value")

    assert timed.expire(b"key", 1000) is True
    clock.advance(999)
    assert timed.get(b"key") == b"value"
    clock.advance(1)
    assert timed.get(b"key") is None


def test_expire_reports_false_for_a_missing_key(timed: KeyValueStore) -> None:
    assert timed.expire(b"missing", 1000) is False


def test_expire_reports_false_for_an_already_expired_key(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.expire(b"key", 5000) is False
    assert timed.get(b"key") is None


def test_expire_replaces_an_existing_deadline(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)

    timed.expire(b"key", 5000)
    clock.advance(2000)

    assert timed.get(b"key") == b"value"


@pytest.mark.parametrize("ttl_ms", [0, -1, -5000])
def test_expire_with_a_non_positive_ttl_deletes_the_key(
    timed: KeyValueStore, ttl_ms: int
) -> None:
    timed.set(b"key", b"value")

    assert timed.expire(b"key", ttl_ms) is True
    assert timed.get(b"key") is None
    assert timed.dbsize() == 0


def test_expire_rejects_a_deadline_beyond_the_signed_64_bit_range(
    timed: KeyValueStore,
) -> None:
    timed.set(b"key", b"value")

    with pytest.raises(InvalidExpireError):
        timed.expire(b"key", INT64_MAX)


def test_ttl_reports_a_missing_key(timed: KeyValueStore) -> None:
    assert timed.ttl(b"missing") == TtlResult(exists=False, remaining_ms=None)


def test_ttl_reports_a_key_without_a_deadline(timed: KeyValueStore) -> None:
    timed.set(b"key", b"value")

    assert timed.ttl(b"key") == TtlResult(exists=True, remaining_ms=None)


def test_ttl_reports_the_remaining_milliseconds(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(400)

    assert timed.ttl(b"key") == TtlResult(exists=True, remaining_ms=600)


def test_ttl_reports_a_key_gone_at_the_exact_boundary(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.ttl(b"key") == TtlResult(exists=False, remaining_ms=None)


def test_persist_removes_a_deadline_and_keeps_the_key(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)

    assert timed.persist(b"key") is True
    clock.advance(10_000)
    assert timed.get(b"key") == b"value"
    assert timed.ttl(b"key") == TtlResult(exists=True, remaining_ms=None)


def test_persist_reports_false_without_a_deadline(timed: KeyValueStore) -> None:
    timed.set(b"key", b"value")

    assert timed.persist(b"key") is False


def test_persist_reports_false_for_a_missing_key(timed: KeyValueStore) -> None:
    assert timed.persist(b"missing") is False


def test_persist_reports_false_for_an_expired_key(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.persist(b"key") is False


# --------------------------------------------------------------------------
# Interaction between expiration and the P1 operations
# --------------------------------------------------------------------------


def test_plain_set_clears_an_existing_ttl(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)

    timed.set(b"key", b"replacement")

    clock.advance(10_000)
    assert timed.get(b"key") == b"replacement"


def test_set_with_a_ttl_replaces_an_existing_ttl(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=10_000)

    timed.set(b"key", b"value", ttl_ms=1000)

    clock.advance(1000)
    assert timed.get(b"key") is None


def test_incr_preserves_an_existing_ttl(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"counter", b"1", ttl_ms=1000)

    assert timed.incr(b"counter") == 2

    assert timed.ttl(b"counter") == TtlResult(exists=True, remaining_ms=1000)
    clock.advance(1000)
    assert timed.get(b"counter") is None


def test_incr_on_an_expired_key_starts_from_zero(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"counter", b"41", ttl_ms=1000)
    clock.advance(1000)

    assert timed.incr(b"counter") == 1
    assert timed.ttl(b"counter") == TtlResult(exists=True, remaining_ms=None)


def test_a_failed_incr_changes_neither_value_nor_ttl(timed: KeyValueStore) -> None:
    timed.set(b"key", b"abc", ttl_ms=1000)

    with pytest.raises(NotAnIntegerError):
        timed.incr(b"key")

    assert timed.get(b"key") == b"abc"
    assert timed.ttl(b"key") == TtlResult(exists=True, remaining_ms=1000)


def test_delete_removes_the_expiration_metadata(timed: KeyValueStore) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)

    assert timed.delete(b"key") == 1
    assert timed._expires == {}


def test_delete_reports_zero_for_an_expired_key(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"key", b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.delete(b"key") == 0


def test_flushdb_removes_all_expiration_metadata(timed: KeyValueStore) -> None:
    timed.set(b"a", b"1", ttl_ms=1000)
    timed.set(b"b", b"2", ttl_ms=1000)

    timed.flushdb()

    assert timed._expires == {}
    assert timed.dbsize() == 0


def test_dbsize_counts_only_live_keys(timed: KeyValueStore, clock: FakeClock) -> None:
    timed.set(b"permanent", b"1")
    timed.set(b"short", b"2", ttl_ms=1000)

    assert timed.dbsize() == 2
    clock.advance(1000)
    assert timed.dbsize() == 1


def test_dbsize_reclaims_the_keys_it_skips(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"short", b"2", ttl_ms=1000)
    clock.advance(1000)

    timed.dbsize()

    assert timed._data == {}
    assert timed._expires == {}


def test_an_expiration_entry_never_outlives_its_key(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    timed.set(b"a", b"1", ttl_ms=1000)
    timed.set(b"b", b"2", ttl_ms=5000)
    timed.set(b"c", b"3")

    timed.delete(b"b")
    clock.advance(1000)
    timed.get(b"a")

    assert set(timed._expires) <= set(timed._data)
    assert timed._expires == {}


# --------------------------------------------------------------------------
# Active expiration
# --------------------------------------------------------------------------


def test_sweep_collects_expired_keys_that_nobody_read(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    for index in range(5):
        timed.set(b"key%d" % index, b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.sweep_expired(100) == 5
    assert timed._data == {}
    assert timed._expires == {}


def test_sweep_leaves_live_keys_alone(timed: KeyValueStore, clock: FakeClock) -> None:
    timed.set(b"live", b"value", ttl_ms=10_000)
    timed.set(b"dead", b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.sweep_expired(100) == 1
    assert timed.get(b"live") == b"value"


def test_sweep_examines_at_most_the_requested_number_of_keys(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    for index in range(10):
        timed.set(b"key%d" % index, b"value", ttl_ms=1000)
    clock.advance(1000)

    assert timed.sweep_expired(4) == 4
    assert len(timed._expires) == 6


def test_repeated_sweeps_eventually_cover_every_key(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    for index in range(10):
        timed.set(b"key%d" % index, b"value", ttl_ms=1000)
    clock.advance(1000)

    while timed._expires:
        timed.sweep_expired(3)

    assert timed._data == {}


def test_the_sweep_cursor_reaches_keys_behind_a_long_lived_one(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    # A key inserted first with a distant deadline must not shield the rest
    # from ever being examined.
    timed.set(b"immortal", b"value", ttl_ms=10_000_000)
    for index in range(50):
        timed.set(b"key%d" % index, b"value", ttl_ms=1000)
    clock.advance(1000)

    for _ in range(20):
        timed.sweep_expired(5)

    assert timed.dbsize() == 1
    assert timed.get(b"immortal") == b"value"


def test_sweeping_an_idle_store_does_nothing(timed: KeyValueStore) -> None:
    assert timed.sweep_expired(100) == 0

    timed.set(b"permanent", b"value")

    assert timed.sweep_expired(100) == 0
    assert timed.get(b"permanent") == b"value"


def test_sweep_does_not_rescan_within_one_call(
    timed: KeyValueStore, clock: FakeClock
) -> None:
    # Two live keys and a limit of 10: the cursor refills once, finds nothing
    # to remove, and stops rather than spinning over the same keys.
    timed.set(b"a", b"1", ttl_ms=10_000)
    timed.set(b"b", b"2", ttl_ms=10_000)

    assert timed.sweep_expired(10) == 0
    assert timed.dbsize() == 2
