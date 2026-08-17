"""Memory accounting and eviction."""

from __future__ import annotations

import pytest

from pyredis.store import (
    ENTRY_OVERHEAD_BYTES,
    MAXMEMORY_SAMPLES,
    KeyValueStore,
    MaxmemoryPolicy,
    OutOfMemoryError,
)


class FakeClock:
    """A clock the tests move by hand, so no test ever waits in real time."""

    def __init__(self, now: int = 1_700_000_000_000) -> None:
        self.now = now

    def __call__(self) -> int:
        return self.now

    def advance(self, ms: int) -> None:
        self.now += ms


def cost(key: bytes, value: bytes) -> int:
    return len(key) + len(value) + ENTRY_OVERHEAD_BYTES


def recompute(store: KeyValueStore) -> int:
    """The memory total worked out from scratch, to check the running one."""
    return sum(cost(key, value) for key, value in store._data.items())


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def store(clock: FakeClock) -> KeyValueStore:
    return KeyValueStore(clock=clock)


def bounded(limit: int, policy: MaxmemoryPolicy, clock: FakeClock) -> KeyValueStore:
    return KeyValueStore(clock=clock, maxmemory=limit, policy=policy)


# --------------------------------------------------------------------------
# Accounting
# --------------------------------------------------------------------------


def test_an_empty_store_uses_nothing(store: KeyValueStore) -> None:
    assert store.memory_used == 0


def test_a_key_costs_its_bytes_plus_a_fixed_overhead(store: KeyValueStore) -> None:
    store.set(b"key", b"value")

    assert store.memory_used == 3 + 5 + ENTRY_OVERHEAD_BYTES


def test_costs_accumulate_across_keys(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"bb", b"22")

    assert store.memory_used == cost(b"a", b"1") + cost(b"bb", b"22")


def test_binary_keys_and_values_are_counted_by_byte_length(store: KeyValueStore) -> None:
    key = b"\x00\xff\r\n"
    value = b"\x89PNG\x00"

    store.set(key, value)

    assert store.memory_used == 4 + 5 + ENTRY_OVERHEAD_BYTES


def test_overwriting_with_a_longer_value_grows_the_total(store: KeyValueStore) -> None:
    store.set(b"k", b"v")
    before = store.memory_used

    store.set(b"k", b"longer")

    assert store.memory_used == before + 5


def test_overwriting_with_a_shorter_value_shrinks_the_total(store: KeyValueStore) -> None:
    store.set(b"k", b"longer")
    before = store.memory_used

    store.set(b"k", b"v")

    assert store.memory_used == before - 5


def test_incr_growing_a_value_costs_one_more_byte(store: KeyValueStore) -> None:
    store.set(b"counter", b"9")
    before = store.memory_used

    store.incr(b"counter")

    assert store.memory_used == before + 1


def test_incr_shrinking_a_value_frees_a_byte(store: KeyValueStore) -> None:
    store.set(b"counter", b"-10")
    before = store.memory_used

    store.incr(b"counter")

    assert store.memory_used == before - 1


def test_a_new_counter_costs_a_whole_entry(store: KeyValueStore) -> None:
    store.incr(b"counter")

    assert store.memory_used == cost(b"counter", b"1")


def test_delete_frees_the_entry(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    store.delete(b"a")

    assert store.memory_used == cost(b"b", b"2")


def test_flushdb_frees_everything(store: KeyValueStore) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"2")

    store.flushdb()

    assert store.memory_used == 0


def test_an_expired_key_stops_counting_once_reclaimed(
    store: KeyValueStore, clock: FakeClock
) -> None:
    store.set(b"k", b"v", ttl_ms=1000)
    clock.advance(1000)

    store.drop_expired()

    assert store.memory_used == 0


# --------------------------------------------------------------------------
# Deadlines are outside the model
# --------------------------------------------------------------------------


def test_a_timed_write_costs_the_same_as_an_untimed_one(clock: FakeClock) -> None:
    timed = KeyValueStore(clock=clock)
    untimed = KeyValueStore(clock=clock)

    timed.set(b"k", b"v", ttl_ms=60_000)
    untimed.set(b"k", b"v")

    assert timed.memory_used == untimed.memory_used


def test_expire_does_not_change_the_total(store: KeyValueStore) -> None:
    store.set(b"k", b"v")
    before = store.memory_used

    store.expire(b"k", 60_000)

    assert store.memory_used == before


def test_persist_does_not_change_the_total(store: KeyValueStore) -> None:
    store.set(b"k", b"v", ttl_ms=60_000)
    before = store.memory_used

    store.persist(b"k")

    assert store.memory_used == before


def test_expire_is_never_refused_for_want_of_memory(clock: FakeClock) -> None:
    # Deadlines cost nothing under the model, so a full keyspace can still
    # have expirations attached to it.
    store = bounded(cost(b"k", b"v"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"k", b"v")

    assert store.expire(b"k", 60_000) is True
    assert store.ttl(b"k") == (True, 60_000)


# --------------------------------------------------------------------------
# The running total is exact
# --------------------------------------------------------------------------


def test_the_running_total_matches_a_recomputation(
    store: KeyValueStore, clock: FakeClock
) -> None:
    store.set(b"a", b"1")
    store.set(b"b", b"22")
    store.set(b"a", b"333")
    store.incr(b"counter")
    store.incr(b"counter")
    store.set(b"timed", b"value", ttl_ms=1000)
    store.expire(b"b", 5000)
    store.persist(b"b")
    store.delete(b"a")
    store.set(b"restored", b"x")
    store.discard(b"restored")
    store.restore(b"replayed", b"value")
    clock.advance(1000)
    store.drop_expired()

    assert store.memory_used == recompute(store)


def test_the_running_total_returns_to_zero(store: KeyValueStore) -> None:
    for index in range(20):
        store.set(b"key%d" % index, b"value%d" % index)
    for index in range(20):
        store.delete(b"key%d" % index)

    assert store.memory_used == 0


# --------------------------------------------------------------------------
# Unlimited mode
# --------------------------------------------------------------------------


def test_nothing_is_ever_refused_or_evicted_without_a_limit(
    store: KeyValueStore,
) -> None:
    for index in range(500):
        store.set(b"key%d" % index, b"value")

    assert store.dbsize() == 500
    assert store.take_evicted() == []
    assert store.memory_used == recompute(store)


# --------------------------------------------------------------------------
# noeviction
# --------------------------------------------------------------------------


def test_a_write_that_exactly_reaches_the_limit_is_admitted(clock: FakeClock) -> None:
    store = bounded(cost(b"key", b"value"), MaxmemoryPolicy.NOEVICTION, clock)

    store.set(b"key", b"value")

    assert store.memory_used == store._maxmemory


def test_one_byte_past_the_limit_is_refused(clock: FakeClock) -> None:
    store = bounded(cost(b"key", b"value"), MaxmemoryPolicy.NOEVICTION, clock)

    with pytest.raises(OutOfMemoryError):
        store.set(b"key", b"value!")


def test_a_refused_write_changes_nothing(clock: FakeClock) -> None:
    store = bounded(cost(b"key", b"value"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"key", b"value", ttl_ms=60_000)
    before = store.memory_used

    with pytest.raises(OutOfMemoryError):
        store.set(b"key", b"a much longer value")

    assert store.get(b"key") == b"value"
    assert store.ttl(b"key") == (True, 60_000)
    assert store.memory_used == before
    assert store.dbsize() == 1


def test_a_second_key_is_refused_when_the_first_fills_the_limit(
    clock: FakeClock,
) -> None:
    store = bounded(cost(b"a", b"1"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"a", b"1")

    with pytest.raises(OutOfMemoryError):
        store.set(b"b", b"2")

    assert store.dbsize() == 1
    assert store.take_evicted() == []


def test_a_shorter_overwrite_is_admitted_at_the_limit(clock: FakeClock) -> None:
    store = bounded(cost(b"k", b"12345"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"k", b"12345")

    store.set(b"k", b"1")

    assert store.get(b"k") == b"1"


def test_incr_is_refused_when_the_longer_value_will_not_fit(clock: FakeClock) -> None:
    store = bounded(cost(b"c", b"9"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"c", b"9")

    with pytest.raises(OutOfMemoryError):
        store.incr(b"c")

    assert store.get(b"c") == b"9"


def test_incr_that_shortens_a_value_is_admitted_at_the_limit(clock: FakeClock) -> None:
    store = bounded(cost(b"c", b"-10"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"c", b"-10")

    assert store.incr(b"c") == -9


def test_delete_and_flushdb_are_always_admitted(clock: FakeClock) -> None:
    store = bounded(cost(b"a", b"1"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"a", b"1")

    assert store.delete(b"a") == 1
    store.set(b"a", b"1")
    store.flushdb()

    assert store.memory_used == 0


def test_freeing_memory_makes_room_again(clock: FakeClock) -> None:
    store = bounded(cost(b"a", b"1"), MaxmemoryPolicy.NOEVICTION, clock)
    store.set(b"a", b"1")
    store.delete(b"a")

    store.set(b"b", b"2")

    assert store.get(b"b") == b"2"


# --------------------------------------------------------------------------
# allkeys-lru
# --------------------------------------------------------------------------


# Every key in these tests is the same width, so one entry always costs the
# same and a budget can be stated as a whole number of entries.
FIRST = b"first_"
SECOND = b"second"
THIRD = b"third_"
VALUE = b"value"
ENTRY = cost(FIRST, VALUE)


def two_key_store(clock: FakeClock) -> KeyValueStore:
    """A store with room for exactly two entries of equal size."""
    return bounded(2 * ENTRY, MaxmemoryPolicy.ALLKEYS_LRU, clock)


def test_a_write_over_the_limit_evicts_instead_of_failing(clock: FakeClock) -> None:
    store = two_key_store(clock)
    store.set(FIRST, VALUE)
    store.set(SECOND, VALUE)

    store.set(THIRD, VALUE)

    assert store.get(THIRD) == VALUE
    assert store.dbsize() == 2
    assert store.memory_used <= 2 * ENTRY


def test_the_least_recently_used_key_is_the_victim(clock: FakeClock) -> None:
    store = two_key_store(clock)
    store.set(FIRST, VALUE)
    store.set(SECOND, VALUE)

    store.set(THIRD, VALUE)

    assert store.get(FIRST) is None
    assert store.get(SECOND) == VALUE


def test_reading_a_key_saves_it_from_eviction(clock: FakeClock) -> None:
    store = two_key_store(clock)
    store.set(FIRST, VALUE)
    store.set(SECOND, VALUE)

    store.get(FIRST)  # FIRST is now the more recently used of the two
    store.set(THIRD, VALUE)

    assert store.get(FIRST) == VALUE
    assert store.get(SECOND) is None


def test_writing_a_key_saves_it_from_eviction(clock: FakeClock) -> None:
    store = two_key_store(clock)
    store.set(FIRST, VALUE)
    store.set(SECOND, VALUE)

    store.set(FIRST, VALUE)
    store.set(THIRD, VALUE)

    assert store.get(FIRST) == VALUE
    assert store.get(SECOND) is None


def test_incrementing_a_key_saves_it_from_eviction(clock: FakeClock) -> None:
    store = bounded(2 * cost(FIRST, b"1"), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    store.set(FIRST, b"1")
    store.set(SECOND, b"1")

    store.incr(FIRST)
    store.set(THIRD, b"1")

    assert store.get(FIRST) == b"2"
    assert store.get(SECOND) is None


@pytest.mark.parametrize("inspect", ["exists", "ttl"])
def test_inspecting_a_key_does_not_save_it(clock: FakeClock, inspect: str) -> None:
    # EXISTS and TTL are introspection, not use: they must not reorder anything.
    store = two_key_store(clock)
    store.set(FIRST, VALUE)
    store.set(SECOND, VALUE)

    if inspect == "exists":
        store.exists(FIRST)
    else:
        store.ttl(FIRST)
    store.set(THIRD, VALUE)

    assert store.get(FIRST) is None
    assert store.get(SECOND) == VALUE


def test_managing_a_deadline_does_not_save_a_key(clock: FakeClock) -> None:
    store = two_key_store(clock)
    store.set(FIRST, VALUE)
    store.set(SECOND, VALUE)

    store.expire(FIRST, 60_000)
    store.persist(FIRST)
    store.set(THIRD, VALUE)

    assert store.get(FIRST) is None


def test_the_key_being_written_is_never_its_own_victim(clock: FakeClock) -> None:
    store = bounded(cost(b"k", b"12345"), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    store.set(b"k", b"1")

    store.set(b"k", b"12345")

    assert store.get(b"k") == b"12345"
    assert store.take_evicted() == []


def test_one_write_can_evict_several_keys(clock: FakeClock) -> None:
    store = bounded(6 * cost(b"k0", b"v"), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    for index in range(6):
        store.set(b"k%d" % index, b"v")

    store.set(b"big", b"v" * (4 * cost(b"k0", b"v")))
    evicted = store.take_evicted()

    assert len(evicted) >= 3
    assert store.get(b"big") is not None
    assert store.memory_used <= store._maxmemory


def test_eviction_stops_as_soon_as_the_write_fits(clock: FakeClock) -> None:
    store = bounded(3 * cost(b"k0", b"v"), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    store.set(b"k0", b"v")
    store.set(b"k1", b"v")
    store.set(b"k2", b"v")

    store.set(b"k3", b"v")

    assert len(store.take_evicted()) == 1


def test_a_value_too_large_for_an_empty_keyspace_is_refused(clock: FakeClock) -> None:
    # The atomicity guarantee: it must refuse without destroying anything on
    # the way to discovering it could never have fitted.
    store = bounded(4 * cost(b"k0", b"v"), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    for index in range(4):
        store.set(b"k%d" % index, b"v")
    before = store.memory_used

    with pytest.raises(OutOfMemoryError):
        store.set(b"huge", b"v" * 10_000)

    assert store.dbsize() == 4
    assert store.memory_used == before
    assert store.take_evicted() == []


def test_evicted_keys_are_reported_once(clock: FakeClock) -> None:
    store = two_key_store(clock)
    store.set(FIRST, VALUE)
    store.set(SECOND, VALUE)

    store.set(THIRD, VALUE)

    assert store.take_evicted() == [FIRST]
    assert store.take_evicted() == []


def test_eviction_keeps_the_memory_total_exact(clock: FakeClock) -> None:
    store = bounded(10 * cost(b"k00", b"value"), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    for index in range(40):
        store.set(b"k%02d" % index, b"value")

    assert store.memory_used == recompute(store)
    assert store.memory_used <= store._maxmemory


def test_binary_keys_can_be_evicted(clock: FakeClock) -> None:
    store = bounded(2 * cost(b"\x00\xff\r", VALUE), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    store.set(b"\x00\xff\r", VALUE)
    store.set(b"\xfe\r\n", VALUE)

    store.set(b"\x01\x02\x03", VALUE)

    assert store.take_evicted() == [b"\x00\xff\r"]


def test_sampling_eventually_reaches_every_key(clock: FakeClock) -> None:
    # More keys than one sample can hold, so the cursor has to come back round.
    count = MAXMEMORY_SAMPLES * 4
    store = bounded(count * cost(b"k00", b"v"), MaxmemoryPolicy.ALLKEYS_LRU, clock)
    for index in range(count):
        store.set(b"k%02d" % index, b"v")

    for index in range(count):
        store.set(b"n%02d" % index, b"v")

    assert store.dbsize() == count
    assert store.memory_used <= store._maxmemory


# --------------------------------------------------------------------------
# Expiration comes first
# --------------------------------------------------------------------------


def test_expired_keys_are_reclaimed_before_live_ones_are_evicted(
    clock: FakeClock,
) -> None:
    store = bounded(3 * ENTRY, MaxmemoryPolicy.ALLKEYS_LRU, clock)
    store.set(b"doomed", VALUE, ttl_ms=1000)
    store.set(b"live-1", VALUE)
    store.set(b"live-2", VALUE)
    clock.advance(1000)

    store.set(b"fresh_", VALUE)

    assert store.take_evicted() == [], "a live key was evicted over expired garbage"
    assert store.get(b"live-1") == VALUE
    assert store.get(b"live-2") == VALUE
    assert store.get(b"fresh_") == VALUE


def test_an_expired_key_is_never_chosen_as_a_victim(clock: FakeClock) -> None:
    store = bounded(2 * ENTRY, MaxmemoryPolicy.ALLKEYS_LRU, clock)
    store.set(b"expire", VALUE, ttl_ms=1000)
    store.set(b"live__", VALUE)
    clock.advance(1000)

    store.set(b"new___", VALUE)

    assert store.take_evicted() == []
    assert store.dbsize() == 2


def test_reclaiming_expired_keys_can_avoid_a_refusal_entirely(
    clock: FakeClock,
) -> None:
    store = bounded(ENTRY, MaxmemoryPolicy.ALLKEYS_LRU, clock)
    store.set(FIRST, VALUE, ttl_ms=1000)
    clock.advance(1000)

    store.set(SECOND, VALUE)

    assert store.get(SECOND) == VALUE
    assert store.memory_used == ENTRY
