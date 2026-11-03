"""Tests for the state a source keeps about a course it read through Tika."""

from datetime import UTC, datetime

from ol_dlt.extraction import needs_read, record_read


def _at(day: int, hour: int = 4, minute: int = 35) -> datetime:
    return datetime(2026, 10, day, hour, minute, tzinfo=UTC)


def test_failing_course_is_read_again_on_a_bounded_number_of_days() -> None:
    state = record_read(None, "v1", _at(1, 23, 59), retry=True)
    assert not needs_read(state, "v1", _at(1, 23, 59))
    # Not again in a run that crosses midnight.
    assert not needs_read(state, "v1", _at(2, 0, 1))
    assert needs_read(state, "v1", _at(2, 23, 59))
    assert needs_read(state, "v2", _at(1, 23, 59))

    for day in (3, 4):
        state = record_read(state, "v1", _at(day), retry=True)
    assert state == "v1|3|2026-10-04T04:35:00+00:00"
    assert not needs_read(state, "v1", _at(5))

    assert record_read(state, "v1", _at(5), retry=False) == "v1"
    assert not needs_read("v1", "v1", _at(6))
    # A new version starts its attempts over.
    assert record_read(state, "v2", _at(5), retry=True) == (
        "v2|1|2026-10-05T04:35:00+00:00"
    )


def test_a_course_with_an_empty_mark_is_read_when_it_comes_back() -> None:
    mark = "!empty|2026-10-01T23:59:00+00:00"
    assert needs_read(mark, "v2", _at(1, 23, 59))
    assert record_read(mark, "v2", _at(2), retry=True) == (
        "v2|1|2026-10-02T04:35:00+00:00"
    )
