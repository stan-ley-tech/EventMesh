from datetime import datetime, timedelta, timezone

from eventmesh_broker import timeutil


def test_naive_datetime_is_treated_as_utc_not_local_time():
    naive = datetime(2026, 1, 1, 12, 0, 0)
    aware_utc = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    assert timeutil.fmt(naive) == timeutil.fmt(aware_utc)


def test_aware_datetime_in_another_timezone_converts_correctly():
    tz = timezone(timedelta(hours=5))
    aware = datetime(2026, 1, 1, 17, 0, 0, tzinfo=tz)
    expected_utc = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    assert timeutil.fmt(aware) == timeutil.fmt(expected_utc)
