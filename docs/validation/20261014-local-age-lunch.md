# Local snapshot age excludes the lunch break

User-approved CMS-only semantic change on 2026-10-14. Huatai source
`546aba2fa733d4f7f7656af0572115287418b12d`, `worker.cpp:411–447`, uses elapsed
receive wall time. The new local age subtracts the intersection of that interval
with 11:30–13:00, per stock before the existing weighted aggregation. Exchange
age and receive-time freshness retain their original definitions.

## Regression coverage

- Runner tests cover no previous event, unchanged morning age, an unchanged book
  aging across lunch, a receive update during lunch, the exact session boundary,
  and freshness one nanosecond inside the 500ms boundary.
- Journal integration replays the same complete synthetic day with one and eight
  workers. Two equally weighted stocks have last receives at 11:29:59 and 12:15;
  the second stock updates again at 13:00:06.100. All four indices are checked.
- At 13:00:06 the new local age is `(7 + 6) / 2 = 6.5s`; exchange age remains
  `(5407 + 2706) / 2 = 4056.5s`. At 13:00:06.500 local age is 3.95s and fresh
  weight is 0.5; at 13:00:07 local age is 4.45s and fresh weight is zero.
- New local Arrow streams carry schema metadata
  `cms_rtf.local_age_semantics=trading_time_excluding_lunch_v2`. Exchange output
  metadata is unchanged. Exact validation rejects a comparison against legacy
  wall-time age even if the supplied numeric cells happen to match.

## Before/after migration audit

The eight-worker synthetic journal was replayed using the saved pre-change
binary (`548871f`) and the new binary, each producing 115,200 rows per clock.
Exchange: all 106 columns match bitwise, including null masks. Local: the only
changed column is `snapshot_age_seconds`; the other 105 columns match bitwise,
and morning age is unchanged. This audit intentionally removes the new local
metadata only for the explicit migration comparison.

The local audit artifacts are under the ignored directory
`work/local-age-lunch-20261014/`: `compare_before_after.py`,
`migration-check.json`, `ctest.log`, and `python-tests.log`. This is a targeted
semantic regression, not a new full-market latency or memory benchmark.

Required checks are `ctest --test-dir build --output-on-failure` and
`PYTHONPATH=python python3 -m unittest discover -s tests/python -v`.
Local results: all six CTest targets passed; the Python suite completed 60 tests
in 170.3 seconds with no failures (one existing pidfd lifecycle test skipped
because that API is unavailable in the local Python environment).
`scripts/push_online.py` also compiles and repeats both suites on CMS before
atomically changing the active source symlink. No service restart is required
for the next scheduled daily start.
