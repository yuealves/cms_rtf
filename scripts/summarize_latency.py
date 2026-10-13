#!/usr/bin/env python3
"""Summarize CMS latency.csv or Huatai-compatible latency.log (all values in ms).

Read-only: safe against an incomplete final line in a running service's log.
Keep replay/catch-up/tail and normal/timer/EOF closes in separate groups.
"""
import argparse
from collections import defaultdict
import csv
import json
import math
from pathlib import Path


def in_session(hms):
    return '09:30:00.000' < hms <= '11:30:00.000' or '13:00:00.000' < hms <= '15:00:00.000'


def stats(values, threshold):
    x = sorted(values)
    if not x:
        return {'count': 0}
    result = dict(count=len(x), min=min(x), mean=sum(x) / len(x), max=max(x),
                  negative=sum(v < 0 for v in x), over_threshold=sum(v > threshold for v in x))
    for name, q in [('p50', .5), ('p95', .95), ('p99', .99), ('p999', .999)]:
        result[name] = x[int(q * (len(x) - 1))]
    return result


def summarize(path, phase=None, reason=None, session='trading', include_empty=False, threshold=10.):
    path = Path(path)
    if path.is_dir(): path = path / 'latency.csv'
    with path.open() as f:
        lines = [line for line in f if line.endswith('\n')]
    detailed = path.suffix == '.csv'
    if detailed:
        rows = list(csv.DictReader(lines))
        metadata = json.loads(path.with_name('latency.meta.json').read_text())
        if metadata['schema'] != 'cms-latency-v1' or metadata['unit'] != 'ms':
            raise ValueError('unsupported latency schema/unit')
    else:
        metadata = dict(unit='ms', source='two-column close log; no receive timestamps or phase labels')
        rows = [dict(endpoint=s[0], close_to_write_ms=s[1], phase='unknown', reason='unknown', mode='unknown')
                for line in lines if len(s := line.split()) == 2]
    groups = defaultdict(list)
    skipped = defaultdict(int)
    for row in rows:
        if session == 'trading' and not in_session(row['endpoint']):
            skipped['outside_session'] += 1; continue
        if phase and row['phase'] != phase or reason and row['reason'] != reason:
            skipped['phase_or_reason'] += 1; continue
        if detailed and not include_empty and int(row['events']) == 0:
            skipped['empty_window'] += 1; continue
        groups[(row['mode'], row['phase'], row['reason'])].append(row)
    result = []
    for (mode, group_phase, group_reason), group in groups.items():
        entry = dict(mode=mode, phase=group_phase, reason=group_reason, windows=len(group), metrics={})
        if detailed:
            entry.update(distinct_closes=len({r['close_id'] for r in group}),
                         distinct_flushes=len({r['flush_id'] for r in group}))
        for key in ['close_to_write_ms', 'last_read_to_write_ms', 'last_recv_to_write_ms']:
            values = [float(r[key]) for r in group if r.get(key, '') != '']
            if not all(math.isfinite(x) for x in values): raise ValueError('nonfinite latency')
            entry['metrics'][key] = stats(values, threshold)
        result.append(entry)
    return dict(path=str(path), unit='ms', threshold_ms=threshold, metadata=metadata,
                filters=dict(phase=phase, reason=reason, session=session, include_empty=include_empty),
                skipped=dict(skipped), groups=result)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('path', type=Path)
    p.add_argument('--phase', choices=['tail', 'catchup', 'replay'])
    p.add_argument('--reason', choices=['watermark', 'am_timer', 'pm_timer', 'eof'])
    p.add_argument('--session', choices=['trading', 'all'], default='trading')
    p.add_argument('--include-empty', action='store_true')
    p.add_argument('--threshold-ms', type=float, default=10.)
    a = p.parse_args()
    print(json.dumps(summarize(a.path, a.phase, a.reason, a.session, a.include_empty, a.threshold_ms), indent=2))


if __name__ == '__main__': main()
