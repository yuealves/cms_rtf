"""Three-way full-day replay validation; exits nonzero on acceptance failure.

Float32 equality means identical validity masks and identical bits at valid rows.
Bytes under Arrow nulls are unspecified and are deliberately not compared.
"""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .catalog import COLUMN_NAMES
from .compare import _load, compare, write_report


def bitwise_column(a, b):
    if a.type != pa.float32() or b.type != pa.float32():
        raise ValueError('factor columns must be Float32')
    if len(a) != len(b):
        raise ValueError('row count mismatch')
    a, b = a.combine_chunks(), b.combine_chunks()
    av = a.is_valid().to_numpy(zero_copy_only=False)
    bv = b.is_valid().to_numpy(zero_copy_only=False)
    both = av & bv
    # Read the actual value buffers: preserve signed zero and NaN payload bits.
    ab = np.frombuffer(a.buffers()[1], dtype='<u4', count=len(a), offset=a.offset * 4)
    bb = np.frombuffer(b.buffers()[1], dtype='<u4', count=len(b), offset=b.offset * 4)
    mask_diff = int(np.count_nonzero(av != bv))
    bit_diff = int(np.count_nonzero(ab[both] != bb[both]))
    return dict(null_mask_mismatches=mask_diff, valid_bit_mismatches=bit_diff,
                nulls=int(np.count_nonzero(~av)), exact=mask_diff == bit_diff == 0)


def exact_factors(a, b):
    expected = ['datetime', 'code'] + list(COLUMN_NAMES)
    if a.column_names != expected or b.column_names != expected:
        raise ValueError('factor column catalog/order mismatch')
    if not a.schema.equals(b.schema, check_metadata=False):
        raise ValueError('factor schema mismatch')
    age_key = b'cms_rtf.local_age_semantics'
    legacy_age = b'wall_time_v1'
    if ((a.schema.metadata or {}).get(age_key, legacy_age) !=
            (b.schema.metadata or {}).get(age_key, legacy_age)):
        raise ValueError('local snapshot_age_seconds semantics mismatch (wall time vs trading time)')
    if a.num_rows != b.num_rows:
        raise ValueError('factor row count mismatch')
    keys_equal = all(a[k].equals(b[k]) for k in ('datetime', 'code'))
    keys = list(zip(a['datetime'].to_pylist(), a['code'].to_pylist()))
    keys_unique = len(keys) == len(set(keys))
    columns = {name: bitwise_column(a[name], b[name]) for name in COLUMN_NAMES}
    return dict(rows=a.num_rows, keys_equal=keys_equal, keys_unique=keys_unique,
                exact_columns=sum(c['exact'] for c in columns.values()),
                total_columns=len(COLUMN_NAMES), columns=columns)


def parquet_inventory(directory):
    directory = Path(directory)
    manifest = json.loads((directory / 'manifest.json').read_text())
    summary = json.loads((directory / 'summary.json').read_text())
    entries = {e['file']: e for e in manifest['files']}
    if len(entries) != len(manifest['files']) or set(entries) != {f.name for f in directory.glob('*.parquet')}:
        raise ValueError(f'manifest/shard inventory mismatch: {directory}')
    counts = {}
    for kind in ('order', 'trade'):
        files = sorted(directory.glob(f'*_{kind}_data_*.parquet'))
        counts[kind] = dict(files=len(files), rows=sum(
            pq.ParquetFile(f).metadata.num_rows for f in files))
        for f in files:
            if (entries[f.name]['kind'] != kind
                    or entries[f.name]['rows'] != pq.ParquetFile(f).metadata.num_rows
                    or entries[f.name]['size'] != f.stat().st_size):
                raise ValueError(f'manifest/footer mismatch: {f}')
            if (pq.read_schema(f).metadata or {}).get(b'cms_schema') != b'cms-parquet-v2':
                raise ValueError(f'not a v2 shard: {f}')
    rows = sum(c['rows'] for c in counts.values())
    if manifest['schema'] != 'cms-parquet-v2' or manifest['status'] != 'verified':
        raise ValueError(f'unverified v2 archive: {directory}')
    if rows != summary['rows'] or rows != manifest['summary']['rows']:
        raise ValueError(f'footer/summary/manifest row mismatch: {directory}')
    # Exporter summary uses v1 categories (SZ withdrawals under order), whereas
    # v2 shard entries count those withdrawals under trade.
    market_counts = {f'{m}_{k}': sum(e[m] for e in entries.values() if e['kind'] == k)
                     for m in ('SH', 'SZ') for k in ('order', 'trade')}
    if sum(market_counts.values()) != rows:
        raise ValueError(f'manifest market counts mismatch: {directory}')
    return dict(path=str(directory.resolve()), rows=rows, shards=counts, market_counts=market_counts,
                summary=summary)


def sha256(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('replay', 'direct', 'reference', 'source-parquet', 'replay-parquet', 'out'):
        p.add_argument('--' + name, required=True, type=Path)
    p.add_argument('--expected-events', type=int, default=535706255)
    p.add_argument('--expected-factor-rows', type=int, default=115200)
    p.add_argument('--expected-corr-columns', type=int, default=104)
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    source, replay = (parquet_inventory(x) for x in
                      (args.source_parquet, args.replay_parquet))
    exact = exact_factors(_load(args.replay), _load(args.direct))
    reference_keys = _load(args.reference).select(['datetime', 'code'])
    direct_keys = _load(args.direct).select(['datetime', 'code'])
    # This acceptance run requires identical grids, preventing partial joins or
    # duplicate keys from silently making reference correlations look better.
    if not direct_keys.equals(reference_keys):
        raise ValueError('HT reference grid differs from the direct factor grid')
    ht = {}
    for name, path in [('direct', args.direct), ('replay', args.replay)]:
        report = compare(path, args.reference)
        write_report(report, args.out / f'{name}_vs_reference.json')
        high = [n for n, c in report['columns'].items()
                if c['corr'] is not None and c['corr'] > 0.99]
        ht[name] = dict(corr_gt_099=len(high), columns=high,
                        exceptions={n: c for n, c in report['columns'].items() if n not in high})
    end = replay['summary']['session_boundaries'][-1]
    checks = dict(
        event_rows=source['rows'] == replay['rows'] == args.expected_events,
        kind_rows=all(source['shards'][k]['rows'] == replay['shards'][k]['rows']
                      for k in ('order', 'trade')),
        market_rows=source['market_counts'] == replay['market_counts'],
        closed_journal=end['kind'] == 'END' and end['flags'] == 0
                       and end['queue_drops'] == 0
                       and not replay['summary']['input_incomplete'],
        factor_rows=exact['rows'] == args.expected_factor_rows,
        factor_keys=exact['keys_equal'] and exact['keys_unique'],
        factor_bits=exact['exact_columns'] == len(COLUMN_NAMES),
        reference_correlations=ht['direct']['columns'] == ht['replay']['columns']
                               and ht['replay']['corr_gt_099'] == args.expected_corr_columns)
    result = dict(passed=all(checks.values()), checks=checks, source=source, replay=replay,
                  exact=exact, reference=ht,
                  factors={n: dict(path=str(f.resolve()), sha256=sha256(f)) for n, f in
                           [('replay', args.replay), ('direct', args.direct), ('reference', args.reference)]})
    write_report(result, args.out / 'validation.json')
    print(json.dumps(dict(passed=result['passed'], checks=checks,
                          exact_columns=exact['exact_columns'],
                          corr_gt_099=ht['replay']['corr_gt_099']), indent=2))
    raise SystemExit(0 if result['passed'] else 1)


if __name__ == '__main__':
    main()
