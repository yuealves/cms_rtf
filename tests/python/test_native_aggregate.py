"""Differential acceptance against the existing Python factor contract."""
import os
from pathlib import Path
import resource
import signal
import subprocess
import tempfile
import unittest

import numpy as np
import pyarrow as pa

from cms_rtf import aggregate, native_aggregate
from cms_rtf.catalog import endpoint_schedule_ms
from cms_rtf.validate_replay import exact_factors


class NativeAggregateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.binary = Path(os.environ.get('CMS_RTF_TEST_AGGREGATOR', native_aggregate.DEFAULT_BINARY))
        if not self.binary.exists():
            self.skipTest('build cms_index500ms_aggregate first')
        self.grid = endpoint_schedule_ms()
        self.ref = dict(symbols=np.array([1, 2, 3, 600000], dtype=np.int32),
                        weights=np.array([[.8, .1, .05, .05], [.0, .7, .0, .3],
                                          [.1, .2, .3, .4], [.0, .0, .0, .0]]))
        self.ref['official'] = self.ref['weights'].sum(axis=1)
        self.paths = {}
        for kind, dtype in [('book', aggregate.BOOK_REC), ('flow', aggregate.FLOW_REC),
                            ('seeds', aggregate.SEED_REC)]:
            self.paths[kind] = [self.root / f'w0.{kind}.bin']
            np.zeros(0, dtype=dtype).tofile(self.paths[kind][0])
        self.reference = self.root / 'reference.bin'
        native_aggregate.write_reference(self.ref, self.reference)
        self.output = self.root / 'cpp.arrow'

    def command(self):
        return native_aggregate.command(self.paths['book'], self.paths['flow'], self.paths['seeds'],
                                         self.reference, '20261009', self.output, self.binary)

    def compare(self):
        with np.errstate(invalid='ignore', divide='ignore', over='ignore'):
            grid, rows = aggregate.aggregate(self.paths['book'], self.paths['flow'],
                                              self.paths['seeds'], self.ref, '20261009')
            aggregate.write_arrow(grid, rows, '20261009', self.root / 'python.arrow')
        subprocess.run(self.command(), capture_output=True, check=True, timeout=30)
        check = exact_factors(pa.ipc.open_stream(self.root / 'python.arrow').read_all(),
                              pa.ipc.open_stream(self.output).read_all())
        self.assertTrue(check['keys_equal'] and check['keys_unique'])
        self.assertEqual(check['exact_columns'], 106,
                         {k: v for k, v in check['columns'].items() if not v['exact']})

    def populate(self):
        # Missing symbol 3, an empty worker, invalid mids, per-column NaNs, seeds
        # that must not fall back, exact coverage gate, gaps and both sessions.
        for kind in self.paths:
            self.paths[kind] = [self.root / f'w{i}.{kind}.bin' for i in range(3)]
        for i, symbols in enumerate([[600000], [1, 2], []]):
            books, flows, seeds = [], [], []
            for symbol in symbols:
                seeds.append((symbol, np.nan if symbol == 2 else 9.995))
                for pos, mid in [(0, 10.), (2, np.nan), (3, 10.025), (14400, 10.1), (28799, 9.999)]:
                    feat = np.arange(31, dtype=np.float64) * (symbol / 23.7)
                    feat[4] = np.nan if symbol == 600000 else feat[4]
                    feat[7] = np.inf if symbol == 2 else feat[7]
                    feat[9] = -0.0
                    books.append((symbol, self.grid[pos], feat, mid,
                                  int(self.grid[pos]) - (500 if symbol == 1 else 499), 1, (0,) * 7))
                for pos in [0, 3, 3, 2, 14400, 28799]:
                    bases = np.arange(21, dtype=np.float64) * (symbol / 7.9 + pos)
                    flows.append((symbol, self.grid[pos], bases))
            for kind, dtype, rows in [('book', aggregate.BOOK_REC, books),
                                      ('flow', aggregate.FLOW_REC, flows),
                                      ('seeds', aggregate.SEED_REC, seeds)]:
                np.array(rows, dtype=dtype).tofile(self.paths[kind][i])

    def test_empty_workers_and_zero_official(self):
        self.compare()

    def test_sparse_boundaries_nulls_duplicate_flows_and_symbol_merge(self):
        self.populate()
        self.compare()
        # File order does not affect fixed-symbol accumulation for distinct symbols.
        for paths in self.paths.values():
            paths.reverse()
        self.compare()

    def test_repeated_symbol_groups_preserve_file_order(self):
        self.populate()
        for kind in self.paths:
            self.paths[kind].append(self.paths[kind][0])
        self.compare()

    def test_truncated_inputs_preserve_previous_output(self):
        self.populate()
        self.output.write_bytes(b'previous accepted output')
        for path in [self.reference] + [self.paths[k][0] for k in self.paths]:
            with self.subTest(path=path.name):
                original = path.read_bytes()
                path.write_bytes(original[:-1])
                result = subprocess.run(self.command(), capture_output=True, timeout=30)
                self.assertNotEqual(result.returncode, 0)
                self.assertEqual(self.output.read_bytes(), b'previous accepted output')
                path.write_bytes(original)

    def test_bad_version_date_and_symbol_order_rejected(self):
        self.reference.write_bytes(b'oldmagic' + self.reference.read_bytes()[8:])
        self.assertNotEqual(subprocess.run(self.command(), capture_output=True).returncode, 0)
        native_aggregate.write_reference(self.ref, self.reference)
        cmd = self.command()
        cmd[cmd.index('--day') + 1] = '20260230'
        self.assertNotEqual(subprocess.run(cmd, capture_output=True).returncode, 0)
        self.populate()
        path = self.paths['book'][1]
        records = np.fromfile(path, dtype=aggregate.BOOK_REC)
        records[::-1].tofile(path)
        self.assertNotEqual(subprocess.run(self.command(), capture_output=True).returncode, 0)

    def test_write_failure_preserves_previous_output(self):
        self.output.write_bytes(b'previous accepted output')

        def small_file_limit():
            signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
            resource.setrlimit(resource.RLIMIT_FSIZE, (1024, 1024))

        result = subprocess.run(self.command(), capture_output=True,
                                preexec_fn=small_file_limit, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(self.output.read_bytes(), b'previous accepted output')
        self.assertEqual(list(self.root.glob('cpp.arrow.tmp.*')), [])

    def test_invalid_reference_rejected_before_execution(self):
        self.ref['weights'][0, 0] = np.nan
        with self.assertRaises(ValueError):
            native_aggregate.write_reference(self.ref, self.reference)


if __name__ == '__main__':
    unittest.main()
