import json
from pathlib import Path
import struct
import subprocess
import unittest

import numpy as np

from cms_rtf import native_aggregate
from cms_rtf.record_blocks import logical_digest
import test_journal_pipeline as pipeline_fixture


class RecordBlocksTest(unittest.TestCase):
    setUp = pipeline_fixture.PipelineTest.setUp

    def kernel_run(self, mode, workers=1, pipe=False):
        out = self.root / f'{mode}_{workers}_{pipe}'
        out.mkdir()
        cmd = [str(self.kernel), '--thresh', str(self.root / 'thresh'), '--work-dir', str(out),
               '--workers', str(workers), '--output-mode', mode, '--input', '-' if pipe else str(self.root / 'input')]
        result = subprocess.run(cmd, input=self.data if pipe else None, capture_output=True, check=True, timeout=30)
        return out, json.loads(result.stdout)

    def test_blocks_reconstruct_legacy_and_are_written_before_eof(self):
        old, a = self.kernel_run('legacy')
        new, b = self.kernel_run('blocks', pipe=True)
        for kind, size in [('book', 280), ('flow', 176)]:
            self.assertEqual(logical_digest(old / f'w0.{kind}.bin', size),
                             logical_digest(new / f'w0.{kind}.cmsb', size, expand_books=kind == 'book'))
        self.assertEqual((old / 'w0.seeds.bin').read_bytes(), (new / 'w0.seeds.bin').read_bytes())
        self.assertEqual(a['events'], b['events'])
        self.assertLessEqual(b['output_capacity_at_eof_bytes'], 512 * (280 + 176))
        # Observe output while input stays open. Cross enough endpoints to exceed
        # the fixed 1 MiB write buffer, without waiting for EOF.
        out = self.root / 'open_input'
        out.mkdir()
        cmd = [str(self.kernel), '--thresh', str(self.root / 'thresh'), '--work-dir', str(out),
               '--workers', '1', '--input', '-']
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        from cms_rtf.journal_events import JOURNAL_DTYPE
        rows = np.frombuffer(self.data[8:], dtype=JOURNAL_DTYPE).copy()
        ms = 34200100 + np.arange(len(rows), dtype=np.int64) * 500
        rows['exchange_time_raw'] = 20261009000000000 + ((ms // 3600000) * 10000000 +
            (ms // 60000 % 60) * 100000 + (ms // 1000 % 60) * 1000 + ms % 1000)
        try:
            proc.stdin.write(self.data + rows.tobytes())
            proc.stdin.flush()
            import time
            limit = time.monotonic() + 5
            while time.monotonic() < limit:
                path = out / 'w0.book.cmsb'
                if path.exists() and path.stat().st_size > 16:
                    break
                time.sleep(.01)
            self.assertGreater(path.stat().st_size, 16)
            proc.stdin.close()
            self.assertEqual(proc.wait(timeout=30), 0, proc.stderr.read())
        finally:
            if proc.poll() is None:
                proc.kill(); proc.wait()
            proc.stdout.close(); proc.stderr.close()

    def test_native_aggregation_and_truncation_rejection(self):
        new, _ = self.kernel_run('blocks', workers=8)
        ref = dict(symbols=np.array([1], dtype=np.int32), weights=np.ones((4, 1)), official=np.ones(4))
        books, flows, seeds = native_aggregate.worker_paths(new)
        output = new / 'factors.arrow'
        self.assertEqual(native_aggregate.run(books, flows, seeds, ref, '20261009', output), 115200)
        occupied = next(p for p in books if p.stat().st_size > 48)
        original = occupied.read_bytes()
        for blob in [original[:-1], b'OLDVERSN' + original[8:],
                     original[:-32] + b'nofooter' + original[-24:]]:
            occupied.write_bytes(blob)
            with self.assertRaises(subprocess.CalledProcessError):
                native_aggregate.run(books, flows, seeds, ref, '20261009', output)
        occupied.write_bytes(original)
        # Offset corruption in an otherwise complete index also fails closed.
        blob = bytearray(original)
        index_offset = struct.unpack_from('<Q', blob, len(blob) - 24)[0]
        struct.pack_into('<Q', blob, index_offset + 8, 0)
        occupied.write_bytes(blob)
        with self.assertRaises(subprocess.CalledProcessError):
            native_aggregate.run(books, flows, seeds, ref, '20261009', output)

    def test_sparse_boundaries_and_grouped_day_match_dense(self):
        from cms_rtf.journal_events import JOURNAL_DTYPE, rows_to_events
        from cms_rtf.codec import EVENT_DTYPE
        from cms_rtf.validate_replay import exact_factors
        import pyarrow as pa
        rows = np.frombuffer(self.data[8:], dtype=JOURNAL_DTYPE)[:16].copy()
        # Quiet gaps, lunch, late timestamps, and post-close captures. Two stocks
        # ensure the grouped single-thread driver flushes at symbol changes.
        times = [92959100, 93000500, 93004000, 93001500, 113000000,
                 130005100, 145959999, 150000001] * 2
        rows['exchange_time_raw'] = 20261009000000000 + np.array(times)
        rows['symbol'][8:] = b'000002'
        self.data = b'CMSPQ01\0' + rows.tobytes()
        (self.root / 'input').write_bytes(self.data)
        np.array([(s, 100000., 20000.) for s in [1, 2]],
                 dtype=[('s', '<i4'), ('xl', '<f8'), ('m', '<f8')]).tofile(self.root / 'thresh')
        dense, _ = self.kernel_run('legacy')
        sparse, _ = self.kernel_run('blocks')
        for kind, size in [('book', 280), ('flow', 176)]:
            self.assertEqual(logical_digest(dense / f'w0.{kind}.bin', size),
                             logical_digest(sparse / f'w0.{kind}.cmsb', size, expand_books=kind == 'book'))
        events = rows_to_events(rows, np.array([1, 2]))
        canonical = np.zeros(len(events['symbol']), dtype=EVENT_DTYPE)
        for field in EVENT_DTYPE.names:
            if field in events:
                canonical[field] = events[field]
        canonical.tofile(self.root / 'events')
        day = self.root / 'day'
        day.mkdir()
        subprocess.run([str(self.kernel.with_name('cms_index500ms_day')), '--events', str(self.root / 'events'),
                        '--thresh', str(self.root / 'thresh'), '--out-book', str(day / 'book.bin'),
                        '--out-flow', str(day / 'flow.bin'), '--out-seeds', str(day / 'seeds.bin')],
                       capture_output=True, check=True)
        for kind in ['book', 'flow', 'seeds']:
            self.assertEqual((day / f'{kind}.bin').read_bytes(), (dense / f'w0.{kind}.bin').read_bytes())
        ref = dict(symbols=np.array([1, 2], dtype=np.int32), weights=np.ones((4, 2)), official=np.full(4, 2.))
        tables = []
        for directory in [dense, sparse]:
            output = directory / 'factors.arrow'
            native_aggregate.run(*native_aggregate.worker_paths(directory), ref, '20261009', output)
            tables.append(pa.ipc.open_stream(output).read_all())
        self.assertEqual(exact_factors(*tables)['exact_columns'], 106)

    def test_python_backend_and_stale_mixed_directory_rejected(self):
        new, _ = self.kernel_run('blocks')
        books, flows, seeds = native_aggregate.worker_paths(new)
        with self.assertRaisesRegex(ValueError, 'requires --output-mode legacy'):
            native_aggregate.run_backend('python', books, flows, seeds, {}, '20261009', new / 'unused.arrow')
        (new / 'w0.book.bin').touch()
        with self.assertRaisesRegex(ValueError, 'mixed legacy/block'):
            native_aggregate.worker_paths(new)


if __name__ == '__main__':
    unittest.main()
