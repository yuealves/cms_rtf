import json
import os
from pathlib import Path
import subprocess
import time
import unittest

import numpy as np
import pyarrow as pa

from cms_rtf import native_aggregate
from cms_rtf.journal_events import JOURNAL_DTYPE
from cms_rtf.validate_replay import exact_factors
import test_journal_pipeline as fixture


class StreamAggregateTest(unittest.TestCase):
    setUp = fixture.PipelineTest.setUp

    def prepare(self):
        # Two stocks on one channel: stock 2 becomes quiet while stock 1 advances.
        # Include invalid books, exact endpoints, lunch, and post-close events.
        times = [92959100, 92959200, 92959300, 92959400,
                 93000500, 93001100, 93004000, 113000000,
                 130000100, 130001000, 145959999, 150000010]
        rows = np.frombuffer(self.data[8:], dtype=JOURNAL_DTYPE)[:len(times)].copy()
        rows['exchange_time_raw'] = 20261009000000000 + np.array(times)
        rows['symbol'][2:4] = b'000002'
        self.data = b'CMSPQ01\0' + rows.tobytes()
        (self.root / 'input').write_bytes(self.data)
        np.array([(s, 100000., 20000.) for s in [1, 2]],
                 dtype=[('s', '<i4'), ('xl', '<f8'), ('m', '<f8')]).tofile(self.root / 'thresh')
        ref = dict(symbols=np.array([1, 2]), weights=np.array([[.2, .8]] * 4), official=np.ones(4))
        native_aggregate.write_reference(ref, self.root / 'ref')
        return ref

    def command(self, out, workers=1, batch=1):
        out.mkdir()
        return [str(self.kernel), '--thresh', str(self.root / 'thresh'), '--work-dir', str(out),
                '--workers', str(workers), '--output-mode', 'stream', '--ref', str(self.root / 'ref'),
                '--day', '20261009', '--out', str(out / 'factors.arrow'), '--window-batch', str(batch)]

    def test_exact_old_path_workers_batching_and_no_stock_files(self):
        ref = self.prepare()
        old = self.root / 'old'; old.mkdir()
        subprocess.run([str(self.kernel), '--thresh', str(self.root / 'thresh'), '--input', str(self.root / 'input'),
                        '--work-dir', str(old), '--output-mode', 'blocks'], check=True, capture_output=True)
        native_aggregate.run(*native_aggregate.worker_paths(old), ref, '20261009', old / 'factors.arrow')
        expected = pa.ipc.open_stream(old / 'factors.arrow').read_all()
        for workers, batch in [(1, 1), (8, 1), (8, 64)]:
            dest = self.root / f'new_{workers}_{batch}'
            result = subprocess.run(self.command(dest, workers, batch) + ['--input', '-'], input=self.data,
                                    check=True, capture_output=True, timeout=30)
            self.assertEqual(json.loads(result.stdout)['emitted_endpoints'], 28800)
            self.assertEqual([p.name for p in dest.iterdir()], ['factors.arrow'])
            actual = pa.ipc.open_stream(dest / 'factors.arrow').read_all()
            check = exact_factors(expected, actual)
            self.assertEqual(check['exact_columns'], 106)
            self.assertTrue(check['keys_equal'])

    def test_output_before_eof_and_late_event_fails_without_replacing_previous(self):
        self.prepare()
        dest = self.root / 'live'
        cmd = self.command(dest, workers=8)
        final = dest / 'factors.arrow'; final.write_bytes(b'previous accepted output')
        rows = np.frombuffer(self.data[8:], dtype=JOURNAL_DTYPE).copy()
        proc = subprocess.Popen(cmd + ['--input', '-'], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            proc.stdin.write(b'CMSPQ01\0' + rows[:7].tobytes()); proc.stdin.flush()
            limit = time.monotonic() + 10
            partial = dest / f'factors.arrow.tmp.{proc.pid}'
            published = None
            while time.monotonic() < limit:
                if partial.exists() and partial.stat().st_size > 0:
                    try:
                        published = pa.ipc.open_stream(partial).read_all()
                        if published.num_rows: break
                    except pa.ArrowInvalid:
                        pass
                time.sleep(.01)
            self.assertIsNotNone(published)
            self.assertGreater(published.num_rows, 0)
            self.assertLess(published.num_rows, 115200)
            self.assertIsNone(proc.poll())
            self.assertEqual(final.read_bytes(), b'previous accepted output')
            # A previously closed timestamp must never silently alter an old row.
            proc.stdin.write(rows[:1].tobytes()); proc.stdin.flush(); proc.stdin.close()
            self.assertNotEqual(proc.wait(timeout=20), 0)
            self.assertIn(b'late event behind published watermark', proc.stderr.read())
            self.assertFalse(partial.exists())
            self.assertEqual(final.read_bytes(), b'previous accepted output')
        finally:
            if proc.poll() is None: proc.kill(); proc.wait()
            if not proc.stdin.closed: proc.stdin.close()
            proc.stdout.close(); proc.stderr.close()

    def test_stalled_watermark_budget_and_truncated_input_fail_closed(self):
        self.prepare()
        for label, blob, extra, expected in [
            ('truncated', self.data[:-1], [], b'truncated'),
        ]:
            dest = self.root / label
            cmd = self.command(dest) + extra
            target = dest / 'factors.arrow'; target.write_bytes(b'accepted')
            result = subprocess.run(cmd + ['--input', '-'], input=blob, capture_output=True, timeout=30)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(expected, result.stderr)
            self.assertEqual(target.read_bytes(), b'accepted')
            self.assertEqual(list(dest.iterdir()), [target])
        # Channel 2 never progresses after pre-open. Channel 1 generates enough
        # distinct endpoints to exceed a deliberately tiny pending-record budget.
        rows = np.frombuffer(self.data[8:], dtype=JOURNAL_DTYPE)[:1].repeat(4099)
        rows['channel'] = 1; rows['channel'][0] = 2
        rows['business_seq'] = np.arange(len(rows)) + 1
        rows['buy_ref'] = rows['business_seq']; rows['sell_ref'] = rows['business_seq']
        ms = 34200000 + np.arange(len(rows)) * 500
        hhmmssmmm = (ms // 3600000 * 10000000 + ms // 60000 % 60 * 100000 +
                     ms // 1000 % 60 * 1000 + ms % 1000)
        rows['exchange_time_raw'] = 20261009000000000 + hhmmssmmm
        dest = self.root / 'stalled'
        cmd = self.command(dest) + ['--pending-mib', '1', '--input', '-']
        result = subprocess.run(cmd, input=b'CMSPQ01\0' + rows.tobytes(), capture_output=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'pending window budget exceeded', result.stderr)
        self.assertFalse(list(dest.iterdir()))


if __name__ == '__main__':
    unittest.main()
