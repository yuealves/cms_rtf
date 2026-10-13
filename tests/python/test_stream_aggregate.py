import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest

import numpy as np
import pyarrow as pa

from cms_rtf import native_aggregate
from cms_rtf.journal_events import JOURNAL_DTYPE
from cms_rtf.validate_replay import exact_factors
from cms_rtf.run_journal_mt import run_publishing_pipeline
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
        # One hot stock needs >1/8 MiB but <1 MiB. All workers share credits;
        # otherwise an even split incorrectly rejects this valid replay.
        shared = self.root / 'shared_budget'
        result = subprocess.run(self.command(shared, workers=8) + ['--pending-mib', '1', '--input', '-'],
                                input=b'CMSPQ01\0' + rows[:1500].tobytes(), capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(pa.ipc.open_stream(shared / 'factors.arrow').read_all().num_rows, 115200)
        dest = self.root / 'stalled'
        cmd = self.command(dest) + ['--pending-mib', '1', '--input', '-']
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            try:
                proc.stdin.write(b'CMSPQ01\0' + rows.tobytes()); proc.stdin.flush()
            except BrokenPipeError:
                pass
            # Leave upstream open and idle: worker failure must interrupt the
            # consumer's input wait without relying on producer EOF.
            self.assertNotEqual(proc.wait(timeout=10), 0)
            self.assertIn(b'pending window budget exceeded', proc.stderr.read())
        finally:
            if proc.poll() is None: proc.kill(); proc.wait()
            try: proc.stdin.close()
            except BrokenPipeError: pass
            proc.stdout.close(); proc.stderr.close()
        self.assertFalse(list(dest.iterdir()))

    def test_integrated_producer_failure_preserves_previous_output(self):
        self.prepare()
        dest = self.root / 'producer'
        cmd = self.command(dest)
        pos = cmd.index('--out'); del cmd[pos:pos + 2]
        final = dest / 'factors.arrow'; final.write_bytes(b'accepted')
        exporter = [sys.executable, '-c',
                    'import pathlib,sys;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes());sys.exit(int(sys.argv[2]))',
                    str(self.root / 'input')]
        with self.assertRaisesRegex(RuntimeError, 'exporter=7'):
            run_publishing_pipeline(exporter + ['7'], cmd, dest, os.environ.copy(), final)
        self.assertEqual(final.read_bytes(), b'accepted')
        self.assertFalse(list(dest.glob('.*pending*')))
        for keep in [False, True]:
            health, _ = run_publishing_pipeline(exporter + ['0'], cmd, dest, os.environ.copy(), final, keep)
            self.assertEqual(health['emitted_endpoints'], 28800)
            self.assertEqual(pa.ipc.open_stream(final).read_all().num_rows, 115200)

    def test_session_close_timestamp_is_not_a_completion_marker(self):
        ref = self.prepare()
        rows = np.frombuffer(self.data[8:], dtype=JOURNAL_DTYPE).copy()
        rows['symbol'] = np.array([b'000001', b'000001', b'000002', b'000002',
                                   b'000001', b'000001', b'000002', b'000001',
                                   b'000002', b'000001', b'000002', b'000002'])
        times = [92959100, 92959200, 92959300, 92959400, 93001100,
                 113000000, 112959000, 130000100, 130000200,
                 150000000, 145951120, 145959800]
        rows['exchange_time_raw'] = 20261009000000000 + np.array(times)
        (self.root / 'input').write_bytes(b'CMSPQ01\0' + rows.tobytes())
        old = self.root / 'closing_old'; old.mkdir()
        subprocess.run([str(self.kernel), '--thresh', str(self.root / 'thresh'), '--input', str(self.root / 'input'),
                        '--work-dir', str(old), '--output-mode', 'blocks'], check=True, capture_output=True)
        native_aggregate.run(*native_aggregate.worker_paths(old), ref, '20261009', old / 'factors.arrow')
        dest = self.root / 'closing_stream'
        cmd = self.command(dest, workers=8)
        pos = cmd.index('--out'); del cmd[pos:pos + 2]
        # Separate input arrivals, so the close timestamp is observed by the
        # dispatcher before the following stock's delayed pre-close record.
        producer = [sys.executable, '-u', '-c',
                    'import pathlib,sys,time\n'
                    'b=pathlib.Path(sys.argv[1]).read_bytes();sys.stdout.buffer.write(b[:8])\n'
                    'for i in range(8,len(b),128):\n'
                    ' sys.stdout.buffer.write(b[i:i+128]);sys.stdout.flush();time.sleep(.03)\n',
                    str(self.root / 'input')]
        output = dest / 'factors.arrow'
        run_publishing_pipeline(producer, cmd, dest, os.environ.copy(), output)
        check = exact_factors(pa.ipc.open_stream(old / 'factors.arrow').read_all(),
                              pa.ipc.open_stream(output).read_all())
        self.assertEqual(check['exact_columns'], 106)
        self.assertTrue(check['keys_equal'])


if __name__ == '__main__':
    unittest.main()
