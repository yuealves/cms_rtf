import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import numpy as np

from cms_rtf.journal_events import JOURNAL_DTYPE
from cms_rtf.run_journal_mt import run_pipeline


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.kernel = Path(os.environ.get('CMS_RTF_TEST_KERNEL',
                           Path(__file__).resolve().parents[2] / 'build/cms_index500ms_mt'))
        if not self.kernel.exists():
            self.skipTest('build cms_index500ms_mt first')
        np.array([(1, 100000., 20000.)],
                 dtype=[('s', '<i4'), ('xl', '<f8'), ('m', '<f8')]).tofile(self.root / 'thresh')
        rows = np.zeros(4099, dtype=JOURNAL_DTYPE)  # non-multiple of batch size
        rows['symbol'] = b'000001'
        rows['market'] = 2
        rows['action_code'] = ord('A')
        rows['side_raw'] = np.where(np.arange(len(rows)) % 2, 49, 50)
        rows['price_raw'] = np.where(rows['side_raw'] == 49, 100000, 101000)
        rows['quantity_raw'] = 1000
        rows['value_raw'] = 50
        rows['business_seq'] = np.arange(1, len(rows) + 1)
        rows['buy_ref'] = rows['business_seq']
        rows['sell_ref'] = rows['business_seq']
        rows['exchange_time_raw'] = 20261009093000100
        self.data = b'CMSPQ01\0' + rows.tobytes()
        (self.root / 'input').write_bytes(self.data)

    def command(self, out, workers=1):
        out.mkdir()
        return [str(self.kernel), '--thresh', str(self.root / 'thresh'),
                '--work-dir', str(out), '--workers', str(workers)]

    def test_pipe_file_headerless_and_worker_equivalence(self):
        outputs = []
        for i, (pipe, header, workers) in enumerate([(False, True, 1), (True, True, 1),
                                                     (False, False, 1), (True, True, 8)]):
            out = self.root / str(i)
            cmd = self.command(out, workers)
            blob = self.data if header else self.data[8:]
            if pipe:
                result = subprocess.run(cmd + ['--input', '-'], input=blob,
                                        capture_output=True, check=True, timeout=30)
            else:
                path = self.root / 'stream'
                path.write_bytes(blob)
                result = subprocess.run(cmd + ['--input', str(path)],
                                        capture_output=True, check=True, timeout=30)
            summary = json.loads(result.stdout)
            self.assertEqual(summary['events'], 4099)
            self.assertEqual(summary['input_rows'], 4099)
            outputs.append({kind: b''.join(p.read_bytes() for p in sorted(out.glob(f'w*.{kind}.bin')))
                            for kind in ('book', 'flow', 'seeds')})
        self.assertTrue(all(out == outputs[0] for out in outputs))

    def test_truncated_and_bad_magic_rejected(self):
        for i, blob in enumerate([b'badmagic', self.data[:-1]]):
            result = subprocess.run(self.command(self.root / str(i)) + ['--input', '-'],
                                    input=blob, capture_output=True, timeout=30)
            self.assertNotEqual(result.returncode, 0)

    def test_producer_failure_is_not_successful_eof(self):
        exporter = [sys.executable, '-c',
                    'import pathlib,sys;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes());sys.exit(7)',
                    str(self.root / 'input')]
        cmd = self.command(self.root / 'out')
        with self.assertRaisesRegex(RuntimeError, 'exporter=7'):
            run_pipeline(exporter, cmd, self.root, os.environ.copy())

    def test_truncated_thresholds_rejected(self):
        (self.root / 'thresh').write_bytes(b'incomplete')
        result = subprocess.run(self.command(self.root / 'out') + ['--input', '-'],
                                input=self.data, capture_output=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'truncated thresholds', result.stderr)

    @unittest.skipUnless(Path('/dev/full').exists(), 'requires Linux /dev/full')
    def test_output_write_failure_is_not_success(self):
        dest = self.root / 'out'
        cmd = self.command(dest)
        (dest / 'w0.book.bin').symlink_to('/dev/full')
        result = subprocess.run(cmd + ['--input', '-'], input=self.data,
                                capture_output=True, timeout=30)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'worker failed', result.stderr)

    def test_consumer_failure_does_not_hang_producer(self):
        exporter = [sys.executable, '-c', 'import sys;sys.stdout.buffer.write(bytes(1048576))']
        consumer = [sys.executable, '-c', 'import sys;sys.exit(3)']
        with self.assertRaisesRegex(RuntimeError, 'kernel=3'):
            run_pipeline(exporter, consumer, self.root, os.environ.copy())

    def test_keep_stream_matches_direct_pipe(self):
        exporter = [sys.executable, '-c',
                    'import pathlib,sys;sys.stdout.buffer.write(pathlib.Path(sys.argv[1]).read_bytes())',
                    str(self.root / 'input')]
        for i, keep in enumerate([False, True]):
            inter = self.root / str(i)
            cmd = self.command(inter)
            summary, timing = run_pipeline(exporter, cmd, inter, os.environ.copy(), keep)
            self.assertEqual(summary['events'], 4099)
            self.assertEqual((inter / 'stream.bin').exists(), keep)
            self.assertIn('kernel_mt' if keep else 'export_kernel', timing)
