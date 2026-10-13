import json
from pathlib import Path
import signal
import subprocess
import sys
import unittest

import pyarrow as pa
from cms_rtf.validate_replay import exact_factors
import test_live_journal as fixture
from test_live_journal import DAY0, order

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from benchmark_paced_journal import replay


class PacedJournalTest(unittest.TestCase):
    setUp = fixture.LiveJournalTest.setUp
    command = fixture.LiveJournalTest.command
    wait_status = fixture.LiveJournalTest.wait_status

    def test_five_x_growing_pages_keep_bytes_clocks_and_factors(self):
        w = self.writer
        w.append(2, 34199000, b'{}')
        w.append(1, 34199100, order(1, 34199100))
        w.append(1, 34199200, order(2, 34199200, side=50, price=100100))
        w.close_page(); w.new_page()
        w.append(1, 34200200, order(3, 34200200))
        w.append(1, 34201100, order(4, 34201100))
        w.append(3, 34202000)
        original_pos = w.pos
        w.append(4, 34203000)
        copied = self.root / 'paced-journal'
        out = self.root / 'paced'
        cmd = self.command(out, workers=8)
        cmd[cmd.index('--journal') + 1] = str(copied)
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def drain(n):
            self.wait_status(out, lambda x: x['records'] == n and x['caught_up'])
        try:
            report = replay(w.directory, copied, DAY0 + 34200000 * 1000000,
                            DAY0 + 34202500 * 1000000, 5., drain)
            drain(report['records'])
            self.assertEqual(report['records'], 6)
            self.assertEqual(report['paced_records'], 3)
            self.assertGreaterEqual(report['elapsed_paced_seconds'], .399)
            self.assertLessEqual(report['effective_speed'], 5.01)
            self.assertFalse(report['synthesized_end'])
            # Header, payload, frame timestamps and hashes match through the
            # copied prefix; no fake END or closed-page marker follows it.
            for page, length in [(1, 137 + (38 + 98) + 2 * (38 + 96 + 72) + 1), (2, original_pos)]:
                name = f'yjj.cms_l2_raw_v1.{page}.journal'
                with (copied / name).open('rb') as a, (w.directory / name).open('rb') as b:
                    self.assertEqual(a.read(length), b.read(length))
            with (copied / 'yjj.cms_l2_raw_v1.2.journal').open('rb') as f:
                f.seek(original_pos); self.assertEqual(f.read(1), b'\0')
            proc.send_signal(signal.SIGTERM)
            _, err = proc.communicate(timeout=10)
            self.assertEqual(proc.returncode, 0, err)
            expected_out = self.root / 'frozen'
            result = subprocess.run(self.command(expected_out, 'replay', 8), capture_output=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            a = pa.ipc.open_stream(out / 'factors.arrow').read_all()
            b = pa.ipc.open_stream(expected_out / 'factors.arrow').read_all()
            check = exact_factors(a, b)
            self.assertEqual(check['exact_columns'], 106); self.assertTrue(check['keys_equal'])
            self.assertFalse(json.loads((out / 'live-status.json').read_text())['journal_ended'])
        finally:
            if proc.poll() is None: proc.kill()
            proc.communicate(timeout=10)

    def test_invalid_speed_or_existing_destination_cannot_overwrite(self):
        dest = self.root / 'existing'; dest.mkdir()
        keep = dest / 'keep'; keep.write_bytes(b'keep')
        for speed, error in [(0, ValueError), (-1, ValueError), (float('nan'), ValueError), (5, FileExistsError)]:
            with self.assertRaises(error): replay(self.writer.directory, dest, 1, 2, speed, lambda _: None)
        self.assertEqual(keep.read_bytes(), b'keep')
