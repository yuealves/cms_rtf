"""Exercise a growing, committed-frame journal without a gateway or paged."""
import datetime
import json
import os
from pathlib import Path
import signal
import struct
import subprocess
import tempfile
import time
import unittest

import numpy as np
import pyarrow as pa

from cms_rtf import native_aggregate
from cms_rtf.validate_replay import exact_factors

DAY = '20261009'
DAY0 = int(datetime.datetime(2026, 10, 9, tzinfo=datetime.timezone(datetime.timedelta(hours=8))).timestamp()) * 10**9


def murmur(data):
    h = 97 ^ len(data)
    for i in range(0, len(data) // 4 * 4, 4):
        k = int.from_bytes(data[i:i + 4], 'little') * 0x5bd1e995 & 0xffffffff
        k = (k ^ (k >> 24)) * 0x5bd1e995 & 0xffffffff
        h = (h * 0x5bd1e995 & 0xffffffff) ^ k
    tail = data[len(data) // 4 * 4:]
    if tail:
        h = (h ^ int.from_bytes(tail, 'little')) * 0x5bd1e995 & 0xffffffff
    h ^= h >> 13
    h = h * 0x5bd1e995 & 0xffffffff
    return h ^ (h >> 15)


def order(seq, ms, side=49, price=100000, symbol=b'000001', volume=100):
    raw = ((ms // 3600000) * 10000000 + (ms // 60000 % 60) * 100000 +
           (ms // 1000 % 60) * 1000 + ms % 1000)
    b = bytearray(72)
    struct.pack_into('<I', b, 0, seq)
    b[8] = 23; b[11:17] = symbol
    struct.pack_into('<QHq', b, 21, int(DAY) * 1000000000 + raw, 1, seq)
    struct.pack_into('<IqBB', b, 43, price, volume * 100, side, 50)
    return bytes(b)


class JournalWriter:
    def __init__(self, directory):
        self.directory = Path(directory); self.directory.mkdir()
        self.page = 0; self.seq = 0; self.epoch = 1; self.new_page()

    def new_page(self):
        self.page += 1; self.pos = 137; self.frames = 0
        self.path = self.directory / f'yjj.cms_l2_raw_v1.{self.page}.journal'
        with self.path.open('wb') as f:
            # Match the real writer: size the page before publishing its header.
            # Otherwise a follower can observe a transient 137-byte file.
            f.truncate(128 * 1024 * 1024)
            f.write(struct.pack('<B30shqqiih6s72s', 0, b'cms_l2_raw_v1', self.page,
                                DAY0, -1, 0, 0, 2, bytes(6), bytes(72)))
            f.flush(); f.seek(0); f.write(b'\x01')

    def append(self, kind, ms, payload=b'', commit=True):
        self.seq += 1
        nano = DAY0 + ms * 1000000
        b = bytearray(96 + len(payload))
        b[:4] = b'CMS1'
        struct.pack_into('<HHHBBIQQQq', b, 4, 1, 96, kind, 2 if kind == 1 else 0, 0,
                         int(DAY), self.epoch, self.seq, self.seq, nano)
        struct.pack_into('<I', b, 68, len(payload)); b[96:] = payload
        # A physically newer frame time must not accelerate the historical clock.
        h = struct.pack('<BhqiIhBiqi', 0, 1, nano + 10**12, 38 + len(b), murmur(b),
                        22001, 1, -1, nano, 0)
        at = self.pos
        with self.path.open('r+b', buffering=0) as f:
            f.seek(at); f.write(h + b)
            if commit:
                f.seek(at); f.write(b'\x01')
        self.pos += len(h) + len(b); self.frames += 1
        return at

    def commit(self, at):
        with self.path.open('r+b', buffering=0) as f:
            f.seek(at); f.write(b'\x01')

    def close_page(self):
        with self.path.open('r+b', buffering=0) as f:
            f.seek(41); f.write(struct.pack('<qii', DAY0 + 1, self.frames, self.pos))
            f.seek(self.pos); f.write(b'\x02')


class LiveJournalTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.kernel = Path(os.environ.get('CMS_RTF_KERNEL', Path(__file__).resolve().parents[2] / 'build/cms_index500ms_mt'))
        if not self.kernel.exists():
            self.skipTest('build kernel first')
        np.array([(1, 100000., 20000.), (2, 100000., 20000.)],
                 dtype=[('s', '<i4'), ('xl', '<f8'), ('m', '<f8')]).tofile(self.root / 'thresh')
        self.ref = dict(symbols=np.array([1, 2]), weights=np.array([[.5, .5]] * 4), official=np.ones(4))
        native_aggregate.write_reference(self.ref, self.root / 'ref')
        self.writer = JournalWriter(self.root / 'journal')

    def command(self, out, mode='follow', workers=2):
        out.mkdir()
        return [str(self.kernel), '--journal', str(self.writer.directory), '--journal-mode', mode,
                '--journal-clock', 'source', '--day', DAY, '--thresh', str(self.root / 'thresh'),
                '--ref', str(self.root / 'ref'), '--work-dir', str(out), '--workers', str(workers),
                '--output-mode', 'stream', '--out', str(out / 'factors.arrow')]

    def wait_status(self, out, predicate):
        limit = time.monotonic() + 30
        last = None
        while time.monotonic() < limit:
            try:
                last = json.loads((out / 'live-status.json').read_text())
                if predicate(last):
                    return last
            except FileNotFoundError:
                pass
            time.sleep(.02)
        self.fail(f'timed out: {last}')

    def start(self, out):
        proc = subprocess.Popen(self.command(out), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        def finish():
            if proc.poll() is None:
                proc.kill()
            proc.communicate(timeout=30)
        self.addCleanup(finish)
        return proc

    def opening(self):
        w = self.writer
        w.append(2, 34199000, b'{}')
        w.append(1, 34199100, order(1, 34199100))
        w.append(1, 34199200, order(2, 34199200, side=50, price=100100))
        w.append(1, 34201100, order(3, 34201100))
        w.append(3, 34202000)

    def test_growing_pages_uncommitted_frame_restart_and_partial_stop(self):
        out = self.root / 'follow'; proc = self.start(out)
        self.opening()
        before = self.wait_status(out, lambda x: x['emitted_endpoints'] == 2)
        self.assertEqual(before['records'], 5)
        first = pa.ipc.open_stream(out / 'factors.arrow').read_all()
        self.assertEqual(first.num_rows, 8)
        pending = self.writer.append(1, 34203100, order(4, 34203100), commit=False)
        time.sleep(.15)
        self.assertIsNone(proc.poll())
        self.writer.commit(pending)
        self.writer.close_page()
        # The next page arrives late; never reread the preceding page.
        time.sleep(.15); self.assertIsNone(proc.poll())
        self.writer.new_page()
        self.writer.append(4, 34204000)
        self.writer.epoch = 2
        self.writer.append(2, 34205000, b'{}')
        self.writer.append(1, 34205100, order(5, 34205100))
        self.writer.append(3, 34206000)
        status = self.wait_status(out, lambda x: x['records'] == 10)
        self.assertEqual(status['input_rows'], 5)
        self.assertEqual(status['sessions'], 2)
        self.assertTrue(status['input_incomplete'])
        self.assertEqual(status['page'], 2)
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 0, stderr)
        status = json.loads((out / 'live-status.json').read_text())
        self.assertEqual(status['status'], 'stopped')
        self.assertFalse(status['full_grid'])
        actual = pa.ipc.open_stream(out / 'factors.arrow').read_all()
        self.assertTrue(actual.slice(0, 8).equals(first))
        self.assertLess(actual.num_rows, 115200)
        self.assertFalse(list(out.glob('w*')))

    def test_explicit_session_timers_and_late_event_do_not_rewrite(self):
        out = self.root / 'timers'; proc = self.start(out)
        self.opening()
        self.wait_status(out, lambda x: x['emitted_endpoints'] == 2)
        first = pa.ipc.open_stream(out / 'factors.arrow').read_all()
        self.writer.append(1, 34203000, order(4, 34200200, volume=999999))
        self.writer.append(1, 34203001, order(40, 34200200, symbol=b'000002'))
        self.writer.append(3, 41459999)
        self.wait_status(out, lambda x: x['records'] == 8)
        self.assertEqual(pa.ipc.open_stream(out / 'factors.arrow').read_all().num_rows, 8)
        self.writer.append(3, 41460001)
        am = self.wait_status(out, lambda x: x['emitted_endpoints'] == 14400)
        self.assertEqual(am['late_events'], 2)
        self.assertTrue(pa.ipc.open_stream(out / 'factors.arrow').read_all().slice(0, 8).equals(first))
        self.writer.append(1, 46800100, order(5, 46800100))
        self.writer.append(3, 54060000)
        self.writer.append(4, 55800000)
        self.wait_status(out, lambda x: x['full_grid'])
        proc.send_signal(signal.SIGTERM); _, stderr = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 0, stderr)
        # Frozen-journal replay uses recorded clocks, independent of replay speed
        # or file chunk boundaries, and must match the growing journal result.
        expected = pa.ipc.open_stream(out / 'factors.arrow').read_all()
        # Two on-time 100-share orders, weight .5 for each of four indices.
        # Neither the huge late order nor the newly seen stock's late order may
        # leak into the next bucket's flow.
        self.assertEqual(np.nansum(expected['order_buy_volume_500ms'].to_numpy()), 400.)
        for workers in (1, 8):
            replay = self.root / f'replay{workers}'
            result = subprocess.run(self.command(replay, 'replay', workers), capture_output=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            actual = pa.ipc.open_stream(replay / 'factors.arrow').read_all()
            check = exact_factors(expected, actual)
            self.assertTrue(check['keys_equal']); self.assertEqual(check['exact_columns'], 106)

    def test_watermark_closes_pm_tail_before_the_sixty_second_timer(self):
        out = self.root / 'early_tail'; proc = self.start(out)
        self.opening()
        self.writer.append(1, 54000100, order(10, 54000000))
        self.writer.append(1, 54000200, order(11, 54000001))
        self.writer.append(3, 54000501)
        self.wait_status(out, lambda x: x['full_grid'])
        lines = (out / 'latency.log').read_text().splitlines()
        self.assertEqual(lines[-1].split()[0], '15:00:00.500')
        self.assertFalse(any(line.startswith('15:01:00.000') for line in lines))
        self.assertEqual(pa.ipc.open_stream(out / 'factors_local.arrow').read_all().num_rows, 115200)
        proc.send_signal(signal.SIGTERM); _, err = proc.communicate(timeout=30)
        self.assertEqual(proc.returncode, 0, err)

    def test_integrity_errors_and_missing_final_end(self):
        self.opening()
        out = self.root / 'missing_end'
        result = subprocess.run(self.command(out, 'replay'), capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'missing final END', result.stderr)
        self.assertFalse((out / 'factors.arrow').exists())
        self.writer.append(4, 55800000)
        # Mutate a committed payload while retaining its old hash.
        with self.writer.path.open('r+b') as f:
            f.seek(137 + 38 + 16); f.write(b'\xff')
        bad = self.root / 'bad_hash'
        result = subprocess.run(self.command(bad, 'replay'), capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'hash mismatch', result.stderr)
        self.assertFalse((bad / 'factors.arrow').exists())

    def test_follow_refuses_overwriting_a_previous_output(self):
        out = self.root / 'existing'; cmd = self.command(out)
        (out / 'factors.arrow').write_bytes(b'accepted')
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((out / 'factors.arrow').read_bytes(), b'accepted')

    def test_latency_matches_cutoffs_and_both_outputs_not_exchange_endpoints(self):
        out = self.root / 'latency'; proc = self.start(out)
        w = self.writer
        w.append(2, 34199000, b'{}')
        w.append(1, 34200200, order(1, 34200200))
        self.wait_status(out, lambda x: x['input_rows'] == 1)
        self.assertEqual((out / 'latency.log').read_text(), '')
        time.sleep(.2)  # waiting for the next frame is outside Huatai latency
        w.append(1, 34201100, order(2, 34201100))
        w.append(3, 34202000)
        status = self.wait_status(out, lambda x: x['emitted_endpoints'] == 2)
        rows = [line.split() for line in (out / 'latency.log').read_text().splitlines()]
        self.assertEqual([r[0] for r in rows], ['09:30:00.500', '09:30:01.000', '09:30:01.500'])
        self.assertTrue(all(0 <= float(r[1]) < 190 for r in rows))
        self.assertEqual(status['local_emitted_endpoints'], 3)
        self.assertEqual(pa.ipc.open_stream(out / 'factors.arrow').read_all().num_rows, 8)
        self.assertEqual(pa.ipc.open_stream(out / 'factors_local.arrow').read_all().num_rows, 12)
        self.assertFalse((out / 'latency.csv').exists())
        self.assertFalse((out / 'latency.meta.json').exists())
        proc.send_signal(signal.SIGTERM); _, stderr = proc.communicate(timeout=60)
        self.assertEqual(proc.returncode, 0, stderr)

    def test_latency_on_off_preserves_both_grids_and_counts_no_row_cutoffs(self):
        self.opening()
        self.writer.append(3, 41460000)
        self.writer.append(3, 54060000)
        self.writer.append(4, 55800000)
        tables = []
        for enabled in ('on', 'off'):
            out = self.root / enabled
            result = subprocess.run(self.command(out, 'replay', 8) + ['--latency', enabled],
                                    capture_output=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            tables.append([pa.ipc.open_stream(out / name).read_all()
                           for name in ['factors.arrow', 'factors_local.arrow']])
        for i in range(2):
            check = exact_factors(tables[0][i], tables[1][i])
            self.assertEqual(check['exact_columns'], 106); self.assertTrue(check['keys_equal'])
            self.assertEqual(tables[0][i].num_rows, 115200)
        self.assertFalse((self.root / 'off/latency.log').exists())
        rows = [line.split() for line in (self.root / 'on/latency.log').read_text().splitlines()]
        self.assertEqual(len(rows), 28800 + 3600 + 2 + 1)
        self.assertEqual(sum(r[0] == '15:01:00.000' for r in rows), 2)
        self.assertEqual(sum(r[0] == '11:31:00.000' for r in rows), 1)
        self.assertEqual(rows[-1][0], '15:30:00.000')

    def test_missing_intermediate_page_is_not_a_successful_end(self):
        self.opening(); self.writer.append(4, 55800000); self.writer.close_page()
        self.writer.new_page()
        self.writer.path.rename(self.writer.directory / 'yjj.cms_l2_raw_v1.3.journal')
        out = self.root / 'page_gap'
        result = subprocess.run(self.command(out, 'replay'), capture_output=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(b'missing journal page', result.stderr)

    def test_direct_journal_matches_existing_dtype_replay_without_late_events(self):
        from cms_rtf.journal_events import JOURNAL_DTYPE
        self.opening()
        self.writer.append(3, 41460000)
        self.writer.append(1, 46800100, order(5, 46800100))
        self.writer.append(3, 54060000); self.writer.append(4, 55800000)
        rows = np.zeros(4, dtype=JOURNAL_DTYPE)
        rows['market'] = 2; rows['message_type'] = 23; rows['symbol'] = b'000001'
        rows['action_code'] = ord('A'); rows['value_raw'] = 50; rows['channel'] = 1
        rows['business_seq'] = [1, 2, 3, 5]; rows['side_raw'] = [49, 50, 49, 49]
        rows['buy_ref'] = [1, 0, 3, 5]; rows['sell_ref'] = [0, 2, 0, 0]
        rows['price_raw'] = [100000, 100100, 100000, 100000]; rows['quantity_raw'] = 10000
        rows['exchange_time_raw'] = [20261009092959100, 20261009092959200,
                                     20261009093001100, 20261009130000100]
        dtype = self.root / 'dtype.bin'; dtype.write_bytes(b'CMSPQ01\0' + rows.tobytes())
        baseline = self.root / 'dtype'; baseline.mkdir()
        subprocess.run([str(self.kernel), '--input', str(dtype), '--thresh', str(self.root / 'thresh'),
                        '--ref', str(self.root / 'ref'), '--day', DAY, '--output-mode', 'stream',
                        '--work-dir', str(baseline), '--out', str(baseline / 'factors.arrow')],
                       check=True, capture_output=True, timeout=60)
        dest = self.root / 'direct'
        result = subprocess.run(self.command(dest, 'replay'), capture_output=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)
        check = exact_factors(pa.ipc.open_stream(baseline / 'factors.arrow').read_all(),
                              pa.ipc.open_stream(dest / 'factors.arrow').read_all())
        self.assertTrue(check['keys_equal']); self.assertEqual(check['exact_columns'], 106)

    def paced_replay(self, dest, start, stop, speed, drain):
        cmd = [str(self.kernel.with_name('cms_journal_replayer')), '--source', str(self.writer.directory),
               '--dest', str(dest.relative_to(self.root)), '--pace-from-ns', str(start), '--until-ns', str(stop),
               '--speed', str(speed), '--wait-ready']
        proc = subprocess.Popen(cmd, cwd=self.root, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            ready = json.loads(proc.stdout.readline())
            drain(ready['ready_records'])
            out, err = proc.communicate('go\n', timeout=60)
            self.assertEqual(proc.returncode, 0, err)
            return json.loads(out)
        finally:
            if proc.poll() is None: proc.kill()
            proc.communicate(timeout=30)

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
            report = self.paced_replay(copied, DAY0 + 34200000 * 1000000,
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
            _, err = proc.communicate(timeout=30)
            self.assertEqual(proc.returncode, 0, err)
            expected_out = self.root / 'frozen'
            result = subprocess.run(self.command(expected_out, 'replay', 8), capture_output=True, timeout=60)
            self.assertEqual(result.returncode, 0, result.stderr)
            a = pa.ipc.open_stream(out / 'factors.arrow').read_all()
            b = pa.ipc.open_stream(expected_out / 'factors.arrow').read_all()
            check = exact_factors(a, b.slice(0, a.num_rows))
            self.assertEqual(check['exact_columns'], 106); self.assertTrue(check['keys_equal'])
            self.assertFalse(json.loads((out / 'live-status.json').read_text())['journal_ended'])
        finally:
            if proc.poll() is None: proc.kill()
            proc.communicate(timeout=30)

    def test_paced_replay_rejects_invalid_speed_and_existing_destination(self):
        dest = self.root / 'existing'; dest.mkdir()
        keep = dest / 'keep'; keep.write_bytes(b'keep')
        for speed in ['0', '-1', 'nan', '5']:
            cmd = [str(self.kernel.with_name('cms_journal_replayer')), '--source', str(self.writer.directory),
                   '--dest', str(dest), '--pace-from-ns', '1', '--until-ns', '2', '--speed', speed]
            result = subprocess.run(cmd, capture_output=True, timeout=60)
            self.assertNotEqual(result.returncode, 0)
        self.assertEqual(keep.read_bytes(), b'keep')


if __name__ == '__main__':
    unittest.main()
