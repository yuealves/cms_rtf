import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('summarize_latency', Path(__file__).resolve().parents[2] / 'scripts/summarize_latency.py')
summary = importlib.util.module_from_spec(spec); spec.loader.exec_module(summary)


class LatencySummaryTest(unittest.TestCase):
    def test_partitions_phases_timers_empty_windows_and_partial_tail(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'latency.meta.json').write_text(json.dumps(dict(schema='cms-latency-v1', unit='ms')))
            path = root / 'latency.csv'
            fields = ['endpoint', 'mode', 'phase', 'reason', 'events', 'close_id', 'flush_id',
                      'close_to_write_ms', 'last_read_to_write_ms', 'last_recv_to_write_ms']
            with path.open('w') as f:
                w = csv.writer(f); w.writerow(fields)
                for phase, reason, events, recv in [('tail', 'watermark', 2, -1), ('catchup', 'watermark', 3, 99999),
                                                  ('tail', 'pm_timer', 4, 60000), ('tail', 'watermark', 0, '')]:
                    w.writerow(['10:00:00.500', 'journal_follow_wall', phase, reason, events, 1, 1, .05, 3, recv])
                f.write('10:00:01.000,journal_follow_wall,tail')
            d = summary.summarize(path, phase='tail', reason='watermark')
            self.assertEqual(len(d['groups']), 1)
            g = d['groups'][0]
            self.assertEqual(g['windows'], 1)
            self.assertEqual(g['metrics']['close_to_write_ms']['mean'], .05)
            self.assertEqual(g['metrics']['last_recv_to_write_ms']['negative'], 1)
            self.assertEqual(d['skipped'], dict(phase_or_reason=2, empty_window=1))

    def test_huatai_log_is_milliseconds_and_excludes_after_hours_by_default(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'latency.log'
            path.write_text('09:30:00.500 3\n10:00:00.500 5\n15:09:59.500 0.068093\n15:10:00.000 ')
            d = summary.summarize(path)
            self.assertEqual(d['groups'][0]['metrics']['close_to_write_ms']['mean'], 4.)
            self.assertEqual(d['groups'][0]['metrics']['last_recv_to_write_ms']['count'], 0)
            self.assertEqual(d['skipped']['outside_session'], 1)
