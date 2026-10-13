import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq
from cms_rtf.catalog import INDEX_WEIGHT_DIRS

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('apply_bundle', ROOT / 'deploy/apply_bundle.py')
apply = importlib.util.module_from_spec(spec); spec.loader.exec_module(apply)


class BundleDeployTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name); self.repo = self.root / 'dev'; self.repo.mkdir()
        self.prefix = self.root / 'cms-rtf'; self.prefix.mkdir()
        apply.git(self.repo, 'init')
        apply.git(self.repo, 'config', 'user.name', 'CMS test')
        apply.git(self.repo, 'config', 'user.email', 'cms-test@example.invalid')
        self.first = self.commit('initial')

    def commit(self, text):
        (self.repo / 'source').write_text(text)
        apply.git(self.repo, 'add', 'source'); apply.git(self.repo, 'commit', '-m', text)
        return apply.git(self.repo, 'rev-parse', 'HEAD')

    def bundle(self, basis=None):
        path = self.root / ('increment.bundle' if basis else 'full.bundle')
        apply.git(self.repo, 'bundle', 'create', str(path), 'HEAD', *(['^' + basis] if basis else []))
        return path

    def test_bootstrap_incremental_and_dirty_checkout_protection(self):
        old = apply.sync_source(self.prefix, self.bundle(), self.first, None)
        second = self.commit('updated')
        new = apply.sync_source(self.prefix, self.bundle(self.first), second, self.first)
        self.assertEqual((old / 'source').read_text(), 'initial')
        self.assertEqual((new / 'source').read_text(), 'updated')
        self.assertEqual(apply.current_source(self.prefix / 'repo.git'), second)
        (new / 'source').write_text('operator change')
        with self.assertRaisesRegex(RuntimeError, 'modified'):
            apply.sync_source(self.prefix, None, second, second)
        self.assertEqual((new / 'source').read_text(), 'operator change')

    def test_failed_build_does_not_activate_and_divergence_is_rejected(self):
        old = apply.sync_source(self.prefix, self.bundle(), self.first, None)
        (self.prefix / 'current').symlink_to(old)
        second = self.commit('next')
        bundle = self.bundle(self.first)
        import hashlib
        args = ['apply_bundle.py', '--prefix', str(self.prefix), '--bundle', str(bundle),
                '--bundle-sha256', hashlib.sha256(bundle.read_bytes()).hexdigest(),
                '--commit', second, '--basis', self.first]
        with mock.patch.object(sys, 'argv', args), mock.patch.object(apply, 'build_release', side_effect=RuntimeError('compile failed')):
            with self.assertRaisesRegex(RuntimeError, 'compile failed'):
                apply.main()
        self.assertEqual((self.prefix / 'current').resolve(), old)
        apply.git(self.repo, 'checkout', '--detach', self.first)
        other = self.commit('diverged')
        divergent = self.root / 'diverged.bundle'
        apply.git(self.repo, 'bundle', 'create', str(divergent), 'HEAD')
        with self.assertRaises(subprocess.CalledProcessError):
            apply.sync_source(self.prefix, divergent, other, second)
        self.assertEqual(apply.current_source(self.prefix / 'repo.git'), second)


class ServiceLifecycleTest(unittest.TestCase):
    def test_start_duplicate_stop_and_unrelated_pid_protection(self):
        if not hasattr(os, 'pidfd_open'):
            self.skipTest('Linux pidfd required')
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp); source = base / 'source'; prefix = base / 'rtf'
            (source / 'deploy').mkdir(parents=True); (source / 'build').mkdir()
            (source / 'python').symlink_to(ROOT / 'python')
            shutil.copy2(ROOT / 'deploy/service.py', source / 'deploy/service.py')
            (source / 'build/cms_index500ms_mt').symlink_to(ROOT / 'build/cms_index500ms_mt')
            (source / 'build/deployment.json').write_text(json.dumps(dict(commit='test', python_root=str(Path(pa.__file__).parent.parent))))
            prefix.mkdir(); apply.install_wrappers(prefix)
            (prefix / 'current').symlink_to(source)
            for name in INDEX_WEIGHT_DIRS:
                dest = prefix / 'references/weights' / name; dest.mkdir(parents=True)
                pq.write_table(pa.table({'stock_code': ['000001.SZ'], 'weight': [1.]}), dest / '2026-10-09.parquet')
            thresholds = prefix / 'references/thresholds'; thresholds.mkdir()
            (thresholds / 'threshold_20261009.csv').write_text('a,b,symbol,avg_daily_money,circ_cap\nx,x,000001,100000,1\n')
            cfg = json.loads((prefix / 'config/live.json').read_text())
            cfg['journal_root'] = str(base / 'empty-journal')
            cfg['journal_clock'] = 'source'  # lifecycle test works at any wall time
            (prefix / 'config/live.json').write_text(json.dumps(cfg))
            cmd = [sys.executable, '-B', str(source / 'deploy/service.py'), '--prefix', str(prefix)]
            def control(action):
                r = subprocess.run(cmd + [action], capture_output=True, text=True, timeout=40)
                self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
                return json.loads(r.stdout)
            try:
                first = control('start')
                self.assertEqual(first['status'], 'started')
                duplicate = control('start')
                self.assertEqual(duplicate['status'], 'already_running')
                self.assertEqual(first['pid'], duplicate['pid'])
                self.assertTrue(control('status')['running'])
            finally:
                result = control('stop')
            self.assertFalse(result['running'])
            self.assertEqual(result['last_result']['exit_code'], 0)
            self.assertEqual(result['last_result']['health']['status'], 'stopped')
            self.assertEqual(result['last_result']['health']['emitted_endpoints'], 0)
            self.assertFalse(list((prefix / 'data').glob('index_500ms/*/current')))
            # A stale or forged PID record must never signal an unrelated process.
            bad = dict(first, pid=os.getpid(), start_ticks=Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(')', 1)[1].split()[19])
            (prefix / 'run/service.json').write_text(json.dumps(bad))
            self.assertFalse(control('stop')['running'])


if __name__ == '__main__':
    unittest.main()
