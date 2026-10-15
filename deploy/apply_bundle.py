#!/usr/bin/env python3
"""Apply a source bundle and build/test on the CMS host before activation."""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tarfile
import time


def run(args, **kwargs):
    return subprocess.run([str(x) for x in args], check=True, **kwargs)


def git(repo, *args):
    return run(['git', '-C', repo, *args], capture_output=True, text=True).stdout.strip()


def current_source(repo):
    if not (repo / 'HEAD').exists():
        return None
    r = subprocess.run(['git', '-C', str(repo), 'rev-parse', '--verify', 'refs/heads/source'],
                       capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


def sync_source(prefix, bundle, commit, basis):
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('invalid commit')
    repo = prefix / 'repo.git'
    if not repo.exists():
        run(['git', 'init', '--bare', repo], capture_output=True)
    actual = current_source(repo)
    if actual != basis and actual != commit:
        raise RuntimeError(f'remote source changed: expected {basis}, found {actual}')
    if bundle:
        git(repo, 'bundle', 'verify', str(bundle))
        git(repo, 'fetch', '--no-tags', str(bundle), 'HEAD')
        if git(repo, 'rev-parse', 'FETCH_HEAD') != commit:
            raise RuntimeError('bundle HEAD does not match requested commit')
    elif actual != commit:
        raise RuntimeError('new source requires a bundle')
    if actual and actual != commit:
        git(repo, 'merge-base', '--is-ancestor', actual, commit)
    source = prefix / 'sources' / commit
    if source.exists():
        if git(source, 'rev-parse', 'HEAD') != commit or git(source, 'status', '--porcelain'):
            raise RuntimeError(f'existing source checkout was modified: {source}')
    else:
        source.parent.mkdir(parents=True, exist_ok=True)
        git(repo, 'worktree', 'add', '--detach', str(source), commit)
    git(repo, 'update-ref', 'refs/heads/source', commit)
    return source


def atomic_json(path, value):
    temp = path.with_name(path.name + '.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n'); temp.replace(path)


def install_build_tools(prefix, archive, digest):
    if hashlib.sha256(archive.read_bytes()).hexdigest() != digest:
        raise RuntimeError('build tool archive hash mismatch')
    parent = prefix / 'toolchains'; parent.mkdir(exist_ok=True)
    with tarfile.open(archive) as tf:
        names = {Path(m.name).parts[0] for m in tf.getmembers()}
        if len(names) != 1:
            raise RuntimeError('build tools archive must have one root')
        name = names.pop()
        dest = parent / name
        if not dest.exists():
            for m in tf.getmembers():
                p = Path(m.name)
                if p.is_absolute() or '..' in p.parts or m.isdev() or m.isfifo() or m.islnk():
                    raise RuntimeError('unsafe build tools archive member')
                if m.issym() and (str(p.relative_to(name)), m.linkname) not in (
                        ('sysroot/lib', '/lib'), ('sysroot/lib64', '/lib64')):
                    raise RuntimeError('unexpected build tools symlink')
            tf.extractall(parent, filter='fully_trusted')
    manifest = json.loads((dest / 'manifest.json').read_text())
    if manifest['os'] != 'Ubuntu 24.04' or manifest['arch'] != 'x86_64':
        raise RuntimeError('unexpected build tools platform')
    for name, sha in manifest['files'].items():
        p = Path(name)
        if p.is_absolute() or '..' in p.parts:
            raise RuntimeError('invalid build tools manifest path')
        if hashlib.sha256((dest / p).read_bytes()).hexdigest() != sha:
            raise RuntimeError('build tools file mismatch: ' + name)
    link = prefix / '.build-tools.next'
    link.unlink(missing_ok=True); link.symlink_to(dest.relative_to(prefix)); link.replace(prefix / 'build-tools')


def build_release(prefix, source, jobs, python_root):
    kit = prefix / 'build-tools/bin'
    cmake, ctest = (str(kit / n) if (kit / n).exists() else n for n in ('cmake', 'ctest'))
    python_root = python_root.resolve(strict=True)
    env = os.environ.copy()
    env['PYTHONPATH'] = str(source / 'python') + ':' + str(python_root)
    env['PYTHONDONTWRITEBYTECODE'] = '1'
    if kit.exists():
        env['PATH'] = str(kit) + ':' + str(kit.parent / 'sysroot/usr/bin') + ':' + env['PATH']
    commit = git(source, 'rev-parse', 'HEAD')
    log = prefix / 'log' / f'build-{commit}.log'; log.parent.mkdir(exist_ok=True)
    started = time.monotonic()
    build = source / 'build'
    with log.open('a') as stream:
        def checked(cmd):
            stream.write('\n' + shlex.join([str(x) for x in cmd]) + '\n'); stream.flush()
            run(cmd, cwd=source, env=env, stdout=stream, stderr=subprocess.STDOUT)
        checked(['/usr/bin/python3', '-B', '-c',
                 'import numpy,pyarrow; print("numpy",numpy.__version__,"pyarrow",pyarrow.__version__)'])
        cmd = [cmake, '-S', source, '-B', build, '-DCMAKE_BUILD_TYPE=Release',
               '-DPython3_EXECUTABLE=/usr/bin/python3', '-DCMAKE_DISABLE_FIND_PACKAGE_Arrow=ON']
        if kit.exists():
            cmd += ['-DCMAKE_CXX_COMPILER=' + str(kit / 'c++'),
                    '-DCMAKE_MAKE_PROGRAM=' + str(kit / 'make'),
                    '-DCMAKE_AR=' + str(kit.parent / 'sysroot/usr/bin/ar'),
                    '-DCMAKE_RANLIB=' + str(kit.parent / 'sysroot/usr/bin/ranlib'),
                    '-DCMAKE_LINKER=' + str(kit.parent / 'sysroot/usr/bin/ld')]
        checked(cmd)
        checked([cmake, '--build', build, '-j', str(jobs)])
        checked([ctest, '--test-dir', build, '--output-on-failure'])
        checked(['/usr/bin/python3', '-B', '-m', 'unittest', 'discover', '-s', 'tests/python', '-v'])
    result = dict(commit=commit, built_at=datetime.datetime.now().isoformat(),
                  build_seconds=time.monotonic() - started, source=str(source),
                  python_root=str(python_root), log=str(log), checks='ctest+python_unittest')
    atomic_json(build / 'deployment.json', result)
    return result


def install_wrappers(prefix):
    for folder in ('bin', 'config', 'run', 'log', 'data', 'references'):
        (prefix / folder).mkdir(exist_ok=True)
    config = prefix / 'config/live.json'
    if not config.exists():
        atomic_json(config, dict(journal_root='/data/cms/data/journal',
                                weights_dir=str(prefix / 'references/weights'),
                                threshold_dir=str(prefix / 'references/thresholds'),
                                output_root=str(prefix / 'data'), workers=8, pending_mib=256))
    for command in ('start', 'stop', 'status'):
        path = prefix / 'bin' / (command + '.sh')
        body = ('#!/bin/sh\nset -eu\nexec /usr/bin/python3 -B ' +
                shlex.quote(str(prefix / 'current/deploy/service.py')) + ' --prefix ' +
                shlex.quote(str(prefix)) + ' ' + command + ' "$@"\n')
        if path.exists() and path.read_text() != body:
            raise RuntimeError('existing wrapper differs: ' + str(path))
        if not path.exists():
            path.write_text(body); path.chmod(0o755)
    # An example only: source deployment never edits cron or starts a service.
    (prefix / 'crontab.example').write_text(
        '12 9 * * 1-5 ' + shlex.quote(str(prefix / 'bin/start.sh')) +
        ' >> ' + shlex.quote(str(prefix / 'log/cron.log')) + ' 2>&1\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prefix', type=Path, required=True)
    p.add_argument('--bundle', type=Path)
    p.add_argument('--bundle-sha256')
    p.add_argument('--commit', required=True)
    p.add_argument('--basis')
    p.add_argument('--build-jobs', type=int, default=2)
    p.add_argument('--python-root', type=Path, default=Path('/data/cms/archive/current/python'))
    p.add_argument('--build-tools', type=Path)
    p.add_argument('--build-tools-sha256')
    a = p.parse_args()
    if not 1 <= a.build_jobs <= 16 or not a.prefix.is_absolute() or len(a.prefix.parts) < 3:
        p.error('invalid prefix/jobs')
    prefix = a.prefix.resolve(); prefix.mkdir(parents=True, exist_ok=True)
    with (prefix / 'deploy.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if a.bundle and hashlib.sha256(a.bundle.read_bytes()).hexdigest() != a.bundle_sha256:
            raise RuntimeError('source bundle hash mismatch')
        if a.build_tools:
            install_build_tools(prefix, a.build_tools, a.build_tools_sha256)
        source = sync_source(prefix, a.bundle, a.commit, a.basis)
        result = build_release(prefix, source, a.build_jobs, a.python_root)
        install_wrappers(prefix)
        current = prefix / 'current'
        if current.exists() and not current.is_symlink():
            raise RuntimeError('current must be a symlink')
        temp = prefix / '.current.next'; temp.unlink(missing_ok=True)
        temp.symlink_to(source.relative_to(prefix)); temp.replace(current)
        atomic_json(prefix / 'deployment.json', result)
        print(json.dumps(dict(status='built_and_activated', **result)))


if __name__ == '__main__':
    main()
