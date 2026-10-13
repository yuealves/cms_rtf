#!/usr/bin/env python3
"""Send only missing Git commits; compile and test on the CMS host.

Based on Huatai sync_online_bundle.sh. No hard reset, system package install,
automatic process restart, or cron mutation. Failed builds leave current intact.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def git(*args):
    return subprocess.check_output(['git', '-C', str(ROOT), *args], text=True).strip()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--via', default='gy_dev', help='SSH relay; empty when running on gy_dev')
    p.add_argument('--remote', default='linwei@127.0.0.1')
    p.add_argument('--port', type=int, default=15423)
    p.add_argument('--prefix', default='/data/cms/rtf')
    p.add_argument('--build-jobs', type=int, default=2)
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--build-tools', type=Path, help='optional one-time build kit archive')
    p.add_argument('--remote-build-tools', help='already uploaded build kit archive')
    p.add_argument('--build-tools-sha256')
    a = p.parse_args()
    if not 1 <= a.port <= 65535 or a.remote.startswith('-') or a.via.startswith('-') or not a.prefix.startswith('/'):
        p.error('invalid remote/port/prefix')
    if git('status', '--porcelain'):
        p.error('commit source changes before deployment')
    head = git('rev-parse', 'HEAD')
    def ssh(command, **kwargs):
        base = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', '-p', str(a.port), a.remote, command]
        return subprocess.run(['ssh', a.via, shlex.join(base)] if a.via else base,
                              check=True, **kwargs)
    probe = ('import pathlib,json,subprocess; p=pathlib.Path(' + repr(a.prefix) + '); '
             'r=subprocess.run(["git","-C",str(p/"repo.git"),"rev-parse","--verify","refs/heads/source"],capture_output=True,text=True); '
             'print(json.dumps({"basis":r.stdout.strip() if r.returncode==0 else None}))')
    basis = json.loads(ssh('python3 -B -c ' + shlex.quote(probe), capture_output=True, text=True).stdout)['basis']
    if basis:
        git('merge-base', '--is-ancestor', basis, head)
    print(json.dumps(dict(commit=head, basis=basis, dry_run=a.dry_run)), flush=True)
    if a.dry_run:
        return
    incoming = a.prefix + '/incoming'
    ssh('mkdir -p ' + shlex.quote(incoming))
    def upload(local, remote):
        with Path(local).open('rb') as f:
            ssh('cat > ' + shlex.quote(remote + '.part'), stdin=f)
        ssh('mv -- ' + shlex.quote(remote + '.part') + ' ' + shlex.quote(remote))
    args = ['--prefix', a.prefix, '--commit', head, '--build-jobs', str(a.build_jobs)]
    if basis:
        args += ['--basis', basis]
    with tempfile.TemporaryDirectory(prefix='cms-rtf-bundle-') as tmp:
        if basis != head:
            bundle = Path(tmp) / 'source.bundle'
            git('bundle', 'create', str(bundle), 'HEAD', *(['^' + basis] if basis else []))
            git('bundle', 'verify', str(bundle))
            dest = incoming + '/' + head + '.bundle'
            upload(bundle, dest)
            args += ['--bundle', dest, '--bundle-sha256', hashlib.sha256(bundle.read_bytes()).hexdigest()]
            print(json.dumps(dict(source_bundle_bytes=bundle.stat().st_size)), flush=True)
        if a.build_tools:
            dest = incoming + '/' + a.build_tools.name
            upload(a.build_tools, dest)
            args += ['--build-tools', dest, '--build-tools-sha256', hashlib.sha256(a.build_tools.read_bytes()).hexdigest()]
        elif a.remote_build_tools:
            if not a.build_tools_sha256:
                p.error('--remote-build-tools requires its SHA256')
            args += ['--build-tools', a.remote_build_tools, '--build-tools-sha256', a.build_tools_sha256]
        helper = incoming + '/apply-' + head + '.py'
        upload(ROOT / 'deploy/apply_bundle.py', helper)
        ssh(shlex.join(['nice', '-n', '10', '/usr/bin/python3', '-B', helper, *args]))


if __name__ == '__main__':
    main()
