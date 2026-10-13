#!/usr/bin/env python3
"""Prepare a user-owned Ubuntu 24.04 build kit; never install system packages.

Run on an Ubuntu 24.04 development host with GCC 13/CMake/Make installed. The
factor executable is NOT included: it will be compiled on the destination host.
Only the initial bootstrap needs this archive; source updates use git bundles.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

PACKAGES = ['gcc-13-x86-64-linux-gnu', 'g++-13-x86-64-linux-gnu',
            'cpp-13-x86-64-linux-gnu', 'libgcc-13-dev', 'libstdc++-13-dev',
            'libc6-dev', 'linux-libc-dev', 'binutils-x86-64-linux-gnu',
            'binutils-common', 'libbinutils', 'cmake', 'cmake-data', 'make']
GLIBC = re.compile(r'^(libc\.so|libm\.so|libmvec\.so|libpthread\.so|libdl\.so|librt\.so|libresolv\.so|ld-linux)')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    release = Path('/etc/os-release').read_text()
    if 'VERSION_ID="24.04"' not in release:
        p.error('build kit must be prepared on Ubuntu 24.04')
    root = a.out.resolve()
    root.mkdir(parents=True, exist_ok=False)
    sysroot = root / 'sysroot'
    runtime = root / 'runtime'; runtime.mkdir()
    versions = {}
    elf_files = []
    for package in PACKAGES:
        versions[package] = subprocess.check_output(
            ['dpkg-query', '-W', '-f=${Version}', package], text=True)
        names = subprocess.check_output(['dpkg-query', '-L', package], text=True).splitlines()
        for name in names:
            src = Path(name)
            if not src.is_file():
                continue
            if not any(name.startswith(x) for x in ('/usr/bin/', '/usr/lib/', '/usr/libexec/',
                                                    '/usr/include/', '/usr/share/cmake-',
                                                    '/usr/share/doc/')):
                continue
            dest = sysroot / name.lstrip('/')
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)  # dereference development-package symlinks
            with src.open('rb') as f:
                if f.read(4) == b'\x7fELF':
                    elf_files.append(src)
    clean_env = dict(os.environ, LD_LIBRARY_PATH='')
    for src in elf_files:
        result = subprocess.run(['/usr/bin/ldd', str(src)], env=clean_env,
                                capture_output=True, text=True)
        if 'not found' in result.stdout:
            raise RuntimeError(result.stdout)
        for name, path in re.findall(r'^\s*(\S+) => (/\S+) ', result.stdout, re.M):
            if not GLIBC.match(name) and not (runtime / name).exists():
                shutil.copy2(path, runtime / name)
    # Link against the destination's glibc; no loader/libc is replaced or put in
    # LD_LIBRARY_PATH. Development headers match Ubuntu 24.04/glibc 2.39.
    (sysroot / 'lib').symlink_to('/lib', target_is_directory=True)
    (sysroot / 'lib64').symlink_to('/lib64', target_is_directory=True)
    bindir = root / 'bin'; bindir.mkdir()
    for command, actual in [('cc', 'x86_64-linux-gnu-gcc-13'), ('c++', 'x86_64-linux-gnu-g++-13'),
                            ('cmake', 'cmake'), ('ctest', 'ctest'), ('make', 'make')]:
        extra = ' --sysroot="$kit/sysroot" -B"$kit/sysroot/usr/lib/x86_64-linux-gnu/" -B"$kit/sysroot/usr/bin/"' if command in ('cc', 'c++') else ''
        script = ('#!/bin/sh\nset -eu\n'
                  'kit=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)\n'
                  'export LD_LIBRARY_PATH="$kit/runtime${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"\n'
                  f'exec "$kit/sysroot/usr/bin/{actual}"{extra} "$@"\n')
        (bindir / command).write_text(script); (bindir / command).chmod(0o755)
    manifest = {'schema': 1, 'os': 'Ubuntu 24.04', 'arch': 'x86_64',
                'minimum_glibc': '2.39', 'packages': versions}
    manifest['files'] = {str(f.relative_to(root)): hashlib.sha256(f.read_bytes()).hexdigest()
                         for base in (sysroot / 'usr', runtime, bindir)
                         for f in base.rglob('*') if f.is_file()}
    (root / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    archive = Path(str(root) + '.tar.gz')
    with tarfile.open(archive, 'w:gz', compresslevel=5) as tf:
        tf.add(root, arcname=root.name)
    print(json.dumps({'archive': str(archive), 'bytes': archive.stat().st_size,
                      'sha256': hashlib.sha256(archive.read_bytes()).hexdigest()}))


if __name__ == '__main__':
    main()
