#!/usr/bin/env python3
"""Fetch APT-verified compiler packages and extract into a private build kit.

This does not run apt install/dpkg -i or alter the system package database.
Use when the CMS host can reach Ubuntu mirrors, avoiding a large SSH transfer.
"""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import urllib.parse
import urllib.request

PACKAGES = ['gcc-13-x86-64-linux-gnu', 'g++-13-x86-64-linux-gnu', 'cpp-13-x86-64-linux-gnu',
            'libgcc-13-dev', 'libstdc++-13-dev', 'libc6-dev', 'linux-libc-dev',
            'binutils', 'binutils-x86-64-linux-gnu', 'binutils-common', 'libbinutils',
            'cmake', 'cmake-data', 'make', 'libmpc3', 'libisl23', 'libcc1-0',
            'libgomp1', 'libjsoncpp25', 'librhash0', 'libuv1t64', 'libarchive13t64',
            'libstdc++6', 'libgcc-s1', 'libsframe1', 'libctf0', 'libctf-nobfd0']


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--prefix', type=Path, default=Path('/data/cms/rtf'))
    p.add_argument('--refresh-metadata', action='store_true', help='refresh APT indexes in this prefix only')
    a = p.parse_args(); prefix = a.prefix.resolve()
    if 'VERSION_ID="24.04"' not in Path('/etc/os-release').read_text():
        p.error('Ubuntu 24.04 required')
    root = prefix / 'toolchains/ubuntu24-gcc13-native'
    cache = prefix / 'incoming/build-debs'; cache.mkdir(parents=True, exist_ok=True)
    apt = ['apt-get']
    if a.refresh_metadata:
        lists = cache / 'lists'; (lists / 'partial').mkdir(parents=True, exist_ok=True)
        sources = cache / 'ubuntu.list'
        sources.write_text(''.join('deb [arch=amd64 signed-by=/usr/share/keyrings/ubuntu-archive-keyring.gpg] '
                                  f'https://archive.ubuntu.com/ubuntu {suite} main\n'
                                  for suite in ('noble', 'noble-updates', 'noble-security')))
        apt += ['-o', 'Dir::Etc::sourcelist=' + str(sources), '-o', 'Dir::Etc::sourceparts=-',
                '-o', 'Dir::State::lists=' + str(lists), '-o', 'Acquire::Languages=none',
                '-o', 'Dir::Cache::pkgcache=' + str(cache / 'pkgcache.bin'),
                '-o', 'Dir::Cache::srcpkgcache=' + str(cache / 'srcpkgcache.bin'),
                '-o', 'APT::Sandbox::User=' + __import__('getpass').getuser(),
                '-o', 'Acquire::IndexTargets::deb::DEP-11::DefaultEnabled=false']
        subprocess.run([*apt, 'update'], check=True)
    # apt-get suppresses URIs for files already present in cwd. Query from an
    # empty directory; reuse verified downloads ourselves in the separate cache.
    query = cache / 'uri-query'; query.mkdir(exist_ok=True)
    lines = subprocess.check_output([*apt, '--print-uris', 'download', *PACKAGES],
                                    cwd=query, text=True).splitlines()
    specs = [shlex.split(s) for s in lines if s.startswith("'")]
    if len(specs) != len(PACKAGES):
        raise RuntimeError('incomplete APT package selection')
    def download(spec):
        url, name, size, checksum = spec
        algorithm, digest = checksum.split(':', 1)
        if algorithm not in ('SHA256', 'SHA512'):
            raise RuntimeError('strong APT hash required')
        path = cache / name
        def valid():
            return (path.exists() and path.stat().st_size == int(size) and
                    hashlib.new(algorithm.lower(), path.read_bytes()).hexdigest() == digest)
        if valid():
            return path
        parts = urllib.parse.urlsplit(url)
        # Paths/hashes come from the host's authenticated APT metadata. Try its
        # configured mirror first; archive.ubuntu.com is an equivalent fallback.
        urls = [urllib.parse.urlunsplit(('https', parts.netloc, parts.path, '', '')),
                urllib.parse.urlunsplit(('https', 'archive.ubuntu.com', parts.path, '', ''))]
        error = None
        for candidate in urls:
            try:
                with urllib.request.urlopen(candidate, timeout=45) as src, path.with_suffix('.part').open('wb') as out:
                    while block := src.read(1024 * 1024):
                        out.write(block)
                path.with_suffix('.part').replace(path)
                if not valid():
                    raise RuntimeError('APT hash mismatch: ' + name)
                print(json.dumps(dict(downloaded=name, bytes=int(size))), flush=True)
                return path
            except Exception as exc:
                error = exc
        raise RuntimeError(f'cannot download {name}: {error}')
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        paths = list(pool.map(download, specs))
    if (root / 'manifest.json').exists():
        subprocess.run([str(root / 'bin/cmake'), '--version'], check=True)
        print(json.dumps(dict(status='build_tools_already_ready', path=str(root))), flush=True)
        return
    sysroot = root / 'sysroot'; sysroot.mkdir(parents=True, exist_ok=True)
    # A failed compiler probe may leave our two native-library links. Remove only
    # those links before retrying extraction, so dpkg-deb cannot traverse /lib.
    for name in ('lib', 'lib64'):
        link = sysroot / name
        if link.is_symlink():
            if os.readlink(link) != '/' + name:
                raise RuntimeError('unexpected sysroot symlink')
            link.unlink()
    for path in paths:
        subprocess.run(['dpkg-deb', '-x', str(path), str(sysroot)], check=True)
    for name in ('lib', 'lib64'):
        if not (sysroot / name).exists():
            (sysroot / name).symlink_to('/' + name, target_is_directory=True)
    bindir = root / 'bin'; bindir.mkdir(exist_ok=True)
    for command, actual in [('cc', 'x86_64-linux-gnu-gcc-13'), ('c++', 'x86_64-linux-gnu-g++-13'),
                            ('cmake', 'cmake'), ('ctest', 'ctest'), ('make', 'make')]:
        extra = ' --sysroot="$kit/sysroot" -B"$kit/sysroot/usr/lib/x86_64-linux-gnu/" -B"$kit/sysroot/usr/bin/"' if command in ('cc', 'c++') else ''
        (bindir / command).write_text('#!/bin/sh\nset -eu\n'
            'kit=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)\n'
            'export LD_LIBRARY_PATH="$kit/sysroot/usr/lib/x86_64-linux-gnu${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"\n'
            f'exec "$kit/sysroot/usr/bin/{actual}"{extra} "$@"\n')
        (bindir / command).chmod(0o755)
    test = root / 'probe.cpp'
    test.write_text('#include <thread>\n#include <iostream>\nint main(){std::thread t([]{});t.join();std::cout<<"native build tools OK\\n";}\n')
    subprocess.run([str(bindir / 'c++'), '-std=c++17', str(test), '-o', str(root / 'probe')], check=True)
    subprocess.run([str(root / 'probe')], check=True)
    subprocess.run([str(bindir / 'cmake'), '--version'], check=True)
    (root / 'manifest.json').write_text(json.dumps(dict(os='Ubuntu 24.04', arch='x86_64', packages=specs), indent=2) + '\n')
    link = prefix / '.build-tools.next'; link.unlink(missing_ok=True)
    link.symlink_to(root.relative_to(prefix)); link.replace(prefix / 'build-tools')
    print(json.dumps(dict(status='build_tools_ready', path=str(root))), flush=True)


if __name__ == '__main__':
    main()
