"""Read/validate CMSBLK01 payloads for logical byte comparisons, not aggregation."""
import hashlib
from pathlib import Path
import struct


def logical_digest(path, record_size, expand_books=False):
    path = Path(path)
    h = hashlib.sha256()
    with path.open('rb') as f:
        if path.suffix != '.cmsb':
            for block in iter(lambda: f.read(8 << 20), b''):
                h.update(block)
            return h.hexdigest()
        magic, size, entry_size = struct.unpack('<8sII', f.read(16))
        if (magic, size, entry_size) != (b'CMSBLK01', record_size, 16):
            raise ValueError('unsupported block format')
        f.seek(-32, 2)
        magic, offset, count, records = struct.unpack('<8sQQQ', f.read(32))
        if magic != b'CMSIDX01' or offset + count * 16 + 32 != path.stat().st_size:
            raise ValueError('invalid block footer')
        f.seek(offset)
        entries = list(struct.iter_unpack('<iIQ', f.read(count * 16)))
        next_offset, actual = 16, 0
        for symbol, n, start in entries:
            if not 0 <= symbol < 1000000 or not 0 < n <= 512 or start != next_offset:
                raise ValueError('invalid block index')
            next_offset += n * size
            actual += n
        if next_offset != offset or records != actual:
            raise ValueError('inconsistent block counts')
        entries.sort(key=lambda x: x[0])
        if expand_books:
            import itertools
            import numpy as np
            from .aggregate import BOOK_REC
            from .catalog import endpoint_schedule_ms
            if size != BOOK_REC.itemsize:
                raise ValueError('book expansion requires BookRec280')
            grid = endpoint_schedule_ms()
            for symbol, group in itertools.groupby(entries, key=lambda x: x[0]):
                parts = []
                for _, n, start in group:
                    f.seek(start)
                    parts.append(f.read(n * size))
                records = np.frombuffer(b''.join(parts), dtype=BOOK_REC)
                if np.any(records['symbol'] != symbol) or np.any(records['ep_ms'][1:] < records['ep_ms'][:-1]):
                    raise ValueError('invalid book group')
                first = np.searchsorted(grid, records['ep_ms'][0])
                endpoints = grid[first:]
                selected = np.searchsorted(records['ep_ms'], endpoints, side='right') - 1
                # Structured fancy indexing copies fields but leaves implicit
                # trailing alignment bytes unspecified. Copy complete records
                # so this oracle checks the original binary ABI too.
                expanded = records.view(np.uint8).reshape(-1, size)[selected].reshape(-1).view(BOOK_REC)
                expanded['ep_ms'] = endpoints
                h.update(expanded.tobytes())
            return h.hexdigest()
        for symbol, n, start in entries:
            f.seek(start)
            blob = f.read(n * size)
            if len(blob) != n * size or any(struct.unpack_from('<i', blob, i * size)[0] != symbol
                                           for i in range(n)):
                raise ValueError('corrupt block payload')
            h.update(blob)
    return h.hexdigest()
