"""Reference preparation and invocation only; aggregation/Arrow writing are C++."""
import json
from pathlib import Path
import struct
import subprocess

import numpy as np


DEFAULT_BINARY = Path(__file__).resolve().parents[2] / 'build/cms_index500ms_aggregate'


def write_reference(ref, path):
    """CMSAG01: magic[8], u32 count, 4*f64 official, count*(i32 symbol+4*f64).

    Preserve the established NumPy official-weight sums exactly, including their
    rounding. This versioned side input does not change the existing record ABI.
    """
    symbols = np.asarray(ref['symbols'], dtype=np.int32)
    weights = np.asarray(ref['weights'], dtype=np.float64)
    official = np.asarray(ref['official'], dtype=np.float64)
    if weights.shape != (4, len(symbols)) or official.shape != (4,):
        raise ValueError('invalid aggregate reference dimensions')
    if (np.any(symbols < 0) or np.any(symbols >= 1000000) or
            np.any(symbols[1:] <= symbols[:-1]) or
            not np.all(np.isfinite(weights)) or np.any(weights < 0) or
            not np.all(np.isfinite(official)) or np.any(official < 0)):
        raise ValueError('invalid aggregate reference values/order')
    with open(path, 'wb') as f:
        f.write(struct.pack('<8sI4d', b'CMSAG01\0', len(symbols), *official))
        for i, symbol in enumerate(symbols):
            f.write(struct.pack('<i4d', int(symbol), *weights[:, i]))


def command(books, flows, seeds, reference, day, output, binary=None):
    args = [str(binary or DEFAULT_BINARY), '--ref', str(reference), '--day', day, '--out', str(output)]
    for option, paths in [('--book', books), ('--flow', flows), ('--seed', seeds)]:
        paths = [paths] if isinstance(paths, (str, Path)) else paths
        for path in paths:
            args += [option, str(path)]
    return args


def run(books, flows, seeds, ref, day, output, binary=None):
    output = Path(output)
    reference = output.with_suffix('.aggregate-ref.bin')
    write_reference(ref, reference)
    result = subprocess.run(command(books, flows, seeds, reference, day, output, binary),
                            capture_output=True, text=True, check=True)
    summary = json.loads(result.stdout)
    output.with_suffix('.aggregate.json').write_text(json.dumps(summary, indent=2) + '\n')
    return summary['rows']


def run_backend(backend, books, flows, seeds, ref, day, output, binary=None):
    if backend == 'cpp':
        return run(books, flows, seeds, ref, day, output, binary)
    if backend != 'python':
        raise ValueError(f'unknown aggregation backend: {backend}')
    from . import aggregate
    grid, rows = aggregate.aggregate(books, flows, seeds, ref, day)
    return aggregate.write_arrow(grid, rows, day, output)
