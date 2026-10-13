#!/usr/bin/env python3
"""Generate the C++ names from the existing 106-column contract."""
import importlib.util
from pathlib import Path
import sys

source, output = map(Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location('catalog', source)
catalog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(catalog)
output.parent.mkdir(parents=True, exist_ok=True)
text = '#pragma once\n#include <array>\nnamespace cms_rtf {\n'
for name, values in [('kColumnNames', catalog.COLUMN_NAMES), ('kIndexCodes', catalog.INDEX_CODES)]:
    text += f'inline constexpr std::array<const char*, {len(values)}> {name} = {{\n'
    text += ''.join(f'  "{value}",\n' for value in values) + '};\n'
text += '}\n'
output.write_text(text)
