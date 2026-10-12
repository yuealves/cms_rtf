"""Acceptance checks must reject differences hidden by numeric equality."""
import unittest

import numpy as np
import pyarrow as pa

from cms_rtf.validate_replay import bitwise_column, exact_factors
from cms_rtf.catalog import COLUMN_NAMES


def column(values):
    return pa.chunked_array([pa.array(values, type=pa.float32())])


class ReplayValidationTest(unittest.TestCase):
    def test_equal_with_nulls_and_different_chunks(self):
        a = column([1, None, 2])
        b = pa.chunked_array([pa.array([1, None], type=pa.float32()),
                              pa.array([2], type=pa.float32())])
        self.assertTrue(bitwise_column(a, b)['exact'])

    def test_signed_zero_is_not_bitwise_equal(self):
        self.assertEqual(bitwise_column(column([0.0]), column([-0.0]))['valid_bit_mismatches'], 1)

    def test_equal_null_counts_different_positions_fail(self):
        report = bitwise_column(column([None, 1, 2]), column([1, None, 2]))
        self.assertEqual(report['null_mask_mismatches'], 2)
        self.assertFalse(report['exact'])

    def test_nan_payload_and_null_are_distinct(self):
        a = np.array([0x7fc00001], dtype=np.uint32).view(np.float32)
        b = np.array([0x7fc00002], dtype=np.uint32).view(np.float32)
        self.assertFalse(bitwise_column(column(a), column(b))['exact'])
        self.assertFalse(bitwise_column(column(a), column([None]))['exact'])

    def test_dtype_and_length_fail(self):
        with self.assertRaises(ValueError):
            bitwise_column(column([1]), pa.chunked_array([[1.0]]))
        with self.assertRaises(ValueError):
            bitwise_column(column([1]), column([1, 2]))

    def test_duplicate_keys_fail(self):
        cols = {'datetime': pa.array([1, 1], type=pa.timestamp('us')), 'code': ['A', 'A']}
        cols.update({n: pa.array([1, 1], type=pa.float32()) for n in COLUMN_NAMES})
        table = pa.table(cols)
        self.assertFalse(exact_factors(table, table)['keys_unique'])


if __name__ == '__main__':
    unittest.main()
