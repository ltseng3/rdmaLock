import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from record_run import parse


class TestParser(unittest.TestCase):
    def test_ordering(self):
        row = parse('One-sided RDMA phase2 committed=1000 seconds=0.125 ordered_entries_per_sec=8000\n', 'ordering')
        self.assertEqual(row['completed'], 1000)
        self.assertEqual(row['mode'], 'RDMA_PHASE2')
        self.assertEqual(row['mops'], '0.008000')

    def test_release(self):
        row = parse('RDMA release mode=CAS operations=100000 seconds=0.5 ops_per_sec=200000\n', 'release')
        self.assertEqual(row['completed'], 100000)
        self.assertEqual(row['mode'], 'CAS')

    def test_failure(self):
        with self.assertRaises(ValueError): parse('ERROR: IB inactive', 'ordering')

    def test_inconsistent(self):
        with self.assertRaises(ValueError): parse('RDMA release mode=CAS operations=10 seconds=1 ops_per_sec=1000', 'release')


if __name__ == '__main__': unittest.main()
