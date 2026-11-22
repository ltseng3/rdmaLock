import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from e2e_results import extract


class ExtractTest(unittest.TestCase):
    line = ("LOCK_E2E positions=100 workers=2 completed=100 violations=0 "
            "verified_markers=100 ordered_entries=101 probe=1 work_ns=0 "
            "ordering_seconds=0.1 handoff_seconds=0.01 "
            "goodput_ops_per_sec=10000 including_ordering_ops_per_sec=909 "
            "status=PASS")

    def test_success(self):
        obj = extract("other output\n" + self.line + "\n")
        self.assertEqual(obj["goodput_ops_per_sec"], "10000")

    def test_mismatch(self):
        with self.assertRaises(ValueError):
            extract(self.line.replace("completed=100", "completed=99"))

    def test_missing_markers(self):
        with self.assertRaises(ValueError):
            extract(self.line.replace("verified_markers=100", "verified_markers=98"))

    def test_failure(self):
        with self.assertRaises(ValueError):
            extract(self.line.replace("status=PASS", "status=FAIL"))

    def test_missing_line(self):
        with self.assertRaises(ValueError):
            extract("nothing completed")


if __name__ == "__main__":
    unittest.main()
