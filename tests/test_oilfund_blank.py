import unittest

import ingest as I

SUB, NET = "Daily_Subsidy (ล้านบาท/วัน)", "Net_Fund_Impact (ล้านบาท/วัน)"


class BlankCellsTest(unittest.TestCase):
    def test_to_float(self):
        self.assertEqual(I.to_float("12.5"), 12.5)
        self.assertEqual(I.to_float(3), 3.0)
        for bad in ("", None, "abc", True):
            self.assertIsNone(I.to_float(bad))

    def test_last_numeric_skips_trailing_blanks(self):
        rows = [{SUB: 240.0, NET: -247.0}, {SUB: "", NET: ""}, {SUB: None, NET: None}]
        self.assertEqual(I.last_numeric(rows, SUB), 240.0)
        self.assertEqual(I.last_numeric(rows, NET), -247.0)

    def test_last_numeric_prefers_most_recent_value(self):
        rows = [{SUB: 100.0}, {SUB: 247.0}, {SUB: ""}]
        self.assertEqual(I.last_numeric(rows, SUB), 247.0)

    def test_last_numeric_none_when_all_blank(self):
        self.assertIsNone(I.last_numeric([{SUB: ""}, {}], SUB))
        self.assertEqual(I.last_numeric([{SUB: ""}], SUB) or 0.0, 0.0)          # ingest_oilfund ใช้ 0 เมื่อไม่มีเลย


if __name__ == "__main__":
    unittest.main()
