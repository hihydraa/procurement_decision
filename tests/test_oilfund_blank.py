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


class BackfillBlankTest(unittest.TestCase):
    D = 46320          # serial ของวันที่ใดก็ได้ ; เทียบกันเองในเทสต์

    def _row(self, serial, cash, sub=""):
        return {"Date": serial, "Cash_Remaining (ล้านบาท)": cash, "Daily_Subsidy (ล้านบาท/วัน)": sub}

    def test_totals_sum_per_day(self):
        eppo = [{"Date": self.D, "Estimated Subsidy (mn baht/day)": 100, "Estimated Collection (mn baht/day)": 0},
                {"Date": self.D, "Estimated Subsidy (mn baht/day)": 50.5, "Estimated Collection (mn baht/day)": 10},
                {"Date": None}]
        t = I.eppo_daily_totals(eppo)
        self.assertEqual(t[I.excel_serial_to_date(self.D)], (150.5, 10.0))

    def test_fills_only_blank_rows_with_eppo_data(self):
        d0, d1, d2 = self.D, self.D + 1, self.D + 2
        existing = [self._row(d0, 500.0, 200.0), self._row(d1, 400.0), self._row(d2, 300.0)]
        totals = {I.excel_serial_to_date(d1): (200.0, 20.0)}           # d2 ยังไม่มีข้อมูล EPPO
        fills = I.oilfund_blank_fills(existing, totals, {"critical_runway_days": 20, "watch_runway_days": 35})
        self.assertEqual(fills, [(3, [200.0, 20.0, -180.0, 2.0, "CRITICAL"])])

    def test_negative_cash_gives_negative_runway(self):
        existing = [self._row(self.D, -1490.47)]
        fills = I.oilfund_blank_fills(existing, {I.excel_serial_to_date(self.D): (250.0, 0.0)}, {})
        self.assertEqual(fills[0][1][3], -5.96)


if __name__ == "__main__":
    unittest.main()
