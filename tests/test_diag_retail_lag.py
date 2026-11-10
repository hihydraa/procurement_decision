import unittest

import pandas as pd

import diag_retail_lag as dr

HEAD = ["Date", "oil type", "RETAIL", "Source"]


def vals(rows):
    return [HEAD] + [list(r) for r in rows]


class Retail(unittest.TestCase):
    def test_daily_filters(self):
        v = vals([("10/1/2026", "H-DIESEL", 40.0, "FRESH"), ("10/2/2026", "H-DIESEL", 40.0, "CARRIED"),
                  ("10/3/2026", "H-DIESEL", 40.5, "FRESH"), ("10/3/2026", "LPG (BAHT/KILOGRAM)", 25.0, "FRESH"),
                  ("10/3/2026", "GASOHOL95 E10", "", "FRESH")])
        d = dr.retail_daily(v)
        self.assertEqual(list(d.fuel), ["B7", "B7"])
        self.assertEqual(list(d.retail), [40.0, 40.5])

    def test_changes_gap_flag(self):
        v = vals([("10/1/2026", "H-DIESEL", 40.0, "FRESH"), ("10/2/2026", "H-DIESEL", 40.5, "FRESH"), ("10/5/2026", "H-DIESEL", 40.0, "FRESH")])
        c = dr.retail_changes(dr.retail_daily(v))
        self.assertEqual(list(c.gap_days), [1, 3])
        self.assertEqual(list(c.chg), [0.5, -0.5])
        out = dr.retail_report(c)
        self.assertIn("ปรับขึ้น 1 ลง 0", out[0])      # เฉพาะคู่ที่ห่าง 1 วัน
        self.assertIn("ข้อมูลขาดวัน 1", out[0])


class Pass(unittest.TestCase):
    def test_pass_through_lag(self):
        days = pd.date_range("2026-10-01", periods=8)
        ws = pd.Series([30, 30, 30.5, 30.5, 30.5, 30.5, 30.5, 30.5], index=days)           # ส่งขึ้น 0.5 วันที่ 3
        retail = pd.DataFrame({"day": days, "retail": [40, 40, 40, 40.5, 40.5, 40.5, 40.5, 40.5]})   # ปลีกตามช้า 1 วัน
        n, res, ratio = dr.pass_through(ws, retail)
        self.assertEqual(n, 1)
        self.assertEqual(res, {0: 0, 1: 1, 2: 1, 3: 1})
        self.assertAlmostEqual(ratio, 1.0)

    def test_no_events(self):
        days = pd.date_range("2026-10-01", periods=4)
        n, _, ratio = dr.pass_through(pd.Series([30.0] * 4, index=days), pd.DataFrame({"day": days, "retail": [40.0] * 4}))
        self.assertEqual(n, 0)
        self.assertTrue(ratio != ratio)


class Forecast(unittest.TestCase):
    def test_tab(self):
        days = pd.date_range("2026-11-01", periods=5)
        retail = pd.DataFrame({"day": days, "fuel": "B7", "retail": [40, 40, 40.5, 40.5, 40.5]})
        o = pd.DataFrame([{"ref": pd.Timestamp("2026-11-02 20:30"), "fuel": "B7", "hours": 24, "tier": "UP_STRONG"}])
        out = dr.forecast_report(retail, o)
        self.assertTrue(any("ทำนาย ขึ้น 1 วัน" in x and "1วัน ↑1" in x for x in out))


if __name__ == "__main__":
    unittest.main()
