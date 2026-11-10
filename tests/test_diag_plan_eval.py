import unittest

import pandas as pd

import diag_plan_eval as pe

HEAD = ["plan_date", "created_at", "run", "branch", "fuel", "depot", "mops_date", "mops_change", "pred_change", "tap_time", "depart_time", "risk"]


def plan_row(day, branch, fuel, depot, tap):
    return [day, "x", "21:34", branch, fuel, depot, "", "", "", tap, "", ""]


def prices(rows):
    return pd.DataFrame([{"jobber": "ต้องบุญ", "provider": p, "warehouse": w, "fuel": f, "effective": pd.Timestamp(t), "price": x} for p, w, f, t, x in rows])


class Units(unittest.TestCase):
    def test_dedupe_by_branch_and_depot_mapping(self):
        v = [HEAD, plan_row("2026-11-08", "ยางตลาด", "B7", "IRPC นครหลวง", "16:30"), plan_row("2026-11-08", "กระนวน", "B7", "IRPC นครหลวง", "16:30"),
             plan_row("2026-11-08", "ยางตลาด", "G95", "IRPC ระยอง", "16:30"), plan_row("2026-11-08", "x", "B7", "บ้านไผ่", "16:30"),
             plan_row("2026-11-08", "x", "B7", "IRPC ระยอง", "เช้า")]
        u = pe.plan_units(v)
        self.assertEqual(len(u), 2)
        self.assertEqual(set(u.depot), {"ไออาร์ อยุธยา", "ไออาร์ ระยอง"})
        self.assertEqual(pe.tap_hours("16:30"), 16.5)
        self.assertIsNone(pe.tap_hours("เช้า"))


class Eval(unittest.TestCase):
    def test_regret_at_tap(self):
        d = pd.Timestamp("2026-11-08")
        # ไออาร์ ระยอง B7 : 40.0 เย็นวันก่อน → 39.5 เวลา 05:00 → 39.8 เวลา 14:00 ; ที่ 16:30 = 39.8 ; ต่ำสุดช่วง 39.5
        p = prices([("ไออาร์", "ระยอง", "B7", d - pd.Timedelta(hours=6), 40.0), ("ไออาร์", "ระยอง", "B7", d + pd.Timedelta(hours=5), 39.5),
                    ("ไออาร์", "ระยอง", "B7", d + pd.Timedelta(hours=14), 39.8), ("ไออาร์", "ระยอง", "B7", d + pd.Timedelta(hours=21), 39.8)])
        u = pd.DataFrame([{"day": d, "depot": "ไออาร์ ระยอง", "fuel": "B7", "hours": 16.5}])
        ev = pe.evaluate(u, p)
        self.assertAlmostEqual(ev.loc[0, "plan_a"], 0.3)
        out = pe.plan_report(u, ev)
        self.assertTrue(any("0.300" in x for x in out))

    def test_previous_day_interpretation(self):
        d = pd.Timestamp("2026-11-08")
        # วันก่อน 10:00 = 40.2 ; 17:00 = 39.9 ; ต่ำสุดในช่วง (D-1 06:00 → D 22:00) = 39.5 เวลา 05:00 วัน D
        p = prices([("ไออาร์", "ระยอง", "B7", d - pd.Timedelta(hours=14), 40.2), ("ไออาร์", "ระยอง", "B7", d - pd.Timedelta(hours=7), 39.9),
                    ("ไออาร์", "ระยอง", "B7", d + pd.Timedelta(hours=5), 39.5), ("ไออาร์", "ระยอง", "B7", d + pd.Timedelta(hours=21), 39.5)])
        u = pd.DataFrame([{"day": d, "depot": "ไออาร์ ระยอง", "fuel": "B7", "hours": 16.5}])
        ev = pe.evaluate(u, p)
        self.assertAlmostEqual(ev.loc[0, "plan_b"], 0.7)            # D-1 16:30 ราคา 40.2 − ต่ำสุด 39.5

    def test_incomplete_prices(self):
        u = pd.DataFrame([{"day": pd.Timestamp("2026-11-08"), "depot": "ไออาร์ ระยอง", "fuel": "B7", "hours": 16.5}])
        ev = pe.evaluate(u, prices([("ไออาร์", "ระยอง", "G95", "2026-11-07 10:00", 39.0)]))
        self.assertTrue(pd.isna(ev.loc[0, "plan_a"]))
        self.assertIn("ราคายังไม่ครบ", pe.plan_report(u, ev)[-1])


class Backtest(unittest.TestCase):
    def test_counts(self):
        v = [["target_date", "depot", "fuel", "mops_change", "pred_change", "actual_change", "direction"],
             ["2026-10-10", "IRPC ระยอง", "B7", "-6", "-0.7", "-1.0", "ถูกทิศ"], ["2026-10-13", "IRPC ระยอง", "B7", "-2", "-0.2", "0.4", "ผิดทิศ"],
             ["2026-10-13", "IRPC ระยอง", "G95", "-2", "-0.2", "-0.4", "ถูกทิศ"]]
        out = pe.backtest_report(v)
        self.assertIn("ถูกทิศ 2 ผิดทิศ 1", out[0])
        self.assertIn("67%", out[0])


if __name__ == "__main__":
    unittest.main()
