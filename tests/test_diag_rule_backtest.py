import unittest

import numpy as np
import pandas as pd

import diag_rule_backtest as rb


class Parse(unittest.TestCase):
    def test_mops_wti_eppo_fund(self):
        m = rb.mops_daily([["Date", "Oil Type", "Price (USD/BBL)", "Type"], ["2/11/2026", "G95", "77.63", ""], ["2/11/2026", "DS", "89.72", ""], ["2/11/2026", "XX", "1", ""],
                           ["2/12/2026", "G95", "x", ""]])
        self.assertEqual(sorted(zip(m.fuel, m.mops)), [("B7", 89.72), ("G95", 77.63)])
        w = rb.wti_daily([["Date", "Price"], ["2/17/2026", "62.33"], ["2/18/2026", "bad"]])
        self.assertEqual(len(w), 1)
        e = rb.eppo_daily([["Date", "oil type", "WHOLESALE (WS)", "MARKETING MARGIN", "RETAIL", "Source"],
                           ["10/1/2026", "H-DIESEL", "37.0", "2.0", "42.0", "FRESH"], ["10/2/2026", "H-DIESEL", "37.0", "2.0", "42.0", "CARRIED"],
                           ["10/1/2026", "GASOHOL95 E10", "35.0", "3.3", "41.0", "FRESH"], ["10/1/2026", "LPG", "1", "1", "1", "FRESH"]])
        self.assertEqual(sorted(e.fuel), ["B7", "G95"])
        f = rb.fund_daily([["Date", "Total_Balance (ล้านบาท)", "Status"], ["2026-03-16", "-12605.00", "CRITICAL"], ["2026-06-01", "500", "OK"]])
        self.assertEqual(list(f.crisis), [True, False])


class Rule(unittest.TestCase):
    def feat(self, **kw):
        return {"m1": 1.0, "m3": 2.0, "w1": 0.5, "mm_gap": -0.3, "crisis": True, **kw}

    def test_variants(self):
        self.assertEqual(rb.classify(self.feat(), "ทั้งหมด(+กองทุน)"), "BUY")
        self.assertEqual(rb.classify(self.feat(crisis=False), "ทั้งหมด(+กองทุน)"), "UNCLEAR")        # ไม่วิกฤติ → ไม่ BUY
        self.assertEqual(rb.classify(self.feat(crisis=False), "MOPS+WTI+margin"), "BUY")
        self.assertEqual(rb.classify(self.feat(mm_gap=0.4), "MOPS+WTI+margin"), "UNCLEAR")           # ค่าการตลาดสูงกว่าค่ากลาง
        self.assertEqual(rb.classify(self.feat(w1=-0.2), "MOPS+WTI"), "UNCLEAR")                      # WTI ขัดแย้ง
        self.assertEqual(rb.classify(self.feat(m1=-1, m3=-2, w1=-1, crisis=False), "ทั้งหมด(+กองทุน)"), "WAIT")
        self.assertEqual(rb.classify(self.feat(m1=-1, m3=-2, w1=-1, crisis=True), "ทั้งหมด(+กองทุน)"), "UNCLEAR")   # กองทุนวิกฤติ → ไม่ WAIT
        self.assertEqual(rb.classify(self.feat(m1=-1, m3=-2), "MOPS"), "WAIT")


class Table(unittest.TestCase):
    def test_outcomes_and_report(self):
        days = pd.date_range("2026-10-01", periods=14)
        mops = pd.DataFrame({"day": days, "fuel": "B7", "mops": np.arange(14) * 1.0 + 80})          # ขึ้นทุกวัน
        wti = pd.DataFrame({"day": days, "wti": np.arange(14) * 0.5 + 60})
        eppo = pd.DataFrame({"day": days, "fuel": "B7", "ws": np.arange(14) * 0.2 + 37, "mm": 1.8, "retail": np.arange(14) * 0.2 + 42})
        fund = pd.DataFrame({"day": [days[0]], "crisis": [True], "status": ["CRITICAL"]})
        t = rb.build_table(mops, wti, eppo, fund)
        self.assertFalse(t.empty)
        r = t[t.day == days[5]].iloc[0]
        self.assertAlmostEqual(r.dws1, 0.2)
        self.assertAlmostEqual(r.dws3, 0.6)
        out = rb.class_report(t)
        self.assertTrue(any("BUY" in x for x in out))
        self.assertTrue(any("[RULE-LAG]" in x for x in rb.lag_report(t)))

    def test_block_ci_runs(self):
        d = pd.date_range("2026-10-01", periods=40)
        a = pd.DataFrame({"day": d[:20], "dws3": np.linspace(0.1, 0.5, 20)})
        b = pd.DataFrame({"day": d[20:], "dws3": np.linspace(-0.3, 0.1, 20)})
        lo, hi = rb.block_ci(a, b, reps=200)
        self.assertLess(lo, hi)


if __name__ == "__main__":
    unittest.main()
