import unittest

import numpy as np
import pandas as pd

import diag_retail_forecast as D


def eppo(rows):
    return pd.DataFrame(rows, columns=["day", "fuel", "ws", "mm", "retail", "exref", "fund"]).assign(day=lambda x: pd.to_datetime(x.day))


class Table(unittest.TestCase):
    def test_deltas_use_calendar_days_and_skip_gaps(self):
        e = eppo([("2026-11-01", "B7", 36.0, 2.0, 40.0, 0, 0), ("2026-11-02", "B7", 35.5, 2.4, 40.0, 0, 0),
                  ("2026-11-03", "B7", 35.5, 2.0, 39.5, 0, 0), ("2026-11-05", "B7", 35.0, 2.0, 39.0, 0, 0)])
        t = D.build_table(e).set_index("day")
        r = t.loc["2026-11-01"]
        self.assertAlmostEqual(r.d1, 0.0)
        self.assertAlmostEqual(r.d2, -0.5)
        self.assertTrue(np.isnan(r.d3))                         # 4 พ.ย. ไม่มีข้อมูล
        self.assertAlmostEqual(t.loc["2026-11-02"].dws, -0.5)
        self.assertAlmostEqual(t.loc["2026-11-01"].score, 2.0 - D.MEDIAN_MM["B7"])
        self.assertTrue(np.isnan(t.loc["2026-11-05"].dws))      # ไม่มีวันก่อนหน้า


class Stats(unittest.TestCase):
    def test_auc(self):
        self.assertEqual(D.auc([1, 2, 3, 4], [False, False, True, True]), 1.0)
        self.assertEqual(D.auc([4, 3, 2, 1], [False, False, True, True]), 0.0)
        self.assertEqual(D.auc([1, 1, 1, 1], [False, False, True, True]), 0.5)
        self.assertTrue(np.isnan(D.auc([1, 2], [True, True])))

    def test_report_runs_on_synthetic_mechanism(self):
        rng = np.random.default_rng(0)
        days = pd.date_range("2026-01-01", periods=120)
        mm = 1.94 + rng.normal(0, 0.4, 120)
        retail = 40 - np.r_[mm[3:] - 1.94, [0, 0, 0]] * 0.8           # ค่าการตลาดสูง → ปลีกลงอีก 3 วันถัดไป
        e = eppo([(d, "B7", 36.0, m, r, 0, 0) for d, m, r in zip(days, mm, retail)])
        t = D.build_table(e)
        lines = D.skill_lines(t) + D.move_lines(t) + D.passthrough_lines(t)
        text = "\\n".join(lines)
        self.assertIn("ปลีก +3 วัน", text)
        self.assertIn("ดีเซล", text)


if __name__ == "__main__":
    unittest.main()
