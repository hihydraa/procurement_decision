import unittest

import numpy as np
import pandas as pd

import exp_model_search as X


def prices(rows):
    return pd.DataFrame(rows, columns=["msg_date", "msg_time", "effective", "provider", "warehouse", "fuel", "price"])


class KnownBookTest(unittest.TestCase):
    def setUp(self):
        self.p = prices([
            ("2026-01-01", "08:00", "2026-01-01 05:00", "A", "x", "B7", 30.00),
            ("2026-01-01", "08:00", "2026-01-01 05:00", "B", "y", "B7", 30.20),
            ("2026-01-02", "17:30", "2026-01-02 23:59", "A", "x", "B7", 30.50),      # ประกาศล่วงหน้า : โพสต์ 17:30 มีผล 23:59
            ("2026-01-02", "21:00", "2026-01-02 23:59", "B", "y", "B7", 30.70),      # โพสต์หลัง 20:30 → ยังไม่รู้ตอนตัดสินใจ
            ("2026-01-03", "08:00", "2026-01-02 12:00", "A", "x", "B7", 29.00),      # โพสต์ย้อนหลัง (มีผลก่อนเวลาโพสต์) → ห้ามเห็นก่อน 03/01 08:00
        ])
        self.kb = X.KnownBook(self.p, "B7")
        self.asof = pd.Timestamp("2026-01-02 20:30")

    def test_snapshot_uses_post_time_not_effective_time(self):
        snap = self.kb.snapshot(self.asof)
        self.assertEqual(len(snap), 3)
        self.assertNotIn(29.00, snap.price.tolist())          # แถวโพสต์ย้อนหลังยังไม่ถูกโพสต์
        self.assertNotIn(30.70, snap.price.tolist())          # โพสต์ 21:00

    def test_pending_announced_change(self):
        f = X.extra_features(self.kb, self.asof, 24)
        self.assertAlmostEqual(f["pend"], 0.20)               # ตลาดต่ำสุดพรุ่งนี้ที่รู้แล้ว = 30.20 (B ยังไม่ประกาศ) เทียบ 30.00
        self.assertEqual((f["pend_up_n"], f["pend_dn_n"]), (1, 0))
        self.assertAlmostEqual(f["disp"], 0.10)

    def test_no_pending_when_nothing_announced(self):
        f = X.extra_features(self.kb, pd.Timestamp("2026-01-02 15:30"), 24)
        self.assertAlmostEqual(f["pend"], 0.0)

    def test_stale_series_dropped(self):
        f = X.extra_features(self.kb, pd.Timestamp("2026-01-09 20:30"), 24)
        self.assertTrue(np.isnan(f["pend"]) or f["n_series"] <= 2)


class Helpers(unittest.TestCase):
    def test_boot_diff_and_policy(self):
        rng = np.random.default_rng(0)
        y = (rng.random(300) > 0.6).astype(float)
        good, bad = y * 0.6 + rng.random(300) * 0.4, rng.random(300)
        lo, hi = X.boot_diff(y, bad, good, n=100)
        self.assertGreater(lo, 0)
        yv = np.where(y > 0, 0.5, -0.3)
        th, g, share, right = X.policy_gain(yv, good, 1 - good, 150)
        self.assertGreater(g, 0)


if __name__ == "__main__":
    unittest.main()
