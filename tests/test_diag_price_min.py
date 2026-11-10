import unittest

import numpy as np
import pandas as pd

import diag_price_min as D


def prices(rows):
    return D.clean_prices(pd.DataFrame(rows, columns=["provider", "warehouse", "fuel", "price", "effective"]))


class QuoteTableTest(unittest.TestCase):
    def setUp(self):
        self.p = prices([
            ["ผู้ขายอื่น", "ที่หนึ่ง", "G95", 36.66, "2026-11-06 20:00"],               # ราคาเก่าค้างต่ำสุด
            ["ไออาร์", "ระยอง", "G95", 37.44, "2026-11-08 08:25"],
            ["ไออาร์", "ระยอง", "G95", 38.83, "2026-11-09 08:08"],
            ["สตาร์ฟูเอลส์ (ดาว)", "สระบุรี", "G95", 38.25, "2026-11-09 08:07"],
            ["ไออาร์", "ระยอง", "B7", 39.5, "2026-11-09 14:00"],
        ])

    def test_stale_low_quote_holds_the_all_min(self):
        t = pd.DatetimeIndex(["2026-11-09 08:30"])
        a = D.quote_table(self.p, "G95", t)
        self.assertEqual(a.price.iloc[0], 36.66)
        self.assertEqual(a.holder.iloc[0], "ผู้ขายอื่น|ที่หนึ่ง")
        self.assertAlmostEqual(a.age_h.iloc[0], 60.5, places=1)
        c = D.quote_table(self.p, "G95", t, only_core=True)
        self.assertEqual(c.price.iloc[0], 38.25)                                         # ไออาร์ 38.83 vs ดาว 38.25
        self.assertTrue(D.is_core(c.holder.iloc[0]))

    def test_quote_expires_after_three_days(self):
        t = pd.DatetimeIndex(["2026-11-09 21:00"])                                       # เกิน 72 ชม. จาก 11-06 20:00
        self.assertEqual(D.quote_table(self.p, "G95", t).price.iloc[0], 38.25)

    def test_no_data_gives_nan(self):
        out = D.quote_table(self.p, "G95", pd.DatetimeIndex(["2026-10-01 08:00"]))
        self.assertTrue(np.isnan(out.price.iloc[0]))
        self.assertTrue(np.isnan(D.quote_table(self.p, "B7", pd.DatetimeIndex(["2026-11-09 08:00"]), only_core=True).price.iloc[0]))


class RescoreTest(unittest.TestCase):
    def test_flat_all_vs_moving_core(self):
        p = prices([
            ["ผู้ขายอื่น", "ที่หนึ่ง", "G95", 36.66, "2026-11-07 08:00"],
            ["ไออาร์", "ระยอง", "G95", 37.00, "2026-11-07 08:00"],
            ["ไออาร์", "ระยอง", "G95", 37.60, "2026-11-08 08:00"],
        ])
        rows = pd.DataFrame([{"date": "2026-11-07", "ref": pd.Timestamp("2026-11-07 20:30"), "fuel": "G95", "hours": 24, "tier": "UP", "outcome_delta": "0"}])
        res = D.rescore(rows, p, pd.Timestamp("2026-11-10 10:00"))
        by = res.set_index("basis").delta
        self.assertAlmostEqual(by["all"], 0.0)                                           # ราคาต่ำสุดไม่ขยับ
        self.assertAlmostEqual(by["core"], 0.60)                                         # ราคาคลังหลักขยับ
        self.assertIn("ถูก= 1", D.summarize(res[res.basis == "core"]))
        self.assertIn("ใกล้เดิม= 1", D.summarize(res[res.basis == "all"]))

    def test_not_ready_rows_are_skipped(self):
        p = prices([["ไออาร์", "ระยอง", "G95", 37.0, "2026-11-07 08:00"]])
        rows = pd.DataFrame([{"date": "2026-11-09", "ref": pd.Timestamp("2026-11-09 20:30"), "fuel": "G95", "hours": 24, "tier": "UP", "outcome_delta": ""}])
        res = D.rescore(rows, p, pd.Timestamp("2026-11-10 10:00"))
        self.assertTrue(res.delta.isna().all())


if __name__ == "__main__":
    unittest.main()
