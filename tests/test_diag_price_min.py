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
        c = D.quote_table(self.p, "G95", t, key_ok=D.is_core)
        self.assertEqual(c.price.iloc[0], 38.25)                                         # ไออาร์ 38.83 vs ดาว 38.25
        self.assertTrue(D.is_core(c.holder.iloc[0]))

    def test_quote_expires_after_three_days(self):
        t = pd.DatetimeIndex(["2026-11-09 21:00"])                                       # เกิน 72 ชม. จาก 11-06 20:00
        self.assertEqual(D.quote_table(self.p, "G95", t).price.iloc[0], 38.25)

    def test_no_data_gives_nan(self):
        out = D.quote_table(self.p, "G95", pd.DatetimeIndex(["2026-10-01 08:00"]))
        self.assertTrue(np.isnan(out.price.iloc[0]))
        self.assertTrue(np.isnan(D.quote_table(self.p, "B7", pd.DatetimeIndex(["2026-11-09 08:00"]), key_ok=D.is_core).price.iloc[0]))


class CoreDepotsTest(unittest.TestCase):
    def test_membership(self):
        for k in ("ไออาร์|ระยอง", "ไออาร์|อยุธยา", "สตาร์ฟูเอลส์ (ดาว)|สระบุรี", "สตาร์ฟูเอลส์ (ดาว)|ระยอง",
                  "บางจาก|บางปะอิน", "บางจาก|บางปะอิน (คลังใน)", "ปตท|สระบุรี-ใหญ่/นอก", "บางจาก (BSRC)|สระบุรี", "แท็ปไลน์ x|ที่ไหนก็ได้"):
            self.assertTrue(D.is_core(k), k)
        for k in ("ไออาร์|ชุมพร", "ไออาร์|บางปะอิน", "สตาร์ฟูเอลส์ (ดาว)|ขอนแก่น", "ปตท|บางปะอิน", "บางจาก|ศรีราชา"):
            self.assertFalse(D.is_core(k), k)

    def test_matches_report_lists_missing_entries(self):
        p = prices([["ไออาร์", "ระยอง", "G95", 37.0, "2026-11-07 08:00"], ["บางจาก", "บางปะอิน", "G95", 37.5, "2026-11-07 08:00"]])
        m = D.core_matches(p)
        self.assertEqual([k for k, _, _ in m["ไออาร์/ระยอง"]], ["ไออาร์|ระยอง"])
        self.assertEqual(m["แท็ปไลน์/*"], [])                                            # ไม่มีคำนี้ในฟีด → รายงานว่าไม่พบ
        self.assertEqual(m["*/สระบุรี"], [])


class FreshAndLoaderTest(unittest.TestCase):
    def test_max_age_ignores_orphan_quote(self):
        p = prices([
            ["บางจาก", "บางปะอิน (คลังนอก)", "G95", 36.66, "2026-11-07 09:30"],          # ชื่อคลังเก่าที่ไม่มีใครโพสต์ซ้ำ
            ["บางจาก", "บางปะอิน", "G95", 37.84, "2026-11-08 08:19"],
            ["ไออาร์", "ระยอง", "G95", 37.44, "2026-11-08 08:25"],
        ])
        t = pd.DatetimeIndex(["2026-11-08 20:30"])
        self.assertEqual(D.quote_table(p, "G95", t).price.iloc[0], 36.66)
        fresh = D.quote_table(p, "G95", t, max_age_h=24)
        self.assertEqual(fresh.price.iloc[0], 37.44)
        self.assertTrue(D.is_core("บางจาก|บางปะอิน (คลังนอก)"))                           # ชุด core ของเจ้าของระบบรวมไว้ จึงต้องมีทางเลือก fresh24
        self.assertFalse(D.is_core07("บางจาก|บางปะอิน (คลังนอก)"))

    def test_loader_handles_serial_dates(self):
        from common import EXCEL_EPOCH
        def serial(s):
            return (pd.Timestamp(s) - pd.Timestamp(EXCEL_EPOCH)).total_seconds() / 86400
        head = ["asof", "date", "ref", "fuel", "hours", "tier", "outcome_delta", "backfilled"]
        values = [head,
                  [serial("2026-11-05 22:40"), serial("2026-11-05"), serial("2026-11-05 20:30"), "G95", 24, "UP", 0.3, ""],
                  [serial("2026-11-05 23:50"), serial("2026-11-05"), serial("2026-11-05 20:30"), "G95", 24, "UP", 0.1, ""],   # รอบหลัง ไม่นับ
                  [serial("2026-11-06 00:50"), serial("2026-11-05"), serial("2026-11-05 20:30"), "B7", 24, "UP", 0.1, ""],     # ช้า 4.3 ชม. ไม่นับ
                  [serial("2026-11-05 17:00"), serial("2026-11-05"), serial("2026-11-05 15:30"), "B7", 24, "UP", 0.1, ""],     # รอบ 15:30 ไม่นับ
                  [serial("2026-11-06 21:00"), serial("2026-11-06"), serial("2026-11-06 20:30"), "B7", 48, "DOWN", -0.2, 1]]    # backfilled ไม่นับ
        d = D.load_outlook(values)
        self.assertEqual(len(d), 1)
        self.assertEqual(d.iloc[0]["fuel"], "G95")
        self.assertEqual(d.iloc[0]["date"], "2026-11-05")


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
