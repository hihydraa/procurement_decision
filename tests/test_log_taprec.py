import unittest

import pandas as pd

import log_taprec as lt


def outlook(rows):
    """rows = (ref, fuel, hours, tier, model_version[, lag_h]) ; lag_h ไม่ระบุ = 1.0"""
    rows = [r if len(r) == 6 else (*r, 1.0) for r in rows]
    return pd.DataFrame(rows, columns=["ref", "fuel", "hours", "tier", "model_version", "lag_h"]).assign(ref=lambda d: pd.to_datetime(d.ref))


def prices(day, shape):
    """ราคาสังเคราะห์ ทุกคลังในรายการ DEPOTS ใช้ชุดเดียวกัน : shape = [(ชั่วโมงจาก 00:00 วัน D, ราคา)]"""
    rows = []
    for depot, (pv, wh) in lt.DEPOTS.items():
        for h, p in shape:
            rows.append({"jobber": "x", "provider": pv, "warehouse": wh, "fuel": "ดีเซล", "price": p, "effective": pd.Timestamp(day) + pd.Timedelta(hours=h)})
    return pd.DataFrame(rows)


class NewRows(unittest.TestCase):
    def test_filters_dedupes_and_maps(self):
        o = outlook([("2026-11-09 20:30", "ดีเซล", 24, "UP_STRONG", "v"),      # ก่อน SHADOW_START
                     ("2026-11-10 20:30", "ดีเซล", 24, "UP_STRONG", "v1"),
                     ("2026-11-10 20:30", "ดีเซล", 48, "UP_STRONG", "v1"),      # ขอบฟ้า 48 ไม่นับ
                     ("2026-11-11 20:30", "ดีเซล", 24, "DOWN_WEAK", "v1"),
                     ("2026-11-12 20:30", "ดีเซล", 24, "FLAT", "v1")])
        ex = set()
        rows = lt.new_rows(ex, o, pd.Timestamp("2026-11-12 21:00"))
        self.assertEqual([r["day"] for r in rows], ["2026-11-11", "2026-11-12", "2026-11-13"])
        self.assertEqual([r["pred"] for r in rows], ["ขึ้น", "ลง", "ไม่ชัด"])
        self.assertEqual((rows[0]["rec_A"], rows[0]["rec_B"], rows[0]["rec_C"]), ("ก่อน22:30", "07:00", "ก่อน22:30"))
        self.assertEqual((rows[1]["rec_A"], rows[1]["rec_B"], rows[1]["rec_C"]), ("16:30",) * 3)
        self.assertEqual(rows[2]["rec_C"], "16:30")
        self.assertEqual(lt.new_rows(ex, o, pd.Timestamp("2026-11-12 21:30")), [])      # ไม่ซ้ำ
        self.assertTrue(all(len(r) == len(lt.COLUMNS) for r in rows))


class LateRows(unittest.TestCase):
    def test_late_flag(self):
        o = outlook([("2026-11-10 20:30", "ดีเซล", 24, "DOWN_STRONG", "v1", 3.15), ("2026-11-11 20:30", "ดีเซล", 24, "UP", "v1", 2.5)])
        rows = lt.new_rows(set(), o, pd.Timestamp("2026-11-11 22:00"))
        self.assertEqual([(r["lag_h"], r["late"]) for r in rows], [(3.15, 1), (2.5, 0)])
        self.assertEqual(rows[0]["rec_A"], "16:30")                      # ยังสร้างแถวและแมปนโยบายตามปกติ

    def test_load_outlook_window(self):
        from tap_policy import load_outlook
        head = ["asof", "date", "ref", "fuel", "hours", "tier", "model_version", "lag_h", "backfilled"]
        v = [head,
             ["2026-11-10 23:39", "2026-11-10", "2026-11-10 20:30", "B7", 24, "UNCLEAR", "v", 3.15, 0],        # ช้า 3.15 ชม.
             ["2026-11-10 22:00", "2026-11-10", "2026-11-10 20:30", "G95", 24, "DOWN", "v", 1.5, 0],            # ตรงเวลา
             ["2026-11-11 09:00", "2026-11-10", "2026-11-10 20:30", "G95", 48, "DOWN", "v", 12.5, 0],           # ช้าเกินเพดาน
             ["2026-11-10 21:00", "2026-11-10", "2026-11-10 20:30", "B7", 48, "UP", "v", 0.5, 1]]               # backfill ตัดทิ้ง
        strict, wide = load_outlook(v), load_outlook(v, 12.0)
        self.assertEqual(sorted(zip(strict.fuel, strict.hours)), [("G95", 24)])
        self.assertEqual(sorted(zip(wide.fuel, wide.hours)), [("B7", 24), ("G95", 24)])
        self.assertAlmostEqual(float(wide[wide.fuel == "B7"].lag_h.iloc[0]), 3.15)


class Fill(unittest.TestCase):
    ROW = {"day": "2026-11-11", "fuel": "ดีเซล", "rec_A": "ก่อน22:30", "rec_B": "07:00", "rec_C": "16:30"}
    # ราคา : 20:30 วันก่อน 30.0 ; ต่ำสุด 29.5 เวลา 04:00 ; 07:00 → 30.2 ; 16:30 → 30.5
    SHAPE = [(-3.5, 30.0), (4.0, 29.5), (6.5, 30.2), (16.0, 30.5), (21.0, 30.5)]

    def test_regrets(self):
        p = prices("2026-11-11", self.SHAPE)
        res = lt.fill_outcome(self.ROW, p, pd.Timestamp("2026-11-11 23:00"))
        self.assertEqual(res["n_depots"], 6)
        self.assertAlmostEqual(res["regret_A"], 0.5)          # ก่อน22:30 ราคา 30.0
        self.assertAlmostEqual(res["regret_B"], 0.7)
        self.assertAlmostEqual(res["regret_C"], 1.0)
        self.assertAlmostEqual(res["fixed_2230"], 0.5)
        self.assertEqual(res["actual_cls"], "ขึ้น")           # 20:30 วัน D = 30.5 เทียบ 30.0
        self.assertAlmostEqual(res["delta"], 0.5)

    def test_not_due_or_insufficient(self):
        p = prices("2026-11-11", self.SHAPE)
        self.assertIsNone(lt.fill_outcome(self.ROW, p, pd.Timestamp("2026-11-11 22:00")))
        self.assertIsNone(lt.fill_outcome(self.ROW, p[p.effective < pd.Timestamp("2026-11-11 20:00")], pd.Timestamp("2026-11-12 01:00")))    # ราคายังไม่ถึงเย็นวัน D


class SummarySplit(unittest.TestCase):
    def test_on_time_main_late_separate(self):
        base = {"filled_at": "x", "regret_B": 0.2, "regret_C": 0.3, "fixed_2230": 0.4, "fixed_0700": 0.5, "fixed_1630": 0.6}
        rows = [{**base, "day": f"2026-11-{11 + i}", "regret_A": 0.1, "late": 0} for i in range(3)] + \
               [{**base, "day": "2026-11-20", "regret_A": 0.9, "late": 1}]
        out = lt.shadow_summary(rows)
        main = next(x for x in out if "ตรงเวลา" in x)
        late = next(x for x in out if "ล็อกช้า" in x)
        self.assertIn("เติมผลแล้ว 3 แถว", main)          # แถวช้าไม่ปนผลหลัก
        self.assertIn("A 0.100", main)
        self.assertIn("เติมผลแล้ว 1 แถว", late)
        self.assertIn("A 0.900", late)

    def test_only_late_rows(self):
        rows = [{"day": "2026-11-11", "filled_at": "x", "regret_A": 0.1, "regret_B": 0.2, "regret_C": 0.3, "fixed_2230": 0.4, "fixed_0700": 0.5, "fixed_1630": 0.6, "late": 1}]
        out = lt.shadow_summary(rows)
        self.assertIn("ตรงเวลา: ยังไม่มีแถวที่เติมผลแล้ว", out[0])


class Summary(unittest.TestCase):
    def test_empty_and_small(self):
        self.assertIn("ยังไม่มี", lt.shadow_summary([{"filled_at": ""}])[0])
        rows = [{"day": f"2026-11-{11 + i}", "filled_at": "x", "regret_A": 0.1, "regret_B": 0.2, "regret_C": 0.3,
                 "fixed_2230": 0.4, "fixed_0700": 0.5, "fixed_1630": 0.6} for i in range(3)]
        out = lt.shadow_summary(rows)
        self.assertIn("ยังไม่พอ", out[1])

    def test_ci_with_enough_days(self):
        rows = [{"day": f"2026-11-{11 + i}", "filled_at": "x", "regret_A": 0.1, "regret_B": 0.2, "regret_C": 0.3,
                 "fixed_2230": 0.4, "fixed_0700": 0.5, "fixed_1630": 0.6} for i in range(9)]
        self.assertIn("95%", lt.shadow_summary(rows)[1])


if __name__ == "__main__":
    unittest.main()
