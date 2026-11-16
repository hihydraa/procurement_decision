"""รอบเที่ยงของดีเซลแบบเงา : ทายเฉพาะหลัง 12:00 ด้วยข้อมูลถึง 12:00 บันทึกวันละแถว และเติมผลเมื่อเลย 06:00 วันถัดไป"""
import json
import os
import sys
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import log_noon as N  # noqa: E402
import model as M  # noqa: E402
import signals as S  # noqa: E402


def _prices(day="2026-11-12", late_drop=False):
    rows = []
    for d in pd.date_range(pd.Timestamp(day) - pd.Timedelta(days=6), pd.Timestamp(day)):
        for prov, base in (("A", 30.0), ("B", 30.2)):
            rows.append({"provider": prov, "warehouse": "W", "fuel": "B7", "price": base, "effective": f"{d:%Y-%m-%d} 05:00",
                         "msg_date": f"{d:%Y-%m-%d}", "msg_time": "05:00", "posted": f"{d:%Y-%m-%d} 05:00"})
    if late_drop:                                                    # โพสต์หลัง 12:00 : ต้องไม่กระทบคำทายรอบ 12:00
        rows.append({"provider": "A", "warehouse": "W", "fuel": "B7", "price": 20.0, "effective": f"{day} 14:00", "msg_date": day, "msg_time": "13:50", "posted": f"{day} 13:50"})
    return pd.DataFrame(rows)


def _tables(**kw):
    return S.Tables().set_prices(_prices(**kw))


class ShippedModelTest(unittest.TestCase):
    def test_model_file_matches_experiment(self):
        m = N.load_model()
        self.assertEqual((m["fuel"], m["ref_hour"], m["target_hour"], m["hz"], m["feature_set"]), ("B7", 12.0, 6.0, 18.0, "v2"))
        for k in ("r10", "f10"):
            self.assertEqual(len(m["models"][k]["w"]), len(M.vectorize_v2({})))
        wf = m["walk_forward"]
        self.assertTrue(wf["ok"])                                    # ผ่านเกณฑ์ของ exp_two_rounds ณ วันที่ฝึก
        self.assertGreaterEqual(wf["right_dn"], 0.60)
        self.assertLess(wf["right_up"], 0.60)                        # ฝั่งขึ้นไม่ผ่าน : เหตุผลที่ตั้งเกณฑ์ให้ใช้เฉพาะฝั่งลง


class DecideTest(unittest.TestCase):
    def test_only_after_noon_and_with_fresh_prices(self):
        m = N.load_model()
        self.assertIsNone(N.decide(_tables(), pd.Timestamp("2026-11-12 11:59"), m))
        d = N.decide(_tables(), pd.Timestamp("2026-11-12 13:05"), m)
        self.assertEqual((d["ref"], d["target"], d["fuel"]), ("2026-11-12 12:00", "2026-11-13 06:00", "B7"))
        self.assertIn(d["call"], ("UP", "DOWN", ""))
        self.assertTrue(0 <= d["p_up"] <= 1 and 0 <= d["p_down"] <= 1)
        self.assertIsNone(N.decide(_tables(), pd.Timestamp("2026-11-12 13:05"), {}))                 # ไม่มีโมเดล
        self.assertIsNone(N.decide(_tables(), pd.Timestamp("2026-11-20 13:05"), m))                  # ฟีดราคาค้าง 8 วัน
        self.assertIsNone(N.decide(S.Tables(), pd.Timestamp("2026-11-12 13:05"), m))                 # ไม่มีฟีดราคา

    def test_uses_only_information_posted_by_noon(self):
        m = N.load_model()
        a = N.decide(_tables(), pd.Timestamp("2026-11-12 13:05"), m)
        b = N.decide(_tables(late_drop=True), pd.Timestamp("2026-11-12 19:00"), m)                   # รันช้า + มีราคาโพสต์ 13:50
        self.assertEqual((a["p_up"], a["p_down"], a["call"]), (b["p_up"], b["p_down"], b["call"]))

    def test_snapshot_file(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "s.json")
            N.write_snapshot(_tables(), pd.Timestamp("2026-11-12 09:00"), p)
            self.assertEqual(json.load(open(p, encoding="utf-8")), [])
            N.write_snapshot(_tables(), pd.Timestamp("2026-11-12 13:05"), p)
            self.assertEqual(len(json.load(open(p, encoding="utf-8"))), 1)


class LogTest(unittest.TestCase):
    SNAP = {"asof": "2026-11-12 13:05", "ref": "2026-11-12 12:00", "fuel": "B7", "target": "2026-11-13 06:00", "call": "DOWN", "p_up": 0.1, "p_down": 0.6,
            "model_version": "2026-10-18"}

    def test_one_row_per_day_with_lag(self):
        rows = N.new_rows([], [self.SNAP])
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertEqual(set(r), set(N.COLUMNS))
        self.assertEqual((r["date"], r["lag_h"], r["late"], r["outcome_delta"]), ("2026-11-12", 1.08, 0, ""))
        self.assertEqual(N.new_rows(rows, [dict(self.SNAP, asof="2026-11-12 15:45")]), [])           # รอบถัดไปของวันเดียวกันไม่บันทึกซ้ำ
        self.assertEqual(N.new_rows([], [dict(self.SNAP, asof="2026-11-12 17:00")])[0]["late"], 1)   # ช้าเกิน 3 ชม. = late
        self.assertEqual(N.new_rows([], [dict(self.SNAP, asof="2026-11-13 07:00")]), [])             # ช้าเกิน 12 ชม. ไม่บันทึก
        self.assertEqual(len(N.COLUMNS), ord(N.LAST_COL) - ord("A") + 1)

    def test_outcome_and_summary(self):
        p = _prices()
        p = pd.concat([p, pd.DataFrame([{"provider": "A", "warehouse": "W", "fuel": "B7", "price": 29.5, "effective": "2026-11-13 00:30"}])], ignore_index=True)
        p["effective"] = pd.to_datetime(p["effective"])
        book = S.PriceBook(p)
        self.assertIsNone(N.compute_outcome(book, "2026-11-12 12:00", "2026-11-13 06:00", pd.Timestamp("2026-11-13 06:10")))    # ยังไม่เลย 30 นาที
        self.assertEqual(N.compute_outcome(book, "2026-11-12 12:00", "2026-11-13 06:00", pd.Timestamp("2026-11-13 07:00")), (30.0, 29.5, -0.5))
        rows = [{"call": "DOWN", "outcome_delta": -0.5, "late": 0}, {"call": "DOWN", "outcome_delta": 0.3, "late": 0}, {"call": "UP", "outcome_delta": 0.02, "late": 0},
                {"call": "DOWN", "outcome_delta": -0.9, "late": 1}, {"call": "", "outcome_delta": 0.0, "late": 0}, {"call": "DOWN", "outcome_delta": "", "late": 0}]
        text = "\n".join(N.summary(rows))
        self.assertIn("ผลจริงตรงเวลา 4/40 วัน", text)                                                # แถว late และแถวที่ยังไม่มีผลไม่นับ
        self.assertIn("ทายลง 2 ครั้ง · ถูก 1 · ผิดทิศ 1", text)
        self.assertIn("ทายขึ้น 1 ครั้ง · ถูก 0 · ผิดทิศ 0", text)


if __name__ == "__main__":
    unittest.main()
