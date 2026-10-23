import unittest

import numpy as np
import pandas as pd

import engine
import log_outlook
import model as M


class ClassifyTest(unittest.TestCase):
    def test_tiers(self):
        self.assertEqual(M.classify_outlook(0.6, 0.1, 0.35, 0.0), "UP_STRONG")
        self.assertEqual(M.classify_outlook(0.6, 0.1, 0.10, 0.0), "UP")
        self.assertEqual(M.classify_outlook(0.1, 0.55, 0.0, 0.2), "DOWN")
        self.assertEqual(M.classify_outlook(0.1, 0.55, 0.0, 0.4), "DOWN_STRONG")
        self.assertEqual(M.classify_outlook(0.45, 0.45, 0.2, 0.2), "UNCLEAR")        # ต่ำกว่า 50% ไม่ฝืนทาย
        self.assertEqual(M.classify_outlook(0.55, 0.60, 0.1, 0.1), "DOWN")            # ทั้งคู่ >= 50% เลือกที่สูงกว่า

    def test_grade(self):
        good = {"n": 60, "right10": 0.65, "wrong10": 0.10, "right_h1": 0.64, "right_h2": 0.66}
        self.assertEqual(M.outlook_grade(good), "สูง")
        self.assertEqual(M.outlook_grade({**good, "right_h1": 0.50, "right_h2": 0.70}), "กลาง")      # ไม่นิ่ง -> ลดหนึ่งระดับ
        self.assertEqual(M.outlook_grade({**good, "right_h1": None}), "กลาง")                          # พิสูจน์ความนิ่งไม่ได้ -> ไม่เกินกลาง
        self.assertEqual(M.outlook_grade({"n": 30, "right10": 0.55, "wrong10": 0.12, "right_h1": 0.5, "right_h2": 0.55}), "กลาง")
        self.assertEqual(M.outlook_grade({"n": 100, "right10": 0.62, "wrong10": 0.24, "right_h1": 0.6, "right_h2": 0.6}), "ต่ำ")   # ผิดทิศบ่อย
        self.assertEqual(M.outlook_grade({"n": 5, "right10": 0.9, "wrong10": 0.0}), "ต่ำ")             # ตัวอย่างน้อย
        self.assertEqual(M.outlook_grade({}), "ต่ำ")


class RefTest(unittest.TestCase):
    def test_ref(self):
        self.assertEqual(engine.outlook_ref(pd.Timestamp("2026-10-22 21:10")), pd.Timestamp("2026-10-22 20:30"))
        self.assertEqual(engine.outlook_ref(pd.Timestamp("2026-10-23 07:00")), pd.Timestamp("2026-10-22 20:30"))
        self.assertEqual(engine.outlook_ref(pd.Timestamp("2026-10-22 20:30")), pd.Timestamp("2026-10-22 20:30"))


class TextTest(unittest.TestCase):
    def _d(self, tier, grade="กลาง"):
        ref = pd.Timestamp("2026-10-22 20:30")
        h = {"hours": 24, "tier": tier, "label": M.OUTLOOK_TIERS[tier], "size": engine.OUTLOOK_SIZE.get(tier), "grade": grade, "until": ref + pd.Timedelta(hours=24),
             "evidence": {"n": 51, "share": 0.1, "right10": 0.82, "right40": 0.47, "wrong10": 0.10, "avg_move": 0.45, "grade": grade}}
        return {"fuel": "B7", "ref": ref, "asof": ref, "horizons": [h], "note": None}

    def test_text_no_confidence_for_unclear_and_no_flat_claim(self):
        t = engine.outlook_text({"DIESEL": self._d("UNCLEAR")})
        self.assertIn("ยังไม่ชัดเจน", t)
        self.assertNotIn("ความเชื่อมั่น", t)
        self.assertNotIn("ทรงตัว", t)
        self.assertIn("❓", t)

    def test_text_up(self):
        t = engine.outlook_text({"DIESEL": self._d("UP_STRONG")})
        self.assertIn("ขึ้นแรง", t)
        self.assertIn("ความเชื่อมั่น: กลาง", t)
        self.assertIn("ถูกทิศ 82%", t)
        self.assertIn("📈🔺", t)
        self.assertIn("🟡", t)
        self.assertLess(len(t), 1000)

    def test_each_tier_has_emoji(self):
        for tier in M.OUTLOOK_TIERS:
            self.assertIn(tier, engine.TIER_EMOJI)
            self.assertIn(engine.TIER_EMOJI[tier], engine.outlook_text({"DIESEL": self._d(tier)}))
        self.assertEqual(set(engine.GRADE_EMOJI), {"สูง", "กลาง", "ต่ำ"})

    def test_empty(self):
        self.assertEqual(engine.outlook_text({"DIESEL": {"fuel": "B7", "horizons": []}}), "")


class LiveStatsTest(unittest.TestCase):
    def test_first_row_per_day_and_direction(self):
        log = pd.DataFrame([
            {"date": "2026-10-22", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "0.30"},
            {"date": "2026-10-22", "fuel": "B7", "hours": 24, "tier": "DOWN", "outcome_delta": "-0.9"},    # ซ้ำวัน: ไม่นับ
            {"date": "2026-10-23", "fuel": "B7", "hours": 24, "tier": "DOWN", "outcome_delta": "-0.20"},
            {"date": "2026-10-24", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "-0.15"},
            {"date": "2026-10-25", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": ""},           # ยังไม่มีผล
            {"date": "2026-10-22", "fuel": "G95", "hours": 24, "tier": "UP", "outcome_delta": "0.5"},
        ])
        st = engine.outlook_live_stats(log, "B7", 24)
        self.assertEqual(st["n_days"], 3)
        self.assertEqual(st["tiers"]["UP"]["n"], 2)
        self.assertAlmostEqual(st["tiers"]["UP"]["right10"], 0.5)
        self.assertAlmostEqual(st["tiers"]["UP"]["wrong10"], 0.5)
        self.assertAlmostEqual(st["tiers"]["DOWN"]["right10"], 1.0)
        self.assertIsNone(engine.outlook_live_stats(pd.DataFrame(), "B7", 24))


class LogRowsTest(unittest.TestCase):
    def test_new_rows_dedupes_by_ref(self):
        snap = [{"asof": "2026-10-22 21:10", "ref": "2026-10-22 20:30", "fuel": "B7", "hours": 24, "tier": "UP", "grade": "กลาง", "model_version": "v"}]
        add = log_outlook.new_rows([], snap)
        self.assertEqual(len(add), 1)
        self.assertEqual(add[0]["date"], "2026-10-22")
        self.assertEqual(add[0]["outcome_delta"], "")
        again = log_outlook.new_rows(add, [{**snap[0], "asof": "2026-10-22 22:40"}])          # รอบถัดไปของคืนเดียวกัน: ไม่บันทึกซ้ำ
        self.assertEqual(again, [])

    def test_late_rows_not_logged(self):
        late = [{"asof": "2026-10-23 13:07", "ref": "2026-10-22 20:30", "fuel": "B7", "hours": 24, "tier": "UP", "grade": "กลาง", "model_version": "v"}]
        self.assertEqual(log_outlook.new_rows([], late), [])        # ออกบ่ายวันถัดไป = ไม่ใช่เดินหน้าจริง

    def test_late_rows_excluded_from_stats(self):
        log = pd.DataFrame([
            {"asof": "2026-10-22 21:00", "ref": "2026-10-22 20:30", "date": "2026-10-22", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "0.30"},
            {"asof": "2026-10-24 13:00", "ref": "2026-10-23 20:30", "date": "2026-10-23", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "0.50"},
        ])
        self.assertEqual(engine.outlook_live_stats(log, "B7", 24)["n_days"], 1)


if __name__ == "__main__":
    unittest.main()
