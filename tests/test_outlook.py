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
        self.assertEqual(engine.outlook_source(pd.Timestamp("2026-10-22 21:10")), (pd.Timestamp("2026-10-22 20:30"), "E"))
        self.assertEqual(engine.outlook_source(pd.Timestamp("2026-10-22 20:30")), (pd.Timestamp("2026-10-22 20:30"), "E"))
        self.assertEqual(engine.outlook_source(pd.Timestamp("2026-10-23 07:00")), (pd.Timestamp("2026-10-22 20:30"), "E"))   # เช้า: ของเมื่อคืน
        self.assertEqual(engine.outlook_source(pd.Timestamp("2026-10-23 15:29")), (pd.Timestamp("2026-10-22 20:30"), "E"))
        self.assertEqual(engine.outlook_source(pd.Timestamp("2026-10-23 15:59")), (pd.Timestamp("2026-10-23 15:30"), "P"))   # บ่าย: ใช้ข้อมูลวันนี้
        self.assertEqual(engine.outlook_source(pd.Timestamp("2026-10-23 20:29")), (pd.Timestamp("2026-10-23 15:30"), "P"))
        self.assertEqual(engine.outlook_ref(pd.Timestamp("2026-10-22 21:10")), pd.Timestamp("2026-10-22 20:30"))


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
        self.assertIn("📈", t)
        for noisy in ("📊", "🕗", "⛽", "⏳", "🔎", "🧪", "🔺", "🔻"):
            self.assertNotIn(noisy, t)
        self.assertIn("🟡", t)
        self.assertLess(len(t), 1000)

    def test_each_tier_has_emoji(self):
        for tier in M.OUTLOOK_TIERS:
            self.assertIn(tier, engine.TIER_EMOJI)
            self.assertIn(engine.TIER_EMOJI[tier], engine.outlook_text({"DIESEL": self._d(tier)}))
        self.assertEqual(set(engine.GRADE_EMOJI), {"สูง", "กลาง", "ต่ำ"})

    def test_stale_label_in_text(self):
        d = self._d("UP")
        d["stale"] = True
        self.assertIn("ข้อมูลเมื่อคืน", engine.outlook_text({"DIESEL": d}))
        d["stale"] = False
        self.assertNotIn("ข้อมูลเมื่อคืน", engine.outlook_text({"DIESEL": d}))

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

    def test_live_stats_split_by_reference_time(self):
        log = pd.DataFrame([
            {"asof": "2026-10-22 21:00", "ref": "2026-10-22 20:30", "date": "2026-10-22", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "0.30"},
            {"asof": "2026-10-22 16:00", "ref": "2026-10-22 15:30", "date": "2026-10-22", "fuel": "B7", "hours": 24, "tier": "DOWN", "outcome_delta": "0.40"},
        ])
        self.assertEqual(list(engine.outlook_live_stats(log, "B7", 24, "20:30")["tiers"]), ["UP"])
        self.assertEqual(list(engine.outlook_live_stats(log, "B7", 24, "15:30")["tiers"]), ["DOWN"])


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


def _log(rows):
    """rows = [(date, tier, outcome)] รอบ 20:30 ของ B7 24 ชม. ออกหลังรอบ 1 ชม."""
    return pd.DataFrame([{"asof": f"{d} 21:30", "ref": f"{d} 20:30", "date": d, "fuel": "B7", "hours": 24, "tier": t, "outcome_delta": str(o)} for d, t, o in rows])


def _days(n):
    return [str(x.date()) for x in pd.date_range("2026-01-01", periods=n)]


class OutlookAlertTest(unittest.TestCase):
    SUMMARY = {"outlook_results": {"B7_H24": {"tiers": {"UP": {"n": 100, "share": 0.2, "avg_move": 0.25}, "DOWN": {"n": 100, "share": 0.2, "avg_move": 0.25},
                                                    "UP_STRONG": {"n": 0}, "UNCLEAR": {"n": 300, "share": 0.6}}}}}   # backtest กำไร/วัน = 0.2*0.25*2 = 0.10

    def test_no_data_and_too_few_days(self):
        self.assertIn("ยังไม่มีบันทึก", engine.outlook_alert_status(pd.DataFrame(), self.SUMMARY, "B7")["reasons"][0])
        few = _log([(d, "UP", 0.3) for d in _days(10)])
        s = engine.outlook_alert_status(few, self.SUMMARY, "B7")
        self.assertEqual(s["n"], 10)
        self.assertFalse(any(s["eligible"].values()))
        self.assertIn("10/40", s["reasons"][0])

    def test_eligible_when_hit_rate_and_gain_ok(self):
        days = _days(60)
        rows = []
        for i, d in enumerate(days):
            if i % 5 == 0:
                rows.append((d, "UP", 0.30 if i % 10 else -0.05))        # ทายขึ้น 12 ครั้ง ถูก 6
            elif i % 5 == 1:
                rows.append((d, "DOWN", -0.30))                          # ทายลง 12 ครั้ง ถูกหมด
            else:
                rows.append((d, "UNCLEAR", 0.0))
        s = engine.outlook_alert_status(_log(rows), self.SUMMARY, "B7")
        self.assertEqual(s["n"], 60)
        self.assertTrue(s["eligible"]["DOWN"], s)
        self.assertTrue(s["eligible"]["UP"], s)

    def test_weak_lift_blocks_that_side(self):
        days = _days(60)
        rows = []
        for i, d in enumerate(days):
            if i % 5 == 0:
                rows.append((d, "UP", 0.0))                              # ทายขึ้นแต่ไม่ขยับ → ไม่ถูก
            elif i % 5 == 1:
                rows.append((d, "DOWN", -0.45))
            elif i % 5 == 2:
                rows.append((d, "UNCLEAR", 0.30))                        # วันอื่นขึ้นบ่อย → อัตราปกติของทิศขึ้นสูง
            else:
                rows.append((d, "UNCLEAR", 0.0))
        s = engine.outlook_alert_status(_log(rows), self.SUMMARY, "B7")
        self.assertFalse(s["eligible"]["UP"])
        self.assertTrue(s["eligible"]["DOWN"], s)

    def test_gain_below_floor_blocks_everything(self):
        rows = [(d, "UP" if i % 5 == 0 else "UNCLEAR", 0.02) for i, d in enumerate(_days(60))]
        s = engine.outlook_alert_status(_log(rows), self.SUMMARY, "B7")
        self.assertFalse(any(s["eligible"].values()))
        self.assertIn("ต่ำกว่า", s["reasons"][0])


if __name__ == "__main__":
    unittest.main()

class LagLedgerTest(unittest.TestCase):
    """S1: คำพยากรณ์ที่ cron มาไม่ทันยังถูกบันทึก (ติด lag_h) แต่ไม่นับในเกณฑ์ ; แถวย้อนหลังไม่นับเลย"""

    def _snap(self, asof, ref="2026-11-03 20:30"):
        return [{"asof": asof, "ref": ref, "fuel": "B7", "hours": 24, "tier": "UP", "grade": "กลาง", "model_version": "v"}]

    def test_late_row_is_kept_with_lag_h(self):
        add = log_outlook.new_rows([], self._snap("2026-11-03 23:32"))               # ช้า 3 ชม. 2 นาที: เดิมถูกทิ้ง
        self.assertEqual(len(add), 1)
        self.assertAlmostEqual(add[0]["lag_h"], 3.03, places=2)
        self.assertEqual(add[0]["backfilled"], 0)

    def test_row_older_than_cap_is_dropped(self):
        self.assertEqual(log_outlook.new_rows([], self._snap("2026-11-04 08:40")), [])   # 12 ชม. 10 นาที

    def test_earliest_row_wins_when_logged_twice(self):
        first = log_outlook.new_rows([], self._snap("2026-11-03 21:10"))
        self.assertEqual(log_outlook.new_rows(first, self._snap("2026-11-03 23:32")), [])

    def test_header_and_row_width_match(self):
        row = log_outlook.new_rows([], self._snap("2026-11-03 21:00"))[0]
        self.assertEqual(list(row), log_outlook.COLUMNS)
        self.assertEqual(log_outlook.COLUMNS[-2:], ["lag_h", "backfilled"])
        self.assertEqual(log_outlook.LAST_COL, "R")

    def test_on_time_and_late_counted_separately(self):
        log = pd.DataFrame([
            {"asof": "2026-11-02 21:30", "ref": "2026-11-02 20:30", "date": "2026-11-02", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "0.30"},
            {"asof": "2026-11-03 23:32", "ref": "2026-11-03 20:30", "date": "2026-11-03", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "0.20"},
            {"asof": "2026-11-05 23:50", "ref": "2026-11-05 20:30", "date": "2026-11-05", "fuel": "B7", "hours": 24, "tier": "DOWN", "outcome_delta": "-0.10", "backfilled": 1},
        ])
        self.assertEqual(engine.outlook_live_stats(log, "B7", 24)["n_days"], 1)          # ตรงเวลา = 2 พ.ย. เท่านั้น
        self.assertEqual(engine.outlook_late_days(log, "B7", 24), 1)                      # ล็อกช้า = 3 พ.ย. ; แถว backfilled ไม่นับ
        self.assertEqual(engine.outlook_alert_status(log, None, "B7")["n"], 1)            # เกณฑ์ความพร้อมนับเฉพาะตรงเวลา

    def test_old_rows_without_new_columns_still_work(self):
        log = pd.DataFrame([{"asof": "2026-10-22 21:00", "ref": "2026-10-22 20:30", "date": "2026-10-22", "fuel": "B7", "hours": 24, "tier": "UP", "outcome_delta": "0.30"}])
        self.assertEqual(engine.outlook_live_stats(log, "B7", 24)["n_days"], 1)
        self.assertEqual(engine.outlook_late_days(log, "B7", 24), 0)


class LogRefsTest(unittest.TestCase):
    def refs(self, ts):
        return [r.strftime("%m-%d %H:%M") for r in engine.log_refs(pd.Timestamp(ts))]

    def test_late_evening_run_gets_both_refs(self):
        self.assertEqual(self.refs("2026-11-03 23:32"), ["11-03 15:30", "11-03 20:30"])

    def test_after_midnight_run_gets_previous_day(self):
        self.assertEqual(self.refs("2026-11-04 00:39"), ["11-03 15:30", "11-03 20:30"])

    def test_afternoon_run_gets_only_pm(self):
        self.assertEqual(self.refs("2026-11-05 16:50"), ["11-05 15:30"])

    def test_morning_run_gets_nothing(self):
        self.assertEqual(self.refs("2026-11-05 10:00"), [])

    def test_cap_boundary(self):
        self.assertEqual(self.refs("2026-11-04 03:30"), ["11-03 15:30", "11-03 20:30"])    # 15:30 ช้า 12 ชม. พอดี ยังนับ
        self.assertEqual(self.refs("2026-11-04 03:31"), ["11-03 20:30"])                    # เกิน 12 ชม. แล้ว


class OutlookReasonsTest(unittest.TestCase):
    def test_horizons_carry_reasons_from_active_features(self):
        import model as M
        import signals as S
        params = M.load_params()
        rows = [{"msg_date": "2026-10-20", "msg_time": h, "provider": p, "direction": d, "scope": "ALL", "stated_time": ""}
                for h, p, d in [("07:00", "SFL", "UP"), ("17:30", "IRPC", "UP"), ("18:10", "PTT", "DOWN")]]
        t = S.Tables(intraday=pd.DataFrame([{"msg_date": "2026-10-20", "msg_time": "15:00", "kind": "TREND", "b": 0.7, "d": 2.0}]), headsup=pd.DataFrame(rows),
                     mops_close=pd.DataFrame([{"msg_date": "2026-10-20", "msg_time": "20:10", "mops_date": "2026-10-20", "g95": 100, "g95_chg": -3.0, "diesel": 150, "diesel_chg": -5.0}]),
                     fund=pd.DataFrame())
        d = engine.decide_outlook("DIESEL", pd.Timestamp("2026-10-20 20:35"), t, params, None)
        for h in d["horizons"]:
            self.assertTrue(h["reasons"])
            self.assertTrue(any("MOPS ปิดล่าสุด" in r for r in h["reasons"]))
