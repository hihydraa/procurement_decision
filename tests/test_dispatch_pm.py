"""แผนส่งรถช่วงบ่ายในไลน์ (เจ้าของระบบ 24/11/2026) : หน้างานตัดสินใจ 10:00–20:00 → การ์ด/ข้อความ/แจ้งเตือนจาก engine.decide_dispatch
ผลจริงแยกตามช่วงที่ออกคำแนะนำ (บ่าย/ค่ำ) แทนการรวมก้อนเดียว"""
import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import line_flex as F  # noqa: E402
import line_outlook as L  # noqa: E402
import test_line_outlook as T  # noqa: E402


def _dlog():
    """DispatchLog : D15 ดีเซล 3 วัน (เร่ง 2 ครั้งขึ้นจริงทั้งคู่ , ชะลอ 1 ครั้งลงจริง) ; D20 1 วัน ; แถวซ้ำวัน/ช่วงเดียวกันนับรอบแรก"""
    rows = [["2026-11-20 15:47", "2026-11-20", "D15", "B7", 10, "URGENT", 0.60],
            ["2026-11-20 17:05", "2026-11-20", "D15", "B7", 10, "WAIT", -1.00],      # รอบที่สองของวันเดียวกัน ไม่นับ
            ["2026-11-21 15:47", "2026-11-21", "D15", "B7", 10, "URGENT", 0.40],
            ["2026-11-22 15:47", "2026-11-22", "D15", "B7", 10, "WAIT", -0.50],
            ["2026-11-22 20:47", "2026-11-22", "D20", "B7", 10, "URGENT", -0.30],
            ["2026-11-22 15:47", "2026-11-22", "D15", "B7", 8, "URGENT", 0.90]]      # ระยะเดินทางอื่น ไม่นับ
    return pd.DataFrame(rows, columns=["asof", "date", "window", "fuel", "lead", "action_code", "outcome_delta"])


def _decisions(b7="URGENT", g95="NORMAL", asof="2026-11-24 15:47", win="D15"):
    a = pd.Timestamp(asof)
    mk = lambda fuel, code, grade: {"fuel": fuel, "window": win, "lead": 10, "asof": a, "arrive": a + pd.Timedelta(hours=10),
                                    "action_code": code, "raw_action_code": code if grade != "อ่อน" else "WAIT", "quality": {"grade": grade},
                                    "p_rise": 0.62, "p_fall": 0.08, "base_rise": 0.35, "base_fall": 0.30}
    return {"diesel": mk("B7", b7, "ดี"), "gasohol95": mk("G95", g95, "อ่อน"), "x": {"fuel": "B7", "action_code": "NO_DATA"}}


def _inp_pm(**kw):
    inp = T._inp(asof="2026-11-24 15:47", ref="2026-11-24 15:30")
    inp["dispatch"] = L.dispatch_input(_decisions(**kw), _dlog(), 10)
    return inp


class DispatchInputTest(unittest.TestCase):
    def test_live_stats_by_window(self):
        dp = L.dispatch_input(_decisions(), _dlog(), 10)
        self.assertEqual(set(dp), {"B7", "G95"})                                     # NO_DATA ไม่ใส่
        lv = dp["B7"]["live"]
        self.assertEqual(lv["n"], 3)                                                 # D15 เท่านั้น , ระยะ 10 ชม. , รอบแรกของวัน
        self.assertAlmostEqual(lv["gain"], (0.60 + 0.40 + 0.50) / 3)
        self.assertEqual(lv["urgent"], {"n": 2, "rate": 1.0})
        self.assertEqual(lv["wait"], {"n": 1, "rate": 1.0})
        self.assertIsNone(dp["G95"]["live"])
        self.assertEqual((dp["B7"]["arrive"], dp["G95"]["grade"]), ("2026-11-25 01:47", "อ่อน"))

    def test_view(self):
        dp = L.dispatch_input(_decisions(), _dlog(), 10)
        v = L.dispatch_view(dp["B7"])
        self.assertEqual((v["verdict"], v["tone"]), ("⚡ เร่งส่งรถ", "up"))
        self.assertEqual(v["why"], "ถ้ารอ 1 วัน โอกาสราคา ณ เวลาถึงคลังแพงขึ้น 62% (ปกติ 35%)")
        self.assertEqual(v["arrive"], "ออก 15:47 ถึงคลังราว 01:47 (25/11) (เดินทาง 10 ชม.)")
        self.assertEqual(v["live"], "ผลจริงช่วง 15:30–20:30: 3 วัน · ได้เปรียบเฉลี่ย +0.50 บาท/ลิตร/วัน · เร่งส่ง 2 ครั้ง ราคาขึ้นจริง 100%")
        g = L.dispatch_view(dp["G95"])
        self.assertEqual((g["verdict"], g["tone"]), ("➖ ส่งตามแผน", "flat"))
        self.assertIn("ยังไม่แม่นพอ", g["why"])
        self.assertEqual(g["live"], "ผลจริงช่วงนี้: ยังไม่มี")


class DispatchAlertTest(unittest.TestCase):
    def test_sends_urgent_or_wait_in_window(self):
        text, ref = L.dispatch_alert(_inp_pm(), pd.Timestamp("2026-11-24 15:50"))
        self.assertEqual(ref, "dispatch-20261124-D15")
        self.assertTrue(text.startswith("🚚 ส่งรถตอนนี้ดีไหม (ข้อมูล 15:47 น. · ทดลอง)"))
        self.assertIn("ดีเซล: ⚡ เร่งส่งรถ — ถ้ารอ 1 วัน", text)
        self.assertNotIn("แก๊สโซฮอล์ 95", text)                                       # ส่งตามแผน = ไม่แจ้ง
        text, _ = L.dispatch_alert(_inp_pm(b7="WAIT"), pd.Timestamp("2026-11-24 15:50"))
        self.assertIn("ดีเซล: ⏸ ชะลอได้ — ถ้ารอ 1 วัน โอกาสถูกลง 8%", text)

    def test_silent_cases(self):
        now = pd.Timestamp("2026-11-24 15:50")
        self.assertEqual(L.dispatch_alert(_inp_pm(b7="NORMAL"), now), (None, None))
        self.assertEqual(L.dispatch_alert(_inp_pm(b7="GRADUAL"), now), (None, None))
        self.assertEqual(L.dispatch_alert(_inp_pm(asof="2026-11-24 13:10", win="D12"), now), (None, None))   # ช่วงเที่ยงไม่แจ้ง
        self.assertEqual(L.dispatch_alert(_inp_pm(), pd.Timestamp("2026-11-24 19:05")), (None, None))      # รถออกไปแล้ว
        self.assertEqual(L.dispatch_alert(_inp_pm(), pd.Timestamp("2026-11-25 00:10")), (None, None))
        self.assertEqual(L.dispatch_alert(T._inp(), now), (None, None))                                  # ไม่มีข้อมูลแผนส่งรถ


class DispatchDisplayTest(unittest.TestCase):
    def test_text_afternoon_only(self):
        text = L.build_message(_inp_pm(), T._log(), on_demand=True)[0]
        self.assertIn("🚚 ส่งรถตอนนี้ดีไหม [ทดลอง] · ประเมิน 15:47 น.", text)
        self.assertIn("รอบ 15:30 (1/40 วัน): ฟันธง 1 · ถูก 0 · ผิดทิศ 1 · ราคานิ่ง 0", text)
        evening = T._inp(asof="2026-11-24 21:35", ref="2026-11-24 20:30")
        evening["dispatch"] = L.dispatch_input(_decisions(asof="2026-11-24 21:35", win="D20"), _dlog(), 10)
        self.assertNotIn("🚚", L.build_message(evening, T._log(), on_demand=True)[0])

    def test_flex_first_bubble(self):
        car = F.build_flex(_inp_pm(), T._log(), None)["contents"]["contents"]
        self.assertEqual(car[0]["header"]["contents"][0]["text"], "🚚 ส่งรถตอนนี้ดีไหม")
        self.assertEqual(car[0]["header"]["contents"][1]["text"], "ประเมิน 15:47 น. 24/11 · ระยะเดินทาง 10 ชม.")
        perf = [n["text"] for n in __import__("test_line_flex").walk(car[-1], []) if n.get("type") == "text"]
        self.assertIn("รอบ 15:30 (1/40 วัน): ฟันธง 1 · ถูก 0 · ผิดทิศ 1 · ราคานิ่ง 0", perf)
        evening = F.build_flex(T._inp(), T._log(), None)["contents"]["contents"]
        self.assertEqual(evening[0]["header"]["contents"][0]["text"], "🧭 ควรโหลดเมื่อไร")

    def test_dashboard_split_by_window(self):
        import main
        h = main.build_dispatch_live(_dlog(), 10)
        self.assertIn("ดีเซล · บ่าย 15:30–20:30</td><td>3 วัน", h)
        self.assertIn("ดีเซล · ค่ำ 20:30 ขึ้นไป</td><td>1 วัน", h)


if __name__ == "__main__":
    unittest.main()
