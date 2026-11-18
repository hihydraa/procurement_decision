"""เตือน "โอกาสปรับสูงกว่าปกติ" ของราคาปลีก (retail_forecast.elevated) — เฉพาะแสดงผล ไม่เปลี่ยนคำทาย"""
import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import line_outlook as L      # noqa: E402
import retail_forecast as RF  # noqa: E402


class Elevated(unittest.TestCase):
    def test_night_of_17_nov(self):
        """ค่าจริงคืน 17/11/2026 (ปลีกลดจริง 18/11) : ดีเซลลง 17.2% , G95 ลง 23.5% → เตือนทั้งคู่"""
        b7 = RF.elevated("B7", "นิ่ง", 0.172, 0.0, "SIG")
        g95 = RF.elevated("G95", "นิ่ง", 0.235, 0.013, "SIG")
        self.assertEqual((b7["dir"], g95["dir"]), ("ลง", "ลง"))
        self.assertIn("โอกาสปรับลงสูงกว่าปกติ 17%", RF.elevated_text(b7))

    def test_quiet_nights_and_rules(self):
        self.assertIsNone(RF.elevated("B7", "นิ่ง", 0.025, 0.014, "SIG"))      # 17/11 ของคืน 16/11
        self.assertIsNone(RF.elevated("G95", "นิ่ง", 0.187, 0.018, "SIG"))     # ต่ำกว่า 2 เท่าของปกติ (~20%)
        self.assertIsNone(RF.elevated("G95", "นิ่ง", 0.05, 0.40, "SIG"))       # G95 ฝั่งขึ้น : ย้อนหลังเตือนแล้วไม่ช่วย → ไม่เปิด
        self.assertIsNone(RF.elevated("B7", "ลง", 0.50, 0.0, "SIG"))           # ฟันธงอยู่แล้ว ไม่ต้องเตือนซ้ำ
        self.assertIsNone(RF.elevated("B7", "นิ่ง", 0.30, 0.0, "OLD"))         # หลักฐานวัดจากโมเดลสัญญาณเท่านั้น
        self.assertIsNone(RF.elevated("B7", "นิ่ง", "", None, "SIG"))
        self.assertEqual(RF.elevated("B7", "ไม่ชัด", 0.05, 0.20, "SIG")["dir"], "ขึ้น")

    def test_line_message_shows_warning(self):
        retail = pd.DataFrame([{"card_id": "2026-11-18 21:30 B7", "asof": "2026-11-17 21:42", "fuel": "B7", "target_day": "2026-11-18",
                                "call": "นิ่ง", "p_down": 0.172, "p_flat": 0.828, "p_up": 0.0, "retail_base": 40.79, "model": "SIG",
                                "size_text": "", "outcome_cls": "", "filled_at": ""}])
        import procurement_summary as PS
        lines = L.retail_lines(PS.prep_retail(retail), pd.Timestamp("2026-11-17 22:00"))
        i = next(k for k, t in enumerate(lines) if t.startswith("ดีเซล (วันที่ 18/11)"))
        self.assertIn("โอกาสปรับลงสูงกว่าปกติ", lines[i + 1])


if __name__ == "__main__":
    unittest.main()
