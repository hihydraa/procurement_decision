import unittest

import pandas as pd

import diag_pricelog as P

HEAD = ["timestamp_logged", "message_id", "group_id", "jobber", "provider", "warehouse_code", "warehouse_name", "fuel_type", "price",
        "update_date", "update_time", "effective_time", "confidence", "raw_oil_type_line", "raw_price_line", "raw_text", "announced_price"]


def row(jobber, provider, wh, fuel, price, ann, date="2026-11-09", time="10:30", eff="10:25"):
    return ["", "", "", jobber, provider, "", wh, fuel, price, date, time, eff, 0.95, "", "", "", ann]


class EffectiveTest(unittest.TestCase):
    def test_same_day_and_next_day_rules(self):
        self.assertEqual(P.effective_ts("2026-11-09", "10:30", "10:25"), pd.Timestamp("2026-11-09 10:25"))
        self.assertEqual(P.effective_ts("2026-11-09", "17:10", "00:00"), pd.Timestamp("2026-11-10 00:00"))     # ประกาศเย็น มีผลเที่ยงคืน = วันถัดไป
        self.assertEqual(P.effective_ts("2026-11-09", "17:10", "23:30"), pd.Timestamp("2026-11-09 23:30"))
        self.assertEqual(P.effective_ts("2026-11-09", "10:30", ""), pd.Timestamp("2026-11-09 10:30"))         # ไม่ระบุ = เวลาโพสต์
        self.assertTrue(pd.isna(P.effective_ts("", "10:30", "10:25")))


class FrameTest(unittest.TestCase):
    def setUp(self):
        vals = [HEAD,
                row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", 39.55, 39.59),
                row("ต้องบุญ", "ไออาร์", "อยุธยา", "B7", 39.78, 39.82),
                row("ต้องบุญ", "สตาร์ฟูเอลส์ (ดาว)", "สระบุรี", "B7", 39.94, 40.00),
                row("ศรีพลัง", "ไออาร์", "ระยอง", "B7", 39.60, 39.60, time="10:34"),
                row("ศรีพลัง", "ไออาร์", "อยุธยา", "B7", 39.83, 39.83, time="10:34"),
                # วันที่/เวลาเป็นเลข serial อย่างที่ Sheets ส่งมาเมื่ออ่านแบบ UNFORMATTED
                row("ต้องบุญ", "บางจาก", "บางปะอิน", "G95", 37.54, 37.60, date=46335, time=0.4375, eff=0.43055),   # 2026-11-09 10:30 / 10:20
                row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", "", 39.0),                                              # ไม่มีราคา ข้าม
                ]
        self.df = P.build_frame(vals)

    def test_frame_parses_and_skips(self):
        self.assertEqual(len(self.df), 6)
        self.assertEqual(self.df.effective.min(), pd.Timestamp("2026-11-09 10:20"))
        self.assertEqual(self.df.iloc[-1].group, "บางจาก")

    def test_discount_report(self):
        lines = "\n".join(P.discount_report(self.df))
        self.assertIn("IRPC    ลด 0.04: 2 (100%", lines)
        self.assertIn("ดาว     ลด 0.06: 1 (100%", lines)
        self.assertIn("บางจาก  ลด 0.06: 1 (100%", lines)
        self.assertIn("ศรีพลัง: 2 แถว", lines)
        self.assertIn("IRPC    ลด 0.00: 2 (100%", lines)

    def test_jobber_gap(self):
        lines = "\n".join(P.jobber_gap_report(self.df))
        self.assertIn("= 2 คู่", lines)
        self.assertIn("IRPC    n=   2", lines)
        self.assertIn("ต้องบุญถูกกว่า 100%", lines)
        self.assertIn("เฉลี่ย +0.050", lines)                    # (39.60-39.55 + 39.83-39.78)/2
        self.assertIn("เทียบประกาศต้องบุญ เฉลี่ย +0.010", lines)    # 39.60-39.59 ; 39.83-39.82


class TimingTest(unittest.TestCase):
    def test_buckets_and_big_moves(self):
        rows = [HEAD]
        # ไออาร์ ระยอง B7 : 30 ครั้งสลับขึ้นใหญ่ 00:00 (+0.50) / ลงเล็ก 10:25 (−0.10)
        price, day = 39.0, pd.Timestamp("2026-11-01")
        for i in range(15):
            d = (day + pd.Timedelta(days=i)).strftime("%Y-%m-%d")
            price += 0.50
            rows.append(row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", round(price, 2), price, date=d, time="17:05", eff="00:00"))   # → วันถัดไป 00:00
            price -= 0.10
            rows.append(row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", round(price, 2), price, date=(day + pd.Timedelta(days=i + 1)).strftime("%Y-%m-%d"), time="10:30", eff="10:25"))
        df = P.build_frame(rows)
        lines = "\n".join(P.change_timing_report(df))
        self.assertIn("IRPC    n=   29", lines)
        self.assertIn("00:00-00:59 15/0 (15/0)", lines)         # ขึ้น 15 ครั้ง ทั้งหมดเป็นการขึ้นใหญ่ ที่เที่ยงคืน
        self.assertIn("09:00-12:59 0/14 (0/0)", lines)          # ลงเล็ก 14 ครั้งช่วงสาย (ครั้งแรกไม่มีค่าก่อนหน้า)


if __name__ == "__main__":
    unittest.main()
