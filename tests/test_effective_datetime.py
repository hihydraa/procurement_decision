import unittest
from datetime import datetime, timedelta
from unittest import mock

import ingest as I


class EffectiveDatetimeTest(unittest.TestCase):
    def eff(self, d, t, e):
        return I.effective_datetime(d, t, e).strftime("%Y-%m-%d %H:%M")

    def test_no_effective_time_uses_post_time(self):
        self.assertEqual(self.eff("2026-11-09", "10:30", ""), "2026-11-09 10:30")
        self.assertEqual(self.eff("2026-11-09", "", ""), "2026-11-09 00:00")

    def test_effective_not_earlier_than_post_stays_same_day(self):
        self.assertEqual(self.eff("2026-11-09", "10:30", "10:25"), "2026-11-09 10:25")      # ห่างไม่ถึง 2 ชม.
        self.assertEqual(self.eff("2026-11-09", "17:10", "23:30"), "2026-11-09 23:30")      # ประกาศล่วงหน้าเวลาเย็นของวันเดียวกัน
        self.assertEqual(self.eff("2026-11-09", "00:30", "23:59"), "2026-11-09 23:59")      # ยังเหมือนเดิม

    def test_after_the_fact_morning_post_is_same_day(self):
        # โพสต์ 08:08 บอกว่า 05.00 น. ราคาขึ้นไปแล้ว → เมื่อเช้า ไม่ใช่เช้าพรุ่งนี้ (เดิมเลื่อนเป็นวันถัดไปผิด)
        self.assertEqual(self.eff("2026-11-09", "08:08", "05:00"), "2026-11-09 05:00")
        self.assertEqual(self.eff("2026-11-09", "08:25", "02:00"), "2026-11-09 02:00")
        self.assertEqual(self.eff("2026-11-09", "11:15", "08:35"), "2026-11-09 08:35")

    def test_evening_announcement_for_tomorrow_still_next_day(self):
        self.assertEqual(self.eff("2026-11-09", "17:05", "05:00"), "2026-11-10 05:00")
        self.assertEqual(self.eff("2026-11-09", "17:10", "00:00"), "2026-11-10 00:00")
        self.assertEqual(self.eff("2026-11-09", "23:10", "01:00"), "2026-11-10 01:00")

    def test_noon_boundary(self):
        self.assertEqual(self.eff("2026-11-09", "11:59", "09:00"), "2026-11-09 09:00")     # ก่อน 12:00 → วันเดียวกัน
        self.assertEqual(self.eff("2026-11-09", "12:00", "09:00"), "2026-11-10 09:00")     # ตั้งแต่ 12:00 → วันถัดไปตามเดิม
        self.assertEqual(self.eff("2026-11-09", "11:59", "09:59"), "2026-11-09 09:59")     # ห่างไม่เกิน 2 ชม. อยู่แล้ว ไม่เปลี่ยน


class PricesFeedTest(unittest.TestCase):
    def test_feed_uses_new_rule(self):
        today = datetime.now(I.TZ).date()
        d = today.isoformat()
        head = ["provider", "warehouse_name", "fuel_type", "price", "update_date", "update_time", "effective_time"]
        values = [head,
                  ["ไออาร์", "ระยอง", "B7", 40.81, d, "08:08", "05:00"],        # ย้อนหลังตอนเช้า → วันนี้ 05:00
                  ["ไออาร์", "อยุธยา", "B7", 39.8, d, "17:05", "05:00"],        # ประกาศล่วงหน้าเย็น → พรุ่งนี้ 05:00
                  ["ไออาร์", "ชุมพร", "B7", 40.0, d, "10:30", ""]]               # ไม่ระบุ → เวลาโพสต์
        with mock.patch("ingest.sheets_get", return_value=values):
            out = I._prices_feed("tok")
        self.assertEqual(out[0], ["provider", "warehouse", "fuel", "price", "effective", "posted"])
        self.assertEqual([r[5] for r in out[1:]], [f"{d} 08:08", f"{d} 17:05", f"{d} 10:30"])
        self.assertEqual([r[4] for r in out[1:]], [f"{d} 05:00", f"{(today + timedelta(days=1)).isoformat()} 05:00", f"{d} 10:30"])


if __name__ == "__main__":
    unittest.main()
