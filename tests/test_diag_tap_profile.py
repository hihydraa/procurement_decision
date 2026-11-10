import unittest

import pandas as pd

import diag_pricelog as P
import diag_tap_profile as T

HEAD = ["timestamp_logged", "message_id", "group_id", "jobber", "provider", "warehouse_code", "warehouse_name", "fuel_type", "price",
        "update_date", "update_time", "effective_time", "confidence", "raw_oil_type_line", "raw_price_line", "raw_text", "announced_price"]


def row(jobber, provider, wh, fuel, price, ann, date="2026-11-09", time="10:30", eff="10:25", text="", msg=""):
    return [date, msg, "", jobber, provider, "", wh, fuel, price, date, time, eff, 0.95, "", "", text, ann]


def cycle_frame(days=7, rise=0.5, cut=0.4, base=39.1):
    """ทุกวัน : ขึ้น +rise ตอน 05:00 (ประกาศเย็นวันก่อน) แล้วตัดลง −cut ตอน 10:25 → ราคาต่ำสุดของช่วงคือ base ทุกวัน"""
    rows = [HEAD]
    start = pd.Timestamp("2026-11-01")
    rows.append(row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", base, base, date="2026-10-31", time="10:30", eff="10:25"))
    for i in range(days):
        d = start + pd.Timedelta(days=i)
        prev = (d - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        rows.append(row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", base + rise, base + rise, date=prev, time="17:00", eff="05:00"))   # → วัน D 05:00
        rows.append(row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", base + rise - cut, base + rise - cut, date=d.strftime("%Y-%m-%d"), time="10:30", eff="10:25"))
    return P.build_frame(rows).reset_index(drop=True)


class ProfileTest(unittest.TestCase):
    def test_price_at_uses_min_across_series_and_ignores_stale(self):
        a = pd.DataFrame({"effective": [pd.Timestamp("2026-11-01 08:00")], "price": [40.0]})
        b = pd.DataFrame({"effective": [pd.Timestamp("2026-10-30 08:00")], "price": [39.0]})          # เก่า 52 ชม.
        t = pd.DatetimeIndex(["2026-11-01 10:00", "2026-10-29 10:00"])
        got = T.price_at([a, b], t)
        self.assertEqual(got[0], 40.0)                                                                  # ราคาเก่าถูกตัดทิ้ง
        self.assertTrue(pd.isna(got[1]))

    def test_regret_by_tap_time_for_daily_cycle(self):
        rows = T.day_rows(cycle_frame())
        self.assertGreaterEqual(len(rows), 3)
        r = rows[(rows.depot == "ไออาร์ ระยอง") & (rows.fuel == "B7")].iloc[0]
        self.assertEqual(r.cls, "ทรง")                                                                  # 20:30 ทุกวันเท่ากัน
        for k in ("ก่อน20:30", "ก่อน22:30", "01:30", "04:30", "10:45", "14:00", "16:30"):
            self.assertAlmostEqual(r[k], 0.0, places=6, msg=k)
        for k in ("07:00", "09:00"):                                                                    # ช่วงหลังขึ้นก่อนตัด
            self.assertAlmostEqual(r[k], 0.4, places=6, msg=k)

    def test_report_lines_and_oracle(self):
        lines = "\n".join(T.profile_report(T.day_rows(cycle_frame())))
        self.assertIn("ไออาร์ ระยอง", lines)
        self.assertIn("07:00 0.40", lines)
        self.assertIn("ก่อน20:30 0.00*", lines)
        self.assertIn("วันทรง", lines)
        self.assertIn("จุดแตะคงที่ที่ดีที่สุด", lines)

    def test_classes_follow_24h_change(self):
        rows = [HEAD]
        # ราคาเพิ่มวันละ +0.5 ที่ 05:00 ไม่มีการตัด → 20:30 สูงกว่าวันก่อน +0.5 = วันขึ้น
        for i in range(8):
            d = pd.Timestamp("2026-11-01") + pd.Timedelta(days=i)
            rows.append(row("ต้องบุญ", "ไออาร์", "ระยอง", "B7", 39.0 + 0.5 * i, 39.0, date=d.strftime("%Y-%m-%d"), time="05:10", eff="05:00"))
        out = T.day_rows(P.build_frame(rows).reset_index(drop=True))
        self.assertTrue((out.cls == "ขึ้น").all())
        self.assertTrue(((out.delta - 0.5).abs() < 1e-9).all())


if __name__ == "__main__":
    unittest.main()
