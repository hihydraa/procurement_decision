import unittest
from datetime import date

import pandas as pd

import diag_po as dp


def prices(rows):
    """rows = (provider, warehouse, fuel, effective, price) ; jobber คงที่"""
    return pd.DataFrame([{"jobber": "ต้องบุญ", "provider": p, "warehouse": w, "fuel": f, "effective": pd.Timestamp(t), "price": x}
                         for p, w, f, t, x in rows])


class Names(unittest.TestCase):
    def test_depot_map(self):
        self.assertEqual(dp.po_depot("IR ระยอง"), "ไออาร์ ระยอง")
        self.assertEqual(dp.po_depot("ดาว สระบุรี"), "ดาว สระบุรี")
        self.assertIsNone(dp.po_depot("TPN บ้านไผ่"))
        self.assertEqual(dp.po_depot("IR นครหลวง"), "ไออาร์ อยุธยา")
        self.assertEqual(dp.po_depot("ไออาร์ ระยอง"), "ไออาร์ ระยอง")

    def test_transport(self):
        self.assertEqual(dp.transport("ดาว สระบุรี", "ยางตลาด"), 0.63)
        self.assertEqual(dp.transport("ไออาร์ ระยอง", "กระนวน"), 0.80)
        self.assertEqual(dp.transport("ดาว สระบุรี", "วานรนิวาส"), 0.75)


class Bench(unittest.TestCase):
    def test_timing_and_depot_selection(self):
        d = pd.Timestamp("2026-10-15")
        # ดาว สระบุรี: 40.0 ตั้งแต่เย็นวันก่อน, ลดเป็น 39.5 เวลา 05:00 ; ไออาร์ ระยอง: 39.4 ตลอด
        p = prices([("สตาร์ฟูเอลส์", "สระบุรี", "B7", d - pd.Timedelta(hours=6), 40.0), ("สตาร์ฟูเอลส์", "สระบุรี", "B7", d + pd.Timedelta(hours=5), 39.5),
                    ("ไออาร์", "ระยอง", "B7", d - pd.Timedelta(hours=6), 39.4)])
        po = pd.DataFrame([{"date": date(2026, 10, 15), "depot": "ดาว สระบุรี", "fuel": "B7", "liters": 1000.0, "price": 40.0, "dest": "ยางตลาด",
                            "transport": 0.63, "price_note": ""}])
        b = dp.benchmark(po, p).iloc[0]
        self.assertEqual(b["why"], "")
        self.assertAlmostEqual(b["timing"], 0.5)                    # จ่าย 40.0 ต่ำสุดของคลังคือ 39.5
        # ต้นทุนส่งถึง: ดาว 39.5+0.63=40.13 ; ไออาร์ 39.4+0.80=40.20 → ต่ำสุด 40.13 ; จ่ายจริง 40.63 → ส่วนเกิน 0.50 เลือกคลังถูกแล้ว
        self.assertAlmostEqual(b["excess"], 0.5)
        self.assertAlmostEqual(b["depot_sel"], 0.0)

    def test_depot_choice_cost(self):
        d = pd.Timestamp("2026-10-15")
        p = prices([("สตาร์ฟูเอลส์", "สระบุรี", "B7", d - pd.Timedelta(hours=6), 40.0), ("ไออาร์", "ระยอง", "B7", d - pd.Timedelta(hours=6), 39.0)])
        po = pd.DataFrame([{"date": date(2026, 10, 15), "depot": "ดาว สระบุรี", "fuel": "B7", "liters": 1000.0, "price": 40.0, "dest": "ยางตลาด",
                            "transport": 0.63, "price_note": ""}])
        b = dp.benchmark(po, p).iloc[0]
        self.assertAlmostEqual(b["timing"], 0.0)
        self.assertAlmostEqual(b["depot_sel"], 40.63 - 39.80)       # ไออาร์ถูกกว่าหลังรวมค่าขนส่ง

    def test_skip_reasons(self):
        base = {"date": date(2026, 10, 15), "fuel": "B7", "liters": 1.0, "price": 40.0, "transport": 0.63, "price_note": ""}
        po = pd.DataFrame([{**base, "depot": "TPN บ้านไผ่", "dest": "ยางตลาด"}, {**base, "depot": "ดาว สระบุรี", "dest": ""},
                           {**base, "depot": "ดาว สระบุรี", "dest": "ยางตลาด"}])
        b = dp.benchmark(po, prices([("ไออาร์", "ระยอง", "G95", "2026-10-14 10:00", 39.0)]))
        self.assertEqual(list(b["why"]), ["นอกคลังหลัก", "ไม่ระบุปลายทาง", "ไม่มีราคาตลาดของคลังที่ซื้อ"])

    def test_after_midnight_note_uses_longer_window(self):
        d = pd.Timestamp("2026-10-15")
        # ราคาเช้าวันก่อน 39.19 (ก่อน 20:30) ซื้อด้วยราคาวันก่อน : หน้าต่างยาวต้องเห็นราคานี้ จึงไม่ติดลบ
        p = prices([("ไออาร์", "ระยอง", "B7", d - pd.Timedelta(hours=18), 39.19), ("ไออาร์", "ระยอง", "B7", d - pd.Timedelta(hours=8), 39.60)])
        base = {"date": date(2026, 10, 15), "depot": "IR ระยอง", "fuel": "B7", "liters": 1.0, "price": 39.19, "dest": "ยางตลาด", "transport": 0.80}
        short = dp.benchmark(pd.DataFrame([{**base, "price_note": ""}]), p).iloc[0]
        long_ = dp.benchmark(pd.DataFrame([{**base, "price_note": "ราคาวันที่ 14 โหลดน้ำมันหลังเที่ยงคืน"}]), p).iloc[0]
        self.assertEqual(short["why"], "ราคาจ่ายต่ำกว่า PriceLog")
        self.assertEqual(long_["why"], "")
        self.assertAlmostEqual(long_["timing"], 0.0)


class Outliers(unittest.TestCase):
    def test_flags_only_unexpected_discount(self):
        base = {"date": date(2026, 10, 1), "tab": "01", "row": 5, "fuel": "B7", "liters": 1000.0, "provider": "p", "price_note": "", "change_note": ""}
        po = pd.DataFrame([{**base, "depot": "ดาว สระบุรี", "price": 39.81, "list_price": 39.87},      # 0.06 ปกติ
                           {**base, "depot": "ดาว สระบุรี", "price": 40.49, "list_price": 39.87},      # -0.62
                           {**base, "depot": "ดาว สระบุรี", "price": 39.83, "list_price": 39.87},      # 0.04
                           {**base, "depot": "IR ระยอง", "price": 39.19, "list_price": 39.23}])      # 0.04 ปกติของไออาร์
        out = dp.discount_outliers(po)
        self.assertIn("2/4", out[0])
        self.assertTrue(any("-0.62" in x for x in out) and any("+0.04" in x for x in out))


class Reports(unittest.TestCase):
    def test_wmean_and_cost_report(self):
        b = pd.DataFrame([{"fuel": "B7", "liters": 1000.0, "timing": 0.2, "depot_sel": 0.0, "excess": 0.2, "why": ""},
                          {"fuel": "B7", "liters": 3000.0, "timing": 0.0, "depot_sel": 0.4, "excess": 0.4, "why": ""},
                          {"fuel": "B7", "liters": 500.0, "timing": float("nan"), "depot_sel": float("nan"), "excess": float("nan"), "why": "นอกคลังหลัก"}])
        self.assertAlmostEqual(dp.wmean(b.dropna(subset=["excess"]), "excess"), 0.35)
        out = dp.cost_report(b)
        self.assertIn("เทียบได้ 2/3", out[0])
        self.assertTrue(any("รวม" in x and "+0.350" in x for x in out))


if __name__ == "__main__":
    unittest.main()
