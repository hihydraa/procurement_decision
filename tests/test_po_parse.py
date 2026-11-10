import unittest
from datetime import date

import po_parse as pp


def pad(row, n=26):
    return row + [""] * (n - len(row))


# สองบล็อกจากแท็บ "15" ของ PO 2569-10 (ย่อ) + บล็อกเดียวจากแท็บ "08" ของ PO 2569-11
TAB15 = [
    ["วันที่ 15/10/2569"], [],
    ["สถานที่รับน้ำมัน", "รถ/พขร", "รายการ", "B7", "B20", "91", "95", "E20"],
    pad(["", "", "จ๊อบเปอร์", "ต้องบุญ", "ต้องบุญ", "ต้องบุญ", "ต้องบุญ", "ต้องบุญ"]),
    pad(["ต้องบุญ", "73", "จำนวนลิตร", "  25,000 ", "", "  5,000 ", "  13,000 ", "", "  -   ", "  43,000 ", "", "", "  ราคาวันที่ 14 โหลดน้ำมันหลังเที่ยงคืน", "", "14/10/69 : ปรับลง บางตัว 16.00 น."]),
    pad(["", "พงษ์ศักดิ์", "ราคา/ลิตร", "39.19", "", "  37.40 ", "37.58", "", "", "", "", "", "", "", "🚩ไออาร์ - iRPC"]),
    pad(["IR ระยอง", "70-2670/70-2671", "จำนวนเงิน", "  979,750 ", "  -   "]),
    pad(["", "", "มุกดาหาร", "", "", "", "", "", "", "", "", "", "", "", "⛽ : D/G91/G95"]),
    pad(["", "", "ยางตลาด", "  25,000 ", "", "  5,000 ", "  13,000 ", "", "", "", "", "", "ค่าขนส่ง 0.80", "", "ระยอง : 39.23/37.44/37.62"]),
    pad(["", "", "กระนวน"]), pad(["", "", "วานรนิวาส"]), [],
    ["สถานที่รับน้ำมัน", "รถ/พขร", "รายการ", "B7"],
    pad(["", "", "จ๊อบเปอร์", "ต้องบุญ", "ต้องบุญ", "ต้องบุญ", "ต้องบุญ", "ต้องบุญ"]),
    pad(["ต้องบุญ", "74", "จำนวนลิตร", "  18,000 ", "  4,000 ", "", "  14,000 ", "", "  -   ", "  44,000 ", "", "", "", "", "15/10/69 : ปรับลง  16.30 น. เฉพาะสระบุรี"]),
    pad(["", "ธีรวัฒน์", "ราคา/ลิตร", "39.81", "  34.80 ", "", "38.39", "", "", "", "", "", "", "", "⭐สตาร์ ฟูเอลส์  (SFL)"]),
    pad(["ดาว สระบุรี", "70-2729/70-1874", "จำนวนเงิน"]),
    pad(["", "", "มุกดาหาร", "", "", "", "", "", "", "", "", "", "", "", "🔔สระบุรี :"]),
    pad(["", "", "ยางตลาด", "  18,000 ", "  4,000 ", "", "  14,000 ", "", "", "", "", "", "ค่าขนส่ง 0.63", "", "D/ULG/G91/G95/E20/B20"]),
    pad(["", "", "กระนวน", "", "", "", "", "", "", "", "", "", "", "", "39.87/48.24/38.23/38.45/32.82/34.86"]),
    pad(["", "", "วานรนิวาส"]), [], [], pad(["", "", "", "40.94"]),
]


class Num(unittest.TestCase):
    def test_num(self):
        self.assertEqual(pp.num("  17,000 "), 17000.0)
        self.assertIsNone(pp.num("  -   "))
        self.assertIsNone(pp.num(""))
        self.assertEqual(pp.num(37.4), 37.4)


class Tab(unittest.TestCase):
    def setUp(self):
        self.rows = pp.parse_tab(TAB15, "2569-10", "15")

    def test_date_and_counts(self):
        self.assertTrue(all(r["date"] == date(2026, 10, 15) for r in self.rows))
        self.assertEqual(len(self.rows), 3 + 3)                 # บล็อกแรก B7/G91/G95 ; บล็อกสอง B7/B20/G95 ; ตัวเลขเศษ 40.94 ท้ายแท็บไม่นับ
        self.assertEqual(sum(r["liters"] for r in self.rows), 43000 + 36000)

    def test_ir_block(self):
        b7 = next(r for r in self.rows if r["depot"] == "IR ระยอง" and r["fuel"] == "B7")
        self.assertEqual((b7["liters"], b7["price"], b7["list_price"], b7["dest"], b7["transport"]), (25000.0, 39.19, 39.23, "ยางตลาด", 0.80))
        self.assertIn("โหลดน้ำมันหลังเที่ยงคืน", b7["price_note"])
        self.assertIn("ไออาร์", b7["provider"])
        g91 = next(r for r in self.rows if r["depot"] == "IR ระยอง" and r["fuel"] == "G91")
        self.assertEqual((g91["price"], g91["list_price"]), (37.40, 37.44))      # ราคาซื้อ = ราคาประกาศ − 0.04 (IRPC)

    def test_dow_block(self):
        b20 = next(r for r in self.rows if r["depot"] == "ดาว สระบุรี" and r["fuel"] == "B20")
        self.assertEqual((b20["liters"], b20["price"], b20["list_price"], b20["transport"]), (4000.0, 34.80, 34.86, 0.63))
        g95 = next(r for r in self.rows if r["depot"] == "ดาว สระบุรี" and r["fuel"] == "G95")
        self.assertAlmostEqual(g95["list_price"] - g95["price"], 0.06)             # ดาว/ต้องบุญ ลด 0.06

    def test_date_fallback(self):
        self.assertEqual(pp.tab_date([[]], "2569-11", "05ไม่มีซื้อ"), date(2026, 11, 5))
        self.assertEqual(pp.parse_tab([["ฟอร์ม"]], "2569-11", "ฟอร์ม"), [])

    def test_no_purchase_day(self):
        rows = [["วันที่ 05/11/2569"], pad(["", "", "จ๊อบเปอร์"]), pad(["", "", "จำนวนลิตร", "  -   ", "  -   "])]
        self.assertEqual(pp.parse_tab(rows, "2569-11", "05ไม่มีซื้อ"), [])


if __name__ == "__main__":
    unittest.main()
