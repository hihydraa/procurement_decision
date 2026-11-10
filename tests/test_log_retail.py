import unittest

import log_retail as lr

HTML = """<html><body><h2 class="text-lg">อัปเดตราคาน้ำมันล่าสุด 10 พฤศจิกายน 2569</h2>
<section id="brand-ptt"><ul>
<li><div class="min-w-0 flex-1"><p class="truncate">ดีเซล B7</p></div><div class="shrink-0 text-right"><p class="text-xl">42.19</p></div></li>
<li><div class="min-w-0 flex-1"><p class="truncate">แก๊สโซฮอล์ 95</p></div><div class="shrink-0 text-right"><p class="text-xl">40.69</p></div></li>
<li><div class="min-w-0 flex-1"><p class="truncate">ผิดรูป</p></div><div class="shrink-0 text-right"><p class="text-xl">-</p></div></li>
<li><div class="min-w-0 flex-1"><p class="truncate">นอกช่วง</p></div><div class="shrink-0 text-right"><p class="text-xl">4.2</p></div></li>
</ul></section>
<section id="brand-shell"><ul><li><div class="min-w-0 flex-1"><p class="truncate">ดีเซล</p></div><div class="shrink-0 text-right"><p class="text-xl">41.00</p></div></li></ul></section>
</body></html>"""


class Parse(unittest.TestCase):
    def test_parse_brand(self):
        items, d = lr.parse_page(HTML, "ptt")
        self.assertEqual(items, [("ดีเซล B7", 42.19), ("แก๊สโซฮอล์ 95", 40.69)])
        self.assertEqual(d, "10 พฤศจิกายน 2569")
        self.assertEqual(lr.parse_page(HTML, "bcp")[0], [])

    def test_changed_structure_empty(self):
        self.assertEqual(lr.parse_page("<html></html>", "ptt"), ([], ""))


class Plan(unittest.TestCase):
    def test_first_run_adds_all(self):
        add, touch = lr.plan([], "ptt", [("ดีเซล B7", 42.19)], "d", "2026-11-10 12:00")
        self.assertEqual(len(add), 1)
        self.assertEqual(touch, [])
        self.assertEqual(len(add[0]), len(lr.COLUMNS))

    def test_unchanged_touches_latest_row_changed_appends(self):
        rows = [["t0", "t1", "ptt", "ดีเซล B7", 41.9, "d", "kapook"], ["t2", "t3", "ptt", "ดีเซล B7", 42.19, "d", "kapook"],
                ["t0", "t1", "ptt", "แก๊สโซฮอล์ 95", 40.69, "d", "kapook"]]
        add, touch = lr.plan(rows, "ptt", [("ดีเซล B7", 42.19), ("แก๊สโซฮอล์ 95", 40.99)], "d", "now")
        self.assertEqual(touch, [3])                 # แถวที่ 2 ของข้อมูล = แถวชีทเลข 3
        self.assertEqual([a[3:5] for a in add], [["แก๊สโซฮอล์ 95", 40.99]])

    def test_price_returns_to_older_value_is_new_row(self):
        rows = [["t0", "t1", "ptt", "ดีเซล B7", 41.9, "d", "k"], ["t2", "t3", "ptt", "ดีเซล B7", 42.19, "d", "k"]]
        add, touch = lr.plan(rows, "ptt", [("ดีเซล B7", 41.9)], "d", "now")
        self.assertEqual((len(add), touch), (1, []))


if __name__ == "__main__":
    unittest.main()
