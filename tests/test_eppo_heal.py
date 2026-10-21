"""ทดสอบการแทนที่แถว EPPO ที่ลากค่าเก่า (CARRIED) ด้วยไฟล์จริง — ใช้ Sheets/HTTP จำลอง ไม่แตะเน็ต"""
import os
import sys
import unittest
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import ingest  # noqa: E402
from common import EXCEL_EPOCH  # noqa: E402

HEAD = ["Date", "oil type", "EX-REFIN.", "EXCISE TAX", "M. TAX", "OIL FUND", "CONSV. FUND", "WHOLESALE (WS)", "VAT (WS)",
        "WS&VAT", "MARKETING MARGIN", "VAT (MM)", "RETAIL", "Avg Consumption (mn L/day)", "Estimated Subsidy (mn baht/day)",
        "Estimated Collection (mn baht/day)", "Net Fund Impact", "Obs Rank", "Source"]


def serial(d):
    return (datetime(d.year, d.month, d.day) - EXCEL_EPOCH.replace(tzinfo=None)).days if EXCEL_EPOCH.tzinfo else (datetime(d.year, d.month, d.day) - EXCEL_EPOCH).days


def row(d, ot, mm, src, rank):
    return [serial(d), ot, 20, 6, 0.6, 1.0, 0.1, 30, 2, 32, mm, 1, 40, 10, 0, 10, 10, rank, src]


class HealTests(unittest.TestCase):
    def setUp(self):
        self.writes, self.appends = [], []
        self.orig = (ingest.sheets_update, ingest.sheets_append, ingest.fetch_eppo_workbook, ingest.parse_eppo_rows, ingest.load_consumption_ref)
        ingest.sheets_update = lambda tok, sid, rng, rows: self.writes.append((rng, rows))
        ingest.sheets_append = lambda tok, sid, rng, rows: self.appends.append(rows)
        ingest.load_consumption_ref = lambda tok: {"A": 10.0, "B": 5.0}
        ingest.parse_eppo_rows = lambda wb, d: [
            {"oil_type": "A", "ex_refin": 21, "excise": 6, "mtax": 0.6, "oilfund": -1.0, "consv": 0.1, "wholesale": 31, "vat_ws": 2, "ws_vat": 33, "mm": 2.5, "vat_mm": 1, "retail": 41},
            {"oil_type": "B", "ex_refin": 22, "excise": 6, "mtax": 0.6, "oilfund": 1.0, "consv": 0.1, "wholesale": 32, "vat_ws": 2, "ws_vat": 34, "mm": 3.5, "vat_mm": 1, "retail": 42}]

    def tearDown(self):
        (ingest.sheets_update, ingest.sheets_append, ingest.fetch_eppo_workbook, ingest.parse_eppo_rows, ingest.load_consumption_ref) = self.orig

    def sheet(self):
        d1, d2 = date(2026, 10, 19), date(2026, 10, 20)
        return [HEAD, row(d1, "A", 1.0, "FRESH", 5), row(d1, "B", 1.0, "FRESH", 5),
                row(d2, "A", 1.0, "CARRIED", 6), row(d2, "B", 1.0, "CARRIED", 6)]

    def test_carried_replaced_when_file_appears(self):
        ingest.fetch_eppo_workbook = lambda d: object() if d == date(2026, 10, 20) else None
        ingest.sheets_get = lambda tok, sid, rng: self.sheet()
        res = ingest.ingest_eppo("t", date(2026, 10, 20))
        self.assertIsNone(res)
        self.assertEqual(len(self.writes), 1)
        rng, rows = self.writes[0]
        self.assertTrue(rng.endswith("!A4:S5"))
        self.assertEqual([r[18] for r in rows], ["FRESH", "FRESH"])
        self.assertEqual([r[10] for r in rows], [2.5, 3.5])
        self.assertEqual([r[17] for r in rows], [6, 6])          # Obs Rank เดิมไม่เปลี่ยน
        self.assertEqual(self.appends, [])

    def test_carried_stays_when_no_file(self):
        ingest.fetch_eppo_workbook = lambda d: None
        ingest.sheets_get = lambda tok, sid, rng: self.sheet()
        ingest.ingest_eppo("t", date(2026, 10, 20))
        self.assertEqual(self.writes, [])
        self.assertEqual(self.appends, [])

    def test_fresh_rows_not_touched(self):
        calls = []
        ingest.fetch_eppo_workbook = lambda d: calls.append(d)
        s = self.sheet()
        for r in s[3:]:
            r[18] = "FRESH"
        ingest.sheets_get = lambda tok, sid, rng: s
        ingest.ingest_eppo("t", date(2026, 10, 20))
        self.assertEqual(calls, [])

    def test_row_count_mismatch_skipped(self):
        ingest.fetch_eppo_workbook = lambda d: object()
        s = self.sheet()[:-1]                                      # แถวคาร์รี่เหลือ 1 แต่ไฟล์มี 2
        ingest.sheets_get = lambda tok, sid, rng: s
        ingest.ingest_eppo("t", date(2026, 10, 20))
        self.assertEqual(self.writes, [])


if __name__ == "__main__":
    unittest.main()
