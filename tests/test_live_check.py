import unittest

import pandas as pd

import live_check as L
import procurement_summary as ps


def feed(rows):
    return pd.DataFrame([{"provider": p, "warehouse": w, "fuel": f, "price": pr, "effective": e} for p, w, f, pr, e in rows])


FEED = feed([("A", "x", "B7", 35.00, "2026-11-10 20:00"), ("B", "y", "B7", 35.20, "2026-11-10 20:00"),
             ("A", "x", "B7", 34.60, "2026-11-11 09:00"), ("A", "x", "B7", 34.55, "2026-11-11 09:30")])
NOW = pd.Timestamp("2026-11-11 12:00")
ROW = {"fuel": "B7", "hours": 24, "tier": "DOWN", "ref": pd.Timestamp("2026-11-10 20:30"), "end": pd.Timestamp("2026-11-11 20:30"), "grade": "กลาง"}


class Check(unittest.TestCase):
    def setUp(self):
        self.book = L.make_book(FEED)

    def test_make_book_bad_input(self):
        self.assertIsNone(L.make_book(pd.DataFrame()))
        self.assertIsNone(L.make_book(pd.DataFrame({"x": [1]})))

    def test_down_forecast_and_price_fell(self):
        c = L.check_one(self.book, ROW, NOW)
        self.assertEqual((c["status"], c["delta"], c["direction"]), ("right", -0.45, "ลง"))
        self.assertAlmostEqual(c["left_h"], 8.5)

    def test_wrong_wait_none(self):
        up = {**ROW, "tier": "UP"}
        self.assertEqual(L.check_one(self.book, up, NOW)["status"], "wrong")
        small = L.check_one(self.book, ROW, pd.Timestamp("2026-11-10 21:00"))
        self.assertEqual(small["status"], "wait")
        self.assertEqual(L.check_one(self.book, {**ROW, "tier": "UNCLEAR"}, NOW)["status"], "none")

    def test_moves_and_timing(self):
        ev = L.moves_today(self.book, "B7", pd.Timestamp("2026-11-11 00:00"), NOW)
        self.assertEqual(ev, [(pd.Timestamp("2026-11-11 09:00"), -0.4)])
        self.assertEqual(L.timing_verdict(ev, "ลง", ("09:00", "15:59"))["status"], "right")
        self.assertEqual(L.timing_verdict(ev, "ลง", ("12:00", "15:59"))["status"], "wrong")
        self.assertEqual(L.timing_verdict(ev, "ขึ้น", ("09:00", "15:59"))["status"], "wait")
        self.assertEqual(L.timing_verdict(ev, "ไม่ชัด", ("09:00", "15:59"))["status"], "none")

    def test_top_slot(self):
        self.assertEqual(L.top_slot("09:00–15:59 (80%) / 07:00–08:59 (17%)"), ("09:00", "15:59"))
        self.assertIsNone(L.top_slot(""))


class Active(unittest.TestCase):
    def log(self, **kw):
        base = {"fuel": "B7", "hours": 24, "tier": "DOWN", "ref": "2026-11-10 20:30", "outcome_delta": "", "backfilled": 0, "grade": "กลาง"}
        base.update(kw)
        return pd.DataFrame([base])

    def test_active_only_unmatured_and_unfilled(self):
        self.assertEqual(len(L.active_rows(self.log(), NOW)), 1)
        self.assertEqual(L.active_rows(self.log(outcome_delta="0.2"), NOW), [])
        self.assertEqual(L.active_rows(self.log(), pd.Timestamp("2026-11-12 00:00")), [])
        self.assertEqual(L.active_rows(self.log(backfilled=1), NOW), [])
        self.assertEqual(L.active_rows(pd.DataFrame(), NOW), [])

    def test_latest_ref_per_fuel_hours(self):
        df = pd.concat([self.log(ref="2026-11-10 15:30"), self.log(ref="2026-11-10 20:30")])
        r = L.active_rows(df, NOW)
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0]["ref"], pd.Timestamp("2026-11-10 20:30"))


class Render(unittest.TestCase):
    def test_section_renders(self):
        log = pd.DataFrame([{"fuel": "B7", "hours": 24, "tier": "DOWN_STRONG", "ref": "2026-11-10 20:30", "outcome_delta": "", "backfilled": 0, "grade": "กลาง"}])
        cards = ps.prep_cards(pd.DataFrame([{"card_id": "x", "asof": "2026-11-10 21:30", "target_day": "2026-11-11", "fuel": "B7", "call": "ลง",
                                             "timing": "09:00–15:59 (80%)", "outcome_cls": "", "filled_at": ""}]))
        html = ps.build_live_check(log, cards, FEED, NOW)
        for t in ("3. เทียบกับราคาจริงตอนนี้", "34.55", "-0.45", "ไปตามที่ทายแล้ว", "09:00", "อยู่ในช่วงที่ทาย"):
            self.assertIn(t, html)

    def test_no_feed_and_no_rows(self):
        self.assertIn("ยังอ่านราคาจริง", ps.build_live_check(pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), NOW))
        self.assertIn("ยังไม่มีคำพยากรณ์ที่ยังไม่ครบกำหนด", ps.build_live_check(pd.DataFrame(), pd.DataFrame(), FEED, NOW))


if __name__ == "__main__":
    unittest.main()
