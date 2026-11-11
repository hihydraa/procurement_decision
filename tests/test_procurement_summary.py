import unittest

import pandas as pd

import engine
import procurement_summary as ps


def card(asof, target, fuel="B7", call="ลง", conf="กลาง", cls="", **kw):
    r = {"card_id": f"{target} 21:30 {fuel}", "asof": asof, "target_day": target, "fuel": fuel, "call": call, "confidence": conf,
         "lo": -1.0, "hi": -0.5, "p_down": 0.75, "p_flat": 0.15, "p_up": 0.10, "timing": "09:00–15:59 (80%)",
         "recommendation": "ชะลอการสั่งซื้อ รอซื้อหลัง 16:00 ข้อควรระวัง: x", "outcome_cls": cls}
    r.update(kw)
    return r


class Cards(unittest.TestCase):
    def test_hindcast_is_not_forward(self):
        df = pd.DataFrame([card("2026-11-11 16:18", "2026-11-11", cls="ลง"), card("2026-11-11 21:30", "2026-11-12")])
        c = ps.prep_cards(df)
        self.assertEqual(list(c.valid_forward), [False, True])
        self.assertEqual(ps.latest_card(c, "B7")["target_day"], "2026-11-12")

    def test_live_stats_exclude_hindcast(self):
        df = pd.DataFrame([card("2026-11-11 16:18", "2026-11-11", cls="ลง"), card("2026-11-11 21:30", "2026-11-12", cls="ลง"),
                           card("2026-11-12 21:30", "2026-11-13", cls="นิ่ง")])
        s = ps.card_live_stats(ps.prep_cards(df), "B7")
        self.assertEqual((s["n_cards"], s["n_called"], s["n_right"], s["n_hind"]), (2, 2, 1, 1))

    def test_empty_and_bad_inputs(self):
        self.assertTrue(ps.prep_cards(pd.DataFrame()).empty)
        self.assertIsNone(ps.latest_card(ps.prep_cards(pd.DataFrame({"x": [1]})), "B7"))
        self.assertEqual(ps.card_live_stats(pd.DataFrame(), "B7")["n_cards"], 0)

    def test_dayfirst_timestamps(self):
        c = ps.prep_cards(pd.DataFrame([card("11/11/2026 21:30:00", "12/11/2026")]))
        self.assertTrue(c.valid_forward.iloc[0])


class Combine(unittest.TestCase):
    def test_agree_down(self):
        r = ps.combine("DOWN", "กลาง", {"call": "ลง", "confidence": "กลาง", "recommendation": "ชะลอการสั่งซื้อ รอซื้อหลัง 16:00"})
        self.assertEqual((r["dir"], r["level"]), ("ลง", "สูง"))
        self.assertIn("รอซื้อหลัง 16:00", r["action"])

    def test_conflict_no_call(self):
        r = ps.combine("UP", "กลาง", {"call": "ลง", "confidence": "กลาง"})
        self.assertIsNone(r["dir"])
        self.assertIn("ขัดกัน", r["action"])

    def test_no_signal(self):
        r = ps.combine("UNCLEAR", None, {"call": "ไม่ชัด"})
        self.assertIsNone(r["dir"])
        self.assertIn("ปกติ", r["action"])

    def test_up_strong_alone(self):
        r = ps.combine("UP_STRONG", "กลาง", None)
        self.assertEqual((r["dir"], r["level"]), ("ขึ้น", "กลาง"))
        self.assertIn("เร่งซื้อ", r["action"])

    def test_card_only_low(self):
        r = ps.combine(None, None, {"call": "ลง", "confidence": "ต่ำ", "recommendation": ""})
        self.assertEqual((r["dir"], r["level"]), ("ลง", "ต่ำ"))


class Horizon48(unittest.TestCase):
    def test_diesel_case_24_unclear_48_up_strong(self):
        r = ps.combine("UNCLEAR", None, None, "UP_STRONG", "ต่ำ")
        self.assertEqual((r["dir"], r["level"]), ("ขึ้น", "ต่ำ"))
        self.assertIn("48 ชม.", r["action"])

    def test_48_opposes_24(self):
        r = ps.combine("DOWN", "กลาง", {"call": "ลง", "confidence": "กลาง", "recommendation": "ชะลอการสั่งซื้อ รอซื้อหลัง 16:00"}, "UP", "กลาง")
        self.assertEqual((r["dir"], r["level"]), ("ลง", "กลาง"))
        self.assertIn("อย่ารอเกิน 24 ชม.", r["action"])
        self.assertIn("48 ชม. ชี้ขึ้น", r["why"])

    def test_48_agrees_or_missing_changes_nothing(self):
        a = ps.combine("DOWN", "กลาง", None)
        self.assertEqual(a, ps.combine("DOWN", "กลาง", None, "DOWN", "กลาง"))
        self.assertEqual(a, ps.combine("DOWN", "กลาง", None, None, None))

    def test_conflict_not_overridden_by_48(self):
        r = ps.combine("UP", "กลาง", {"call": "ลง", "confidence": "กลาง"}, "UP", "กลาง")
        self.assertIsNone(r["dir"])


class Render(unittest.TestCase):
    def outlook(self):
        h = lambda hrs, tier: {"hours": hrs, "tier": tier, "label": "x", "grade": "กลาง", "until": pd.Timestamp("2026-11-12 20:30")}
        return {"DIESEL": {"source": "N", "horizons": [h(24, "DOWN"), h(48, "UNCLEAR")]}, "GASOHOL95": {"source": "N", "horizons": []}}

    def summary(self):
        t = {"n": 100, "share": 0.1, "right10": 0.6, "wrong10": 0.1, "avg_move": 0.22}
        return {"generated": "2026-10-21", "outlook_results": {"B7_H24": {"n_oos_days": 645, "tiers": {"DOWN": t, "UNCLEAR": {"n": 300, "share": 0.5}}}}}

    def test_full_page_renders(self):
        cards = pd.DataFrame([card("2026-11-11 21:30", "2026-11-12")])
        html = ps.build_summary(self.outlook(), self.summary(), pd.DataFrame(), cards, pd.Timestamp("2026-11-11 22:00"), engine)
        for t in ("1. ผลพยากรณ์", "2. คำแนะนำ", "3. Backtest", "4. ความแม่นยำจริง", "ชะลอการสั่งซื้อ", "645 วัน", "60%"):
            self.assertIn(t, html)

    def test_renders_with_nothing(self):
        html = ps.build_summary({}, {}, pd.DataFrame(), pd.DataFrame(), pd.Timestamp("2026-11-11 22:00"), engine)
        self.assertIn("ยังไม่มีผลทดสอบย้อนหลัง", html)
        self.assertIn("ยังไม่มีการ์ด", html)


if __name__ == "__main__":
    unittest.main()
