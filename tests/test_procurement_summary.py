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
        df = pd.DataFrame([card("2026-11-11 16:18", "2026-11-11", cls="ลง", filled_at="2026-11-11 16:18"), card("2026-11-11 21:30", "2026-11-12")])
        c = ps.prep_cards(df)
        self.assertEqual(list(c.valid_forward), [False, True])
        self.assertEqual(ps.latest_card(c, "B7")["target_day"], "2026-11-12")

    def test_live_stats_exclude_hindcast(self):
        df = pd.DataFrame([card("2026-11-11 16:18", "2026-11-11", cls="ลง", filled_at="2026-11-11 16:18"),
                           card("2026-11-11 21:30", "2026-11-12", cls="ลง", filled_at="2026-11-13 07:00"),
                           card("2026-11-12 21:30", "2026-11-13", cls="นิ่ง", filled_at="2026-11-14 07:00")])
        s = ps.card_live_stats(ps.prep_cards(df), "B7")
        self.assertEqual((s["n_cards"], s["n_called"], s["n_right"], s["n_hind"]), (2, 2, 1, 1))

    def test_same_day_morning_card_is_forward_when_filled_later(self):
        df = pd.DataFrame([card("2026-11-12 11:00", "2026-11-12", cls="ลง", filled_at="2026-11-12 21:40")])
        self.assertTrue(ps.prep_cards(df).valid_forward.iloc[0])

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


class Reasons(unittest.TestCase):
    def test_classify(self):
        self.assertEqual(ps.classify_reason("2 ราย ประกาศ 'มีปรับขึ้น' วันนี้ → เสี่ยงราคาขึ้น"), "up")
        self.assertEqual(ps.classify_reason("MOPS ปิดล่าสุด -5.00 USD/bbl → โน้มราคาลง"), "down")
        self.assertEqual(ps.classify_reason("ค่าการตลาดเทียบค่ากลาง +0.06 (EPPO 11/11)"), "note")

    def test_card_reason_items(self):
        s = ("MOPS ดีเซล +1.67 (Δ1D) -10.89 (Δ3D) ณ 11/11 · WTI Δ1D -1.44 · ประกาศกองทุนดีเซลมีผลพรุ่งนี้ +0.37 บาท/ลิตร (ลดอุดหนุน → ดันต้นทุนขึ้น)"
             " · ค่าการตลาดเทียบค่ากลาง +0.06 (EPPO 11/11) · หมายเหตุ: MOPS ซ้ำ")
        kinds = [k for k, _ in ps.card_reason_items(s)]
        self.assertEqual(kinds, ["down", "down", "up", "note", "note"])

    def test_small_moves_are_notes(self):
        k = [k for k, _ in ps.card_reason_items("MOPS G95 +0.10 (Δ1D) +0.20 (Δ3D) ณ 11/11 · WTI Δ1D -0.20")]
        self.assertEqual(k, ["note", "note"])

    def test_collect_dedup_and_sources(self):
        d = {"horizons": [{"hours": 24, "tier": "DOWN", "grade": "กลาง", "reasons": ["MOPS ปิดล่าสุด -5.00 USD/bbl → โน้มราคาลง"]}]}
        items = ps.collect_reasons(d, {"reasons": "WTI Δ1D -1.44 · ค่าการตลาดเทียบค่ากลาง +0.06 (EPPO 11/11)"})
        self.assertEqual([(k, s) for k, _, s in items], [("down", "ตลาด"), ("down", "การ์ด"), ("note", "การ์ด")])

    def test_same_fund_notice_from_both_sources_counts_once(self):
        d = {"horizons": [{"hours": 24, "tier": "UP", "grade": "กลาง", "reasons": ["กองทุนประกาศปรับ +0.37 บาท/ลิตร มีผลพรุ่งนี้ → เสี่ยงราคาขึ้น"]}]}
        items = ps.collect_reasons(d, {"reasons": "ประกาศกองทุนดีเซลมีผลพรุ่งนี้ +0.37 บาท/ลิตร (ลดอุดหนุน → ดันต้นทุนขึ้น) · WTI Δ1D -1.44"})
        self.assertEqual([(k, s) for k, _, s in items], [("up", "ตลาด + การ์ด"), ("down", "การ์ด")])

    def test_opposite_mops_windows_are_kept_separate(self):
        d = {"horizons": [{"hours": 24, "tier": "UP", "grade": "กลาง", "reasons": ["MOPS ปิดล่าสุด +1.67 USD/bbl → เสี่ยงราคาขึ้น"]}]}
        items = ps.collect_reasons(d, {"reasons": "MOPS ดีเซล +1.67 (Δ1D) -10.89 (Δ3D) ณ 11/11"})
        self.assertEqual([k for k, _, _ in items], ["up", "down"])

    def test_page_shows_reasons_and_main_reason(self):
        d = {"horizons": [{"hours": 24, "tier": "DOWN", "label": "x", "grade": "กลาง", "until": pd.Timestamp("2026-11-12 20:30"),
                           "reasons": ["MOPS ปิดล่าสุด -5.00 USD/bbl → โน้มราคาลง"]}]}
        card = {"call": "ลง", "confidence": "กลาง", "lo": -1, "hi": -0.5, "p_down": .75, "p_flat": .1, "p_up": .15, "timing": "", "recommendation": "ชะลอการสั่งซื้อ",
                "reasons": "WTI Δ1D -1.44", "target_dt": pd.Timestamp("2026-11-12"), "asof_dt": pd.Timestamp("2026-11-11 21:30")}
        html = ps._forecast_card("ดีเซล", d, card)
        self.assertIn("ทำไมถึงพยากรณ์แบบนี้", html)
        self.assertIn("ชี้ขึ้น 0 ข้อ · ชี้ลง 2 ข้อ", html)
        self.assertIn("MOPS ปิดล่าสุด", html)
        self.assertIn("เหตุผลหลัก", ps._main_reasons(ps.collect_reasons(d, card), "ลง"))
        self.assertEqual(ps._main_reasons([], "ลง"), "")
        self.assertIn("ชี้ขึ้น:", ps._main_reasons([("up", "A", "ตลาด"), ("down", "B", "การ์ด")], None))

    def test_no_reasons_message(self):
        self.assertIn("ไม่มีปัจจัยที่เด่นพอจะชี้ทิศ ระบบจึงไม่ฟันธง", ps._why_block([]))


class MarketEstimate(unittest.TestCase):
    def h(self, tier, avg=0.22, n=113, p=None):
        return {"hours": 24, "tier": tier, "grade": "กลาง", "label": "x", "until": pd.Timestamp("2026-11-12 20:30"),
                "evidence": None if avg is None else {"avg_move": avg, "n": n}, "p": p or {"r10": 0.2, "f10": 0.62, "r40": 0.05, "f40": 0.1}}

    def test_down_and_up_signs_and_band(self):
        d = ps.expected_change(self.h("DOWN"))
        self.assertEqual((d["est"], d["lo"], d["hi"], d["prob"]), (-0.22, -0.10, -0.40, 0.62))
        u = ps.expected_change(self.h("UP", 0.30))
        self.assertEqual((u["est"], u["lo"], u["hi"], u["prob"]), (0.30, 0.10, 0.40, 0.2))

    def test_strong_has_open_band_and_unclear_none(self):
        s = ps.expected_change(self.h("DOWN_STRONG", 0.41))
        self.assertEqual((s["lo"], s["hi"], s["strong"]), (-0.40, None, True))
        self.assertIsNone(ps.expected_change(self.h("UNCLEAR")))

    def test_line_text(self):
        line = ps._est_line(self.h("DOWN"))
        for t in ("-0.22", "ช่วง -0.10 ถึง -0.40", "โอกาสตามโมเดล 62%", "113 ครั้ง"):
            self.assertIn(t, line)
        self.assertIn("ตั้งแต่ -0.40 ลงไป", ps._est_line(self.h("DOWN_STRONG", 0.41)))
        self.assertEqual(ps._est_line(self.h("UNCLEAR")), "")
        self.assertIn("ช่วง", ps._est_line(self.h("DOWN", avg=None)))      # ไม่มีค่าเฉลี่ย → ยังแสดงช่วงและโอกาส

    def test_card_shows_estimate_for_market(self):
        d = {"horizons": [self.h("DOWN")]}
        html = ps._forecast_card("ดีเซล", d, None)
        self.assertIn("ต้นทุนเปลี่ยนประมาณ", html)


class RetailSection(unittest.TestCase):
    def raw(self, **kw):
        r = {"card_id": "2026-11-12 21:30 B7", "asof": "2026-11-11 21:30", "fuel": "B7", "target_day": "2026-11-12", "call": "ลง",
             "p_down": 0.42, "p_flat": 0.45, "p_up": 0.13, "size_text": "ถ้าปรับ มัธยฐาน 0.80 บาท/ลิตร (ช่วงที่พบบ่อย 0.75–1.50)", "retail_base": 40.79,
             "outcome_cls": "", "filled_at": ""}
        r.update(kw)
        return pd.DataFrame([r])

    def test_block_shows_probs_size_and_experimental(self):
        r = ps.latest_retail(ps.prep_retail(self.raw()), "B7")
        html = ps._retail_block(r, {"retail_now": "ปตท. 40.79 บาท/ลิตร (ตรวจล่าสุด 11/11 23:28)"})
        for t in ("ปลีกน่าจะลง", "ทดลอง", "มัธยฐาน 0.80", "40.79", "ลง 42%", "ยังไม่ผ่านการทดสอบเดินหน้า"):
            self.assertIn(t, html)

    def test_block_without_data(self):
        self.assertIn("ยังไม่มีพยากรณ์ปลีก", ps._retail_block(None, None))
        self.assertIsNone(ps.latest_retail(ps.prep_retail(pd.DataFrame()), "B7"))

    def test_live_stats(self):
        df = pd.concat([self.raw(), self.raw(card_id="b", call="นิ่ง", outcome_cls="นิ่ง", filled_at="2026-11-13 07:00"),
                        self.raw(card_id="c", call="ลง", outcome_cls="นิ่ง", filled_at="2026-11-14 07:00"),
                        self.raw(card_id="d", call="ไม่ชัด", outcome_cls="ลง", filled_at="2026-11-15 07:00")])
        s = ps.retail_live_stats(ps.prep_retail(df), "B7")
        self.assertEqual((s["n"], s["n_called"], s["n_right"]), (3, 2, 1))

    def test_page_includes_retail_and_renamed_labels(self):
        html = ps.build_summary({}, {}, pd.DataFrame(), pd.DataFrame(), pd.Timestamp("2026-11-12 08:00"), __import__("engine"), None, self.raw())
        for t in ("ราคาขายปลีก", "ราคาขายส่ง EPPO", "พยากรณ์ปลีก (ทดลอง)", "เก็บไว้เทียบความแม่นกับตลาด"):
            self.assertIn(t, html)


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
        for t in ("1. ผลพยากรณ์", "2. คำแนะนำ", "3. เทียบกับราคาจริงตอนนี้", "4. Backtest", "5. ความแม่นยำจริง", "ชะลอการสั่งซื้อ", "645 วัน", "60%"):
            self.assertIn(t, html)

    def test_renders_with_nothing(self):
        html = ps.build_summary({}, {}, pd.DataFrame(), pd.DataFrame(), pd.Timestamp("2026-11-11 22:00"), engine)
        self.assertIn("ยังไม่มีผลทดสอบย้อนหลัง", html)
        self.assertIn("ยังไม่มีการ์ด", html)


if __name__ == "__main__":
    unittest.main()
