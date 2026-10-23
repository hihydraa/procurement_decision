import unittest

import pandas as pd

import main as MAIN
import signals as S


def _tables(rows):
    return S.Tables(intraday=pd.DataFrame(rows, columns=["msg_date", "msg_time", "kind", "b", "d", "wti", "wti_chg", "brent", "brent_chg"]))


class TrendCardTest(unittest.TestCase):
    def test_today_strong_diesel(self):
        t = _tables([["2026-10-23", "11:30", "TREND", 0.4, -2.5, None, None, None, None]])
        snap = MAIN.trend_snapshot(t, "B7", pd.Timestamp("2026-10-23 13:00"))
        self.assertEqual(snap["value"], -2.5)
        self.assertTrue(snap["today"])
        self.assertEqual(MAIN.classify_trend_tone(snap)["tone"], "success")
        html = MAIN.trend_card_html("Trend Mops ดีเซล", snap)
        self.assertIn("-2.50", html)
        self.assertIn("กดราคาลง", html)
        self.assertIn("11:30", html)

    def test_benzene_uses_b_column_and_rise(self):
        t = _tables([["2026-10-23", "15:00", "TREND", 1.8, -0.2, None, None, None, None]])
        snap = MAIN.trend_snapshot(t, "G95", pd.Timestamp("2026-10-23 16:00"))
        self.assertEqual(snap["value"], 1.8)
        self.assertEqual(MAIN.classify_trend_tone(snap)["label"], "ดันราคาขึ้น")
        self.assertIn("+1.80", MAIN.trend_card_html("x", snap))

    def test_yesterday_is_flagged_not_today(self):
        t = _tables([["2026-10-22", "15:00", "TREND", 1.8, -3.0, None, None, None, None]])
        snap = MAIN.trend_snapshot(t, "B7", pd.Timestamp("2026-10-23 08:00"))
        self.assertFalse(snap["today"])
        self.assertEqual(MAIN.classify_trend_tone(snap)["label"], "ยังไม่มีของวันนี้")
        self.assertIn("ของวันก่อน", MAIN.trend_card_html("x", snap))

    def test_ignores_future_rows_and_empty(self):
        t = _tables([["2026-10-23", "15:00", "TREND", 1.0, 1.0, None, None, None, None]])
        self.assertIsNone(MAIN.trend_snapshot(t, "B7", pd.Timestamp("2026-10-23 10:00")))      # ยังไม่โพสต์ ณ เวลานั้น
        self.assertIsNone(MAIN.trend_snapshot(S.Tables(), "B7", pd.Timestamp("2026-10-23 10:00")))
        self.assertIn("ยังไม่เคยมี", MAIN.trend_card_html("x", None))


if __name__ == "__main__":
    unittest.main()


class OutlookMiniTest(unittest.TestCase):
    def test_mini_panel(self):
        import engine
        ref = pd.Timestamp("2026-10-22 20:30")
        mk = lambda tier, grade, hz: {"hours": hz, "tier": tier, "label": __import__("model").OUTLOOK_TIERS[tier], "grade": grade}
        outlook = {"DIESEL": {"fuel": "B7", "ref": ref, "horizons": [mk("DOWN_STRONG", "กลาง", 24), mk("DOWN_STRONG", "ต่ำ", 48)]},
                   "GASOHOL95": {"fuel": "G95", "ref": ref, "horizons": [mk("UNCLEAR", "ต่ำ", 24), mk("UNCLEAR", "ต่ำ", 48)]}}
        html = MAIN.build_outlook_mini(outlook)
        self.assertIn("ลงแรง", html)
        self.assertIn("🟡", html)
        self.assertIn("ยังไม่ชัดเจน", html)
        self.assertEqual(MAIN.build_outlook_mini({}), "")
        self.assertEqual(MAIN.build_outlook_mini({"DIESEL": {"fuel": "B7", "ref": ref, "horizons": []}}), "")
