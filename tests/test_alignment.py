import unittest

import pandas as pd

import engine
import main as MAIN
import signals as S


def _outlook(tier):
    return {"horizons": [{"hours": 24, "tier": tier}]}


class LegacyGateTest(unittest.TestCase):
    PARAMS = {"models": {"B7_PM": {}, "B7_EVE": {}}}

    def test_before_six_no_legacy_action(self):
        d = engine.decide("DIESEL", pd.Timestamp("2026-10-26 00:43"), S.Tables(), self.PARAMS)
        self.assertEqual(d["action_code"], "NO_DATA")
        self.assertIn("06:00", d["summary"])

    def test_after_six_gate_not_applied(self):
        d = engine.decide("DIESEL", pd.Timestamp("2026-10-26 08:00"), S.Tables(), self.PARAMS)
        self.assertEqual(d["action_code"], "NO_DATA")
        self.assertNotIn("06:00", d["summary"])             # ผ่านประตูเวลาแล้ว ไปตกที่ตรวจฟีด (ไม่มีข้อมูล)
        self.assertIn("ฟีด", d["summary"])


class AlignmentTest(unittest.TestCase):
    def test_opposite_direction_is_conflict(self):
        self.assertEqual(MAIN.legacy_alignment("URGENT", _outlook("DOWN_STRONG"))[0], "conflict")
        self.assertEqual(MAIN.legacy_alignment("GRADUAL", _outlook("DOWN"))[0], "conflict")
        self.assertEqual(MAIN.legacy_alignment("WAIT", _outlook("UP"))[0], "conflict")

    def test_unclear_outlook_does_not_confirm(self):
        lvl, txt = MAIN.legacy_alignment("GRADUAL", _outlook("UNCLEAR"))
        self.assertEqual(lvl, "unconfirmed")
        self.assertIn("ยังไม่ยืนยัน", txt)
        self.assertEqual(MAIN.legacy_alignment("WAIT", _outlook("UNCLEAR"))[0], "unconfirmed")

    def test_same_direction_is_ok(self):
        self.assertEqual(MAIN.legacy_alignment("URGENT", _outlook("UP"))[0], "ok")
        self.assertEqual(MAIN.legacy_alignment("GRADUAL", _outlook("UP_STRONG"))[0], "ok")
        self.assertEqual(MAIN.legacy_alignment("WAIT", _outlook("DOWN"))[0], "ok")

    def test_neutral_missing_or_no_data_have_no_tag(self):
        for code in ("NORMAL", "NO_DATA", None):
            self.assertEqual(MAIN.legacy_alignment(code, _outlook("UP")), ("none", ""))
        self.assertEqual(MAIN.legacy_alignment("URGENT", None), ("none", ""))
        self.assertEqual(MAIN.legacy_alignment("URGENT", {"horizons": []}), ("none", ""))


if __name__ == "__main__":
    unittest.main()
