import unittest

import pandas as pd

import diag_pricelog_gaps as G

HEAD = ["timestamp_logged", "message_id", "update_date", "update_time"]


def v(rows):
    return [HEAD] + [list(r) for r in rows]


class Messages(unittest.TestCase):
    def test_dedupe_and_parse(self):
        m = G.messages(v([("2026-11-09 10:05:00", "a", "2026-11-09", "10:00"), ("2026-11-09 10:05:00", "a", "2026-11-09", "10:00"),
                          ("2026-11-09 11:00:00", "b", "2026-11-09", "10:50"), ("2026-11-09 11:00:00", "", "2026-11-09", "10:50")]))
        self.assertEqual(list(m.msg), ["a", "b"])
        self.assertEqual(m.loc[0, "posted"], pd.Timestamp("2026-11-09 10:00"))


class Gaps(unittest.TestCase):
    def test_quiet_gap_vs_backlog(self):
        # 08:00 โพสต์ ปกติ ; ช่องว่างถึง 14:00 โดยไม่มีล็อกชดเชย ; 14:00 ถึง 20:30 ช่องว่างที่ข้อความ 17:00 ถูกล็อกพร้อม 20:30
        m = G.messages(v([("2026-11-09 08:01:00", "a", "2026-11-09", "08:00"), ("2026-11-09 14:01:00", "b", "2026-11-09", "14:00"),
                          ("2026-11-09 20:30:00", "c", "2026-11-09", "17:00"), ("2026-11-09 20:31:00", "d", "2026-11-09", "20:30")]))
        g = G.gaps(m, pd.Timestamp("2026-11-01"))
        self.assertEqual(len(g), 2)
        quiet, back = g.iloc[0], g.iloc[1]
        self.assertFalse(quiet.backlog)
        self.assertEqual(quiet.hours, 6.0)
        self.assertTrue(back.backlog)                             # ข้อความ 17:00 ถูกล็อกตอน 20:30 = ชดเชย
        self.assertTrue(quiet.day)

    def test_overnight_not_daytime(self):
        m = G.messages(v([("2026-11-09 21:01:00", "a", "2026-11-09", "21:00"), ("2026-11-10 07:01:00", "b", "2026-11-10", "07:00")]))
        g = G.gaps(m, pd.Timestamp("2026-11-01"))
        self.assertEqual(len(g), 1)
        self.assertFalse(g.iloc[0].day)

    def test_reports(self):
        m = G.messages(v([("2026-11-09 08:01:00", "a", "2026-11-09", "08:00"), ("2026-11-09 14:01:00", "b", "2026-11-09", "14:00")]))
        out = G.gap_report(m, pd.Timestamp("2026-11-10 08:00"))
        self.assertIn("ข้อความล่าสุดเก่า 18.0 ชม.", out[0])
        self.assertIn("1 ครั้ง", out[1])
        self.assertIn("ล็อกหลังโพสต์", G.lag_report(m)[0])


if __name__ == "__main__":
    unittest.main()
