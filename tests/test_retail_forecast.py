import unittest

import numpy as np
import pandas as pd

import retail_forecast as R


def eppo(ws, retail, start="2026-01-01", fuel="B7"):
    days = pd.date_range(start, periods=len(ws))
    return pd.DataFrame({"day": days, "fuel": fuel, "ws": ws, "mm": 2.0, "retail": retail, "exref": 0.0, "fund": 0.0})


def synthetic(n=200, seed=1):
    """ปลีกตาม WS ในวันเดียวกัน 50% ของครั้งที่ WS ขยับ ; ไม่ปรับวันอาทิตย์"""
    rng = np.random.default_rng(seed)
    ws, retail = [36.0], [40.0]
    for i in range(1, n):
        day = pd.Timestamp("2026-01-01") + pd.Timedelta(days=i)
        dws = rng.choice([-0.5, 0.0, 0.5], p=[0.25, 0.5, 0.25])
        ws.append(ws[-1] + dws)
        move = dws != 0 and rng.random() < 0.5 and day.dayofweek != 6
        retail.append(retail[-1] + (dws * 1.5 if move else 0.0))
    return eppo(ws, retail)


class Basics(unittest.TestCase):
    def test_classify(self):
        self.assertEqual([R.classify(x) for x in (-0.5, -0.1, 0.05, 0.1, 0.8)], ["ลง", "ลง", "นิ่ง", "ขึ้น", "ขึ้น"])

    def test_history_only_consecutive_days(self):
        e = eppo([36, 35, 35, 34], [40, 40, 39, 39])
        e = e.drop(index=[2])                                     # ขาดวันที่ 3
        h = R.history(e, "B7")
        self.assertEqual(len(h), 1)                               # เหลือคู่ (1 ม.ค.→2 ม.ค.) เท่านั้น
        self.assertEqual(h.iloc[0].s, "ลง")

    def test_transition_sums_to_one_and_learns_passthrough(self):
        h = R.history(synthetic(), "B7")
        t = R.transition(h)
        for s in R.CLASSES:
            self.assertAlmostEqual(sum(t[s][r] for r in R.CLASSES), 1.0)
        self.assertGreater(t["ลง"]["ลง"], t["นิ่ง"]["ลง"] * 3)    # WS ลง → ปลีกลงบ่อยกว่าตอน WS นิ่งมาก

    def test_dow_scale_detects_no_sunday_moves(self):
        h = R.history(synthetic(), "B7")
        scale_sun, n = R.dow_scale(h, 6)
        scale_wed, _ = R.dow_scale(h, 2)
        self.assertLess(scale_sun, 0.3)
        self.assertGreater(scale_wed, 0.9)
        self.assertEqual(R.dow_scale(h.head(5), 6)[0], 1.0)        # ข้อมูลน้อย = ไม่ปรับ

    def test_probs_sum_to_one(self):
        t = R.transition(R.history(synthetic(), "B7"))
        p = R.retail_probs({"ลง": 0.75, "นิ่ง": 0.10, "ขึ้น": 0.15}, t, 1.0)
        self.assertAlmostEqual(sum(p.values()), 1.0)
        p0 = R.retail_probs({"ลง": 0.75, "นิ่ง": 0.10, "ขึ้น": 0.15}, t, 0.1)
        self.assertGreater(p0["นิ่ง"], p["นิ่ง"])

    def test_decide(self):
        self.assertEqual(R.decide({"ลง": 0.1, "นิ่ง": 0.8, "ขึ้น": 0.1}), "นิ่ง")
        self.assertEqual(R.decide({"ลง": 0.5, "นิ่ง": 0.3, "ขึ้น": 0.2}), "ลง")
        self.assertEqual(R.decide({"ลง": 0.3, "นิ่ง": 0.4, "ขึ้น": 0.3}), "ไม่ชัด")


class Forecast(unittest.TestCase):
    def test_forecast_and_outcome(self):
        e = synthetic()
        f = R.forecast(e, "B7", {"ลง": 0.7, "นิ่ง": 0.1, "ขึ้น": 0.2}, pd.Timestamp("2026-08-05"), 40.79)
        self.assertIsNotNone(f)
        self.assertAlmostEqual(f["p_down"] + f["p_flat"] + f["p_up"], 1.0)
        self.assertEqual(f["retail_base"], 40.79)
        self.assertGreater(f["p_down"], f["p_up"])

    def test_not_enough_history(self):
        e = eppo([36.0, 35.5, 35.5], [40.0, 40.0, 39.0])
        self.assertIsNone(R.forecast(e, "B7", {"ลง": 0.3, "นิ่ง": 0.4, "ขึ้น": 0.3}, pd.Timestamp("2026-01-10"), 40.0))

    def test_outcome(self):
        e = eppo([36, 35, 35], [40.0, 39.2, 39.2])
        self.assertEqual(R.outcome(e, "B7", "2026-01-02"), (-0.8, "ลง"))
        self.assertEqual(R.outcome(e, "B7", "2026-01-03"), (0.0, "นิ่ง"))
        self.assertIsNone(R.outcome(e, "B7", "2026-01-09"))


if __name__ == "__main__":
    unittest.main()


class SignalFeatureTest(unittest.TestCase):
    """ฟีเจอร์สัญญาณ Jobber ของการทดลองปลีก (exp_retail_search.py) : ตัดค่าสุดโต่ง ค่าว่าง = 0 และไม่ใช้ข้อความหลัง 20:30"""

    def test_vector_clips_and_fills(self):
        import numpy as np
        import retail_forecast as RF
        v = RF.signal_vector({"up_n": 9, "dn_n": None, "close_last": float("nan"), "fund_next": -5})
        np.testing.assert_array_equal(v, [4, 0, 0, -3])

    def test_features_cut_at_2030(self):
        import pandas as pd
        import retail_forecast as RF
        import signals as S
        hu = pd.DataFrame([["2026-10-01", "17:00", "A", "UP", "ALL"], ["2026-10-01", "21:00", "B", "UP", "ALL"]],
                          columns=["msg_date", "msg_time", "provider", "direction", "scope"])
        f = RF.signal_features(S.Tables(headsup=hu), "B7", pd.Timestamp("2026-10-01"))
        self.assertEqual(f["up_n"], 1)
        self.assertEqual(set(f), set(RF.SIGNAL_FEATURES))


class SignalShadowTest(unittest.TestCase):
    """โมเดลสัญญาณ Jobber บันทึกคู่ขนาน : ต้องไม่ทำให้แถวหลักเสีย และเว้นว่างพร้อมเหตุผลเมื่อใช้ไม่ได้"""

    def _tables(self, day="2026-11-11"):
        import pandas as pd
        import signals as S
        hu = pd.DataFrame([[day, "17:00", "A", "UP", "ALL"], [day, "17:30", "B", "UP", "ALL"]], columns=["msg_date", "msg_time", "provider", "direction", "scope"])
        mc = pd.DataFrame([[day, "20:05", day, 100, 3.0, 150, 4.0]], columns=["msg_date", "msg_time", "mops_date", "g95", "g95_chg", "diesel", "diesel_chg"])
        return S.Tables(headsup=hu, mops_close=mc)

    def _model(self):
        import json
        import os
        import retail_forecast as RF
        with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), RF.SIGNAL_MODEL_FILE), encoding="utf-8") as fh:
            return json.load(fh)

    def test_shipped_model_matches_feature_list(self):
        import retail_forecast as RF
        m = self._model()
        self.assertEqual(m["features"], list(RF.SIGNAL_FEATURES))
        for fuel in ("B7", "G95"):
            self.assertEqual(len(m[fuel]["up"]["w"]), len(RF.SIGNAL_FEATURES))
            self.assertGreater(m[fuel]["up"]["w"][2], 0)          # MOPS ปิดขึ้น → โอกาสปลีกขึ้นมากขึ้น
            self.assertLess(m[fuel]["down"]["w"][2], 0)

    def test_forecast_needs_fresh_signals_and_sums_to_one(self):
        import pandas as pd
        import retail_forecast as RF
        m, t = self._model(), self._tables()
        f = RF.signal_forecast(m, t, "B7", pd.Timestamp("2026-11-12"))
        self.assertAlmostEqual(f["p_down"] + f["p_flat"] + f["p_up"], 1.0)
        self.assertGreater(f["p_up"], f["p_down"])
        self.assertIsNone(RF.signal_forecast(m, t, "B7", pd.Timestamp("2026-11-14")))      # ฟีดค้าง 2 วัน
        self.assertIsNone(RF.signal_forecast({}, t, "B7", pd.Timestamp("2026-11-12")))

    def test_shadow_columns_and_rounds(self):
        import pandas as pd
        import forecast_card as fc
        row = lambda: {"card_id": "c", "fuel": "G95", "target_day": "2026-11-12", "call": "นิ่ง"}
        m, t = self._model(), self._tables()
        r = fc.signal_shadow([row()], "21:30", pd.Timestamp("2026-11-11 21:35"), t, m)[0]
        self.assertIn(r["sig_call"], ("ลง", "นิ่ง", "ขึ้น", "ไม่ชัด"))
        self.assertEqual(r["call"], "นิ่ง")                                                  # ไม่แตะผลของวิธีหลัก
        r = fc.signal_shadow([row()], "11:00", pd.Timestamp("2026-11-11 11:05"), t, m)[0]
        self.assertEqual((r["sig_call"], r["sig_p_up"]), ("", ""))
        self.assertTrue(r["sig_note"])
        r = fc.signal_shadow([row()], "21:30", pd.Timestamp("2026-11-10 21:35"), t, m)[0]    # ยังไม่ถึงเวลาตัดสัญญาณของวันเป้าหมาย
        self.assertEqual(r["sig_call"], "")
        self.assertEqual(fc.RETAIL_OUT_LAST, "R")                                             # ช่วงผลจริงคงตำแหน่งเดิม
        self.assertEqual(fc.RETAIL_COLUMNS[-5:], list(fc.SIG_KEYS))

    def test_long_history_only_prepends_older_days(self):
        import pandas as pd
        import retail_forecast as RF
        sheet = pd.DataFrame([[pd.Timestamp("2026-03-16"), "B7", 28.0, 0.1, 99.0]], columns=["day", "fuel", "ws", "mm", "retail"])
        h = RF.long_history(sheet)
        self.assertGreater(len(h), 500)
        self.assertEqual(float(h[(h.day == "2026-03-16") & (h.fuel == "B7")].retail.iloc[0]), 99.0)
        self.assertEqual(int((h.day > "2026-03-16").sum()), 0)
        self.assertIs(RF.long_history(sheet, path="/nonexistent.csv"), sheet)
