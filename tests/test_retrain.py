"""ทดสอบ retrain.py (ประตูตรวจ/การรวมข้อมูล) และเกณฑ์อนุญาตแจ้งเตือน engine.alert_status — ข้อมูลสังเคราะห์ ไม่แตะเน็ต"""
import os
import sys
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine  # noqa: E402
import model as M  # noqa: E402
import retrain as R  # noqa: E402


def models(**kw):
    """โมเดลสังเคราะห์: kw = {'rise': {feature: w}, 'fall': {feature: w}}"""
    def w(d):
        return [float(d.get(f, 0.0)) for f in M.FEATURES]
    return {"rise": {"w": w(kw.get("rise", {})), "b": 0, "mu": [0] * 8, "sd": [1] * 8},
            "fall": {"w": w(kw.get("fall", {})), "b": 0, "mu": [0] * 8, "sd": [1] * 8}}


def res(auc_r, auc_f, gain, n=100):
    return {"auc_rise": auc_r, "auc_fall": auc_f, "holdout_policy": {"total_gain_thb_per_litre": gain, "n_days": n}}


class MergeIncrementTests(unittest.TestCase):
    def test_only_rows_newer_than_base_and_no_image_trend(self):
        base = pd.DataFrame({"msg_date": ["2026-09-28", "2026-09-29"], "msg_time": ["11:30", "15:00"], "kind": ["TREND", "TREND"], "b": [0.1, 0.2], "d": [0.3, 0.4]})
        inc = pd.DataFrame({"msg_date": ["2026-09-29", "2026-10-22", "2026-10-22"], "msg_time": ["15:00", "14:01", "15:00"],
                            "kind": ["TREND", "HALF_IMG", "TREND"], "b": [0.2, -3.16, 0.5], "d": [0.4, -4.94, 0.6]})
        merged, n = R.merge_increment("trend", base, inc)
        self.assertEqual(n, 1)                                              # แถวรูป (HALF_IMG) ถูกตัด และแถวเก่าซ้ำฐานถูกตัด
        self.assertEqual(merged.iloc[-1]["b"], 0.5)
        self.assertNotIn("HALF_IMG", merged["kind"].tolist())

    def test_prices_use_effective_time_and_keep_base_columns(self):
        base = pd.DataFrame({"msg_date": ["x"], "effective": ["2026-10-21 00:00"], "provider": ["A"], "warehouse": ["w"], "fuel": ["B7"], "price": [30.0]})
        inc = pd.DataFrame({"provider": ["A", "A"], "warehouse": ["w", "w"], "fuel": ["B7", "B7"], "price": [30.0, 31.0], "effective": ["2026-10-20 8:00", "2026-10-22 8:00"]})
        merged, n = R.merge_increment("prices", base, inc)
        self.assertEqual(n, 1)
        self.assertEqual(merged.iloc[-1]["price"], 31.0)
        self.assertEqual(list(merged.columns), list(base.columns))

    def test_empty_increment_is_noop(self):
        base = pd.DataFrame({"msg_date": ["2026-10-01"], "msg_time": ["10:00"]})
        self.assertEqual(R.merge_increment("mops", base, pd.DataFrame())[1], 0)
        self.assertEqual(R.merge_increment("mops", base, None)[1], 0)


class TrendScaleTests(unittest.TestCase):
    def test_flags_when_image_scale_differs(self):
        rng = np.random.default_rng(0)
        n = 200
        base = pd.DataFrame({"msg_date": pd.date_range("2026-01-01", periods=n).astype(str), "msg_time": "11:30", "kind": "TREND",
                             "b": rng.normal(0, 0.5, n), "d": rng.normal(0, 0.6, n)})
        img = pd.DataFrame({"msg_date": "2026-10-22", "msg_time": "14:01", "kind": "HALF_IMG", "b": rng.normal(0, 3, 20), "d": rng.normal(0, 4, 20)})
        s = R.trend_scale(base, img)
        self.assertEqual(s["img_n"], 20)
        self.assertTrue(s["flag"])
        self.assertGreater(s["ratio_b"], 3)

    def test_no_ratio_with_too_few_image_rows(self):
        base = pd.DataFrame({"msg_date": ["2026-10-01"] * 3, "msg_time": "11:30", "kind": "TREND", "b": [0.1, -0.2, 0.3], "d": [0.1, 0.2, -0.3]})
        img = pd.DataFrame({"msg_date": "2026-10-22", "msg_time": "14:01", "kind": "HALF_IMG", "b": [-3.16], "d": [-4.94]})
        s = R.trend_scale(base, img)
        self.assertIsNone(s["ratio_b"])
        self.assertFalse(s["flag"])


class GateTests(unittest.TestCase):
    def setUp(self):
        self.good = models(rise={"trend": 0.5, "up_n": 0.4, "close_last": 0.5}, fall={"trend": -0.5, "close_last": -0.5})
        self.old_summary = {"results": {"B7_EVE": res(0.70, 0.65, 40.0)}, "dispatch_results": {"B7_D20_L10": res(0.68, 0.65, 60.0)}}

    def new(self, eve, d20, mod=None):
        mod = mod or self.good
        return ({"results": {"B7_EVE": eve}, "dispatch_results": {"B7_D20_L10": d20, "B7_D20_L8": res(0.6, 0.6, 5.0)}},
                {"models": {"B7_EVE": mod, "B7_D20_L10": mod, "B7_D20_L8": mod}})

    def test_passes_when_not_worse(self):
        s, p = self.new(res(0.69, 0.66, 38.0), res(0.68, 0.64, 59.0))
        g = R.gate(self.old_summary, s, p)
        self.assertTrue(all(x["ok"] for x in g.values()), g)
        self.assertTrue(g["dispatch_results:B7_D20_L8"]["ok"])              # key ใหม่ผ่านได้ถ้าสัมประสิทธิ์ผ่าน

    def test_auc_drop_beyond_tolerance_is_rejected(self):
        s, p = self.new(res(0.60, 0.66, 40.0), res(0.68, 0.65, 60.0))
        g = R.gate(self.old_summary, s, p)
        self.assertFalse(g["results:B7_EVE"]["ok"])
        self.assertTrue(g["dispatch_results:B7_D20_L10"]["ok"])             # ตัดสินต่อ key ไม่ลากกันไปทั้งชุด

    def test_gain_drop_is_rejected(self):
        s, p = self.new(res(0.70, 0.65, 40.0), res(0.68, 0.65, 10.0))       # กำไร/วัน 0.6 → 0.1
        self.assertFalse(R.gate(self.old_summary, s, p)["dispatch_results:B7_D20_L10"]["ok"])

    def test_wrong_sign_is_rejected_even_if_metrics_fine(self):
        bad = models(rise={"trend": -0.5, "up_n": 0.4, "close_last": 0.5}, fall={"trend": -0.5, "close_last": -0.5})
        s, p = self.new(res(0.70, 0.65, 40.0), res(0.68, 0.65, 60.0), bad)
        g = R.gate(self.old_summary, s, p)
        self.assertFalse(g["results:B7_EVE"]["ok"])
        self.assertIn("Trend", " ".join(g["results:B7_EVE"]["reasons"]))

    def test_evening_only_signs_not_enforced_in_afternoon(self):
        afternoon = models(rise={"trend": 0.4, "up_n": -0.3, "close_last": -0.2}, fall={"trend": -0.4, "close_last": 0.2})
        self.assertEqual(R.sign_issues(afternoon, "B7_PM"), [])
        self.assertEqual(R.sign_issues(afternoon, "B7_D15_L10"), [])
        self.assertGreaterEqual(len(R.sign_issues(afternoon, "B7_D20_L10")), 2)

    def test_nan_auc_is_rejected(self):
        s, p = self.new(res(float("nan"), 0.65, 40.0), res(0.68, 0.65, 60.0))
        self.assertFalse(R.gate(self.old_summary, s, p)["results:B7_EVE"]["ok"])


class MergeModelsTests(unittest.TestCase):
    def test_only_accepted_keys_replaced(self):
        old_p = {"trained_at": "2026-10-01", "models": {"A": "old", "B": "old"}, "thresholds": {"A": 1, "B": 1}, "dispatch": {"hour_profile": "old"}}
        new_p = {"trained_at": "2026-10-29", "data_through": "2026-10-28", "models": {"A": "new", "B": "new"}, "thresholds": {"A": 2, "B": 2},
                 "dispatch": {"hour_profile": "new", "trained_at": "2026-10-29"}}
        old_s = {"generated": "x", "results": {"A": "old", "B": "old"}, "dispatch_results": {}}
        new_s = {"generated": "y", "results": {"A": "new", "B": "new"}, "dispatch_results": {}}
        p, s = R.merge_models(old_p, old_s, new_p, new_s, ["results:A"])
        self.assertEqual(p["models"], {"A": "new", "B": "old"})
        self.assertEqual(p["thresholds"], {"A": 2, "B": 1})
        self.assertEqual(s["results"], {"A": "new", "B": "old"})
        self.assertEqual(p["trained_at"], "2026-10-29")
        self.assertEqual(p["dispatch"]["hour_profile"], "old")                # ไม่มี key แผนส่งรถผ่าน จึงไม่แตะ
        self.assertEqual(old_p["models"]["A"], "old")                          # ไม่แก้ของเดิมในที่

    def test_nothing_accepted_changes_nothing(self):
        old_p = {"trained_at": "2026-10-01", "models": {"A": "old"}, "thresholds": {"A": 1}}
        p, _ = R.merge_models(old_p, {"results": {"A": "old"}}, {"trained_at": "new", "models": {"A": "new"}, "thresholds": {"A": 2}}, {"generated": "y"}, [])
        self.assertEqual(p, old_p)


class AlertStatusTests(unittest.TestCase):
    def log(self, n, urgent_hit=0.8, wait_hit=0.8, gain_each=0.3):
        rows = []
        for i in range(n):
            code = "URGENT" if i % 2 == 0 else "WAIT"
            hit = (i / 2) % 1 < (urgent_hit if code == "URGENT" else wait_hit)
            if code == "URGENT":
                delta = 0.5 if hit else 0.0
            else:
                delta = -0.5 if hit else 0.0
            rows.append({"asof": (pd.Timestamp("2026-10-01 20:50") + pd.Timedelta(days=i)).strftime("%Y-%m-%d %H:%M"),
                         "date": (pd.Timestamp("2026-10-01") + pd.Timedelta(days=i)).strftime("%Y-%m-%d"), "window": "D20", "fuel": "B7",
                         "lead": 10, "action_code": code, "outcome_delta": delta})
        return pd.DataFrame(rows)

    SUMMARY = {"dispatch_results": {"B7_D20_L10": {"holdout_policy": {"total_gain_thb_per_litre": 30.0, "n_days": 100}}}}

    def test_not_enough_days(self):
        s = engine.alert_status(self.log(10), self.SUMMARY, "B7", "D20", 10)
        self.assertFalse(any(s["eligible"].values()))
        self.assertIn("10/40", s["reasons"][0])

    def test_no_data_at_all(self):
        s = engine.alert_status(pd.DataFrame(), self.SUMMARY, "B7", "D20", 10)
        self.assertEqual(s["n"], 0)
        self.assertFalse(any(s["eligible"].values()))

    def test_eligible_when_lift_and_gain_ok(self):
        s = engine.alert_status(self.log(60), self.SUMMARY, "B7", "D20", 10)
        self.assertEqual(s["n"], 60)
        self.assertTrue(s["eligible"]["URGENT"] and s["eligible"]["WAIT"], s)

    def test_gain_below_floor_blocks_everything(self):
        low = self.log(60)
        low["outcome_delta"] = low["outcome_delta"] * 0.01
        s = engine.alert_status(low, self.SUMMARY, "B7", "D20", 10)
        self.assertFalse(any(s["eligible"].values()))
        self.assertIn("กำไร", s["reasons"][0])

    def test_worse_than_two_thirds_of_backtest_blocks(self):
        strong_bt = {"dispatch_results": {"B7_D20_L10": {"holdout_policy": {"total_gain_thb_per_litre": 500.0, "n_days": 100}}}}   # 5 บาท/ลิตร/วัน
        s = engine.alert_status(self.log(60), strong_bt, "B7", "D20", 10)
        self.assertFalse(any(s["eligible"].values()))
        self.assertIn("backtest", s["reasons"][0])

    def test_weak_lift_blocks_that_action_only(self):
        mixed = self.log(60, urgent_hit=0.9, wait_hit=0.0)                       # ชะลอไม่เคยถูกเลย
        s = engine.alert_status(mixed, self.SUMMARY, "B7", "D20", 10)
        self.assertTrue(s["eligible"]["URGENT"])
        self.assertFalse(s["eligible"]["WAIT"])


class ModelVersionTests(unittest.TestCase):
    def test_changes_with_coefficients(self):
        a = {"trained_at": "2026-10-22", "models": {"k": {"w": [1]}}}
        b = {"trained_at": "2026-10-22", "models": {"k": {"w": [2]}}}
        self.assertNotEqual(M.model_version(a), M.model_version(b))
        self.assertEqual(M.model_version(a), M.model_version(dict(a)))
        self.assertEqual(M.model_version(None), "")


if __name__ == "__main__":
    unittest.main()
