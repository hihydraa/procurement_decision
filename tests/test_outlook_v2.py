"""ฟีเจอร์ชุด v2 ของโมเดลแนวโน้ม (ราคาที่รู้ ณ เวลาตัดสินใจ) : ห้ามใช้ข้อมูลที่ยังไม่โพสต์ และต้องถอยไปโมเดลเดิมได้เมื่อฟีดราคาไม่พร้อม"""
import copy
import json
import os
import sys
import unittest
from unittest import mock

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine as E   # noqa: E402
import model as M    # noqa: E402
import retrain as R  # noqa: E402
import signals as S  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _prices(rows, cols=("provider", "warehouse", "fuel", "price", "effective", "posted")):
    return pd.DataFrame(rows, columns=list(cols))


def _feed():
    """2 คลัง : ราคาปัจจุบัน 30.00/30.40 ; คลัง A ประกาศ 17:30 ว่าจะขึ้นเป็น 30.50 มีผล 23:59"""
    return _prices([
        ["A", "x", "G95", 29.50, "2026-11-08 05:00", "2026-11-08 05:00"],
        ["A", "x", "G95", 30.00, "2026-11-10 05:00", "2026-11-09 17:00"],
        ["B", "y", "G95", 30.40, "2026-11-10 05:00", "2026-11-09 17:10"],
        ["A", "x", "G95", 30.50, "2026-11-10 23:59", "2026-11-10 17:30"],
        ["A", "x", "B7", 40.00, "2026-11-10 05:00", "2026-11-09 17:00"],
    ])


class KnownPricesTest(unittest.TestCase):
    def test_pending_price_seen_only_after_it_is_posted(self):
        kp = S.KnownPrices(_feed(), "G95")
        before = S.price_features(kp, pd.Timestamp("2026-11-10 15:30"), 24)
        after = S.price_features(kp, pd.Timestamp("2026-11-10 20:30"), 24)
        self.assertAlmostEqual(before["pend"], 0.0)                    # ยังไม่โพสต์ → ห้ามรู้
        self.assertEqual(before["pend_up_n"], 0)
        self.assertAlmostEqual(after["pend"], 0.40)                    # ต่ำสุดใหม่ = B 30.40 (A ขึ้นเป็น 30.50)
        self.assertEqual((after["pend_up_n"], after["pend_dn_n"]), (1, 0))

    def test_momentum_dispersion_and_fuel_filter(self):
        kp = S.KnownPrices(_feed(), "G95")
        f = S.price_features(kp, pd.Timestamp("2026-11-10 20:30"), 24)
        self.assertAlmostEqual(f["mom1"], 0.50)                        # 30.00 − 29.50 (ราคาที่มีผลเมื่อ 24 ชม. ก่อน)
        self.assertAlmostEqual(f["disp"], 0.20)                        # ค่ากลาง 30.20 − ต่ำสุด 30.00
        self.assertEqual(f["n_series"], 2)
        self.assertTrue(np.isnan(f["mom10"]))

    def test_posted_falls_back_to_effective_and_msg_columns_win(self):
        no_posted = _feed().drop(columns=["posted"])
        kp = S.KnownPrices(no_posted, "G95")
        self.assertAlmostEqual(S.price_features(kp, pd.Timestamp("2026-11-10 20:30"), 24)["pend"], 0.0)   # ไม่รู้เวลาโพสต์ → ถือว่ารู้ตอนมีผล
        hist = _feed().drop(columns=["posted"]).assign(msg_date="2026-11-01", msg_time="08:00")
        self.assertEqual(str(S._posted_ts(hist).iloc[0]), "2026-11-01 08:00:00")

    def test_empty_and_freshness(self):
        t = S.Tables()
        self.assertTrue(t.known("G95").empty)
        self.assertFalse(t.has_prices("G95", pd.Timestamp("2026-11-10 20:30")))
        t.set_prices(_feed())
        self.assertTrue(t.has_prices("G95", pd.Timestamp("2026-11-10 20:30")))
        self.assertFalse(t.has_prices("G95", pd.Timestamp("2026-11-13 20:30")))     # ฟีดค้างเกิน 36 ชม.
        self.assertTrue(all(np.isnan(v) for v in S.price_features(S.KnownPrices(None, "G95"), pd.Timestamp("2026-11-10"), 24).values()))


class VectorTest(unittest.TestCase):
    def test_length_order_and_clipping(self):
        base = {"dow": 4, "pend": 9.0, "pend_up_n": 3, "mom1": float("nan"), "disp": 0.5}
        x = M.vectorize_v2(base)
        self.assertEqual(len(x), len(M.FEATURES_V2))
        self.assertEqual(M.FEATURES_V2[:len(M.FEATURES)], M.FEATURES)
        v = dict(zip(M.FEATURES_V2, x))
        self.assertEqual((v["pend"], v["pend_pos"], v["pend_neg"]), (3.0, 3.0, 0.0))    # ตัดค่าสุดโต่ง
        self.assertEqual((v["mom1"], v["dow_fri"], v["dow_mon"], v["disp"]), (0.0, 1.0, 0.0, 0.5))
        np.testing.assert_array_equal(x[:len(M.FEATURES)], M.vectorize(base))

    def test_outlook_version_changes_with_outlook_model(self):
        p = {"trained_at": "2026-10-21", "models": {}, "outlook": {"models": {"G95_H24": {"w": [1]}}, "feature_set": {}}}
        q = copy.deepcopy(p)
        q["outlook"]["feature_set"]["G95_H24"] = "v2"
        self.assertNotEqual(M.outlook_version(p), M.outlook_version(q))
        self.assertTrue(M.outlook_version(p).startswith(M.model_version(p) + "+o"))
        self.assertEqual(M.outlook_version(None), "")


class ShippedModelTest(unittest.TestCase):
    """ไฟล์โมเดลที่ใช้งานจริง : G95 24 ชม. เท่านั้นที่เป็น v2 และต้องมีโมเดลเดิมไว้ถอย"""

    @classmethod
    def setUpClass(cls):
        with open(os.path.join(ROOT, "model_params.json"), encoding="utf-8") as fh:
            cls.params = json.load(fh)

    def test_only_gated_key_uses_v2(self):
        o = self.params["outlook"]
        self.assertEqual(o.get("feature_set"), {k: "v2" for k in M.OUTLOOK_V2_KEYS})
        for key, mm in o["models"].items():
            n = len(M.FEATURES_V2) if key in M.OUTLOOK_V2_KEYS else len(M.FEATURES)
            for side in mm.values():
                if isinstance(side, dict) and "w" in side:
                    self.assertEqual(len(side["w"]), n, key)
        for key in M.OUTLOOK_V2_KEYS:
            for side in o["models_v1"][key].values():
                if isinstance(side, dict) and "w" in side:
                    self.assertEqual(len(side["w"]), len(M.FEATURES))

    def _run(self, fuel, tables):
        with mock.patch("engine.feed_status", return_value={"alive": True, "newest": None}):
            return E.decide_outlook(fuel, pd.Timestamp("2026-11-10 21:00"), tables, self.params)

    def test_v2_used_with_fresh_feed_and_v1_fallback_without(self):
        out = self._run("GASOHOL95", S.Tables().set_prices(_feed()))
        h = {x["hours"]: x for x in out["horizons"]}
        self.assertEqual((h[24]["feature_set"], h[48]["feature_set"]), ("v2", "v1"))
        self.assertIsNone(out["note"])
        self.assertTrue(any("ประกาศราคาล่วงหน้า" in r for r in h[24]["reasons"]), h[24]["reasons"])
        self.assertLessEqual(len(h[24]["reasons"]), 6)
        out1 = self._run("GASOHOL95", S.Tables())                                   # ไม่มีฟีดราคา → โมเดลชุดเดิม + แจ้งเหตุ
        self.assertEqual(out1["horizons"][0]["feature_set"], "v1")
        self.assertIn("ใช้โมเดลชุดเดิม", out1["note"])
        self.assertTrue(all(0.0 <= v <= 1.0 for v in out1["horizons"][0]["p"].values()))
        d = self._run("DIESEL", S.Tables().set_prices(_feed()))
        self.assertTrue(d["horizons"] and all(x["feature_set"] == "v1" for x in d["horizons"]))


class RetrainV2Test(unittest.TestCase):
    def test_sheet_price_rows_get_post_time(self):
        base = pd.DataFrame([["2026-10-20", "09:00", "2026-10-20 09:00", "A", "x", "G95", 30.0]],
                            columns=["msg_date", "msg_time", "effective", "provider", "warehouse", "fuel", "price"])
        inc = _prices([["A", "x", "G95", 30.5, "2026-11-10 23:59", "2026-11-10 17:30"],
                       ["A", "x", "G95", 30.6, "2026-11-11 05:00", ""]])
        merged, n = R.merge_increment("prices", base, inc)
        self.assertEqual(n, 2)
        new = merged[merged.effective > "2026-11"].sort_values("effective")
        self.assertEqual(list(zip(new.msg_date, new.msg_time)), [("2026-11-10", "17:30"), ("2026-11-11", "05:00")])

    def test_merge_models_keeps_feature_set_in_step_with_model(self):
        old_p = {"trained_at": "o", "models": {}, "outlook": {"models": {"G95_H24": "old"}, "feature_set": {"G95_H24": "v2"}, "models_v1": {"G95_H24": "old1"}}}
        new_p = {"trained_at": "n", "models": {}, "outlook": {"trained_at": "n", "models": {"G95_H24": "new"}}}          # ชุดใหม่ฝึกแบบ v1 (เช่น ไม่มีตารางราคา)
        s = {"generated": "g", "outlook_results": {"G95_H24": {}}}
        p, _ = R.merge_models(old_p, s, new_p, s, ["outlook_results:G95_H24"])
        self.assertEqual(p["outlook"]["models"]["G95_H24"], "new")
        self.assertNotIn("G95_H24", p["outlook"].get("feature_set", {}))
        self.assertNotIn("G95_H24", p["outlook"].get("models_v1", {}))
        new_p["outlook"].update(feature_set={"G95_H24": "v2"}, models_v1={"G95_H24": "new1"})
        p, _ = R.merge_models({"trained_at": "o", "models": {}, "outlook": {"models": {"G95_H24": "old"}}}, s, new_p, s, ["outlook_results:G95_H24"])
        self.assertEqual((p["outlook"]["feature_set"]["G95_H24"], p["outlook"]["models_v1"]["G95_H24"]), ("v2", "new1"))
        p, _ = R.merge_models(old_p, s, new_p, s, [])
        self.assertEqual(p["outlook"]["models"]["G95_H24"], "old")


if __name__ == "__main__":
    unittest.main()
