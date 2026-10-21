"""ทดสอบหน่วย (ข้อมูลสังเคราะห์ ไม่แตะเน็ต/ข้อมูลส่วนตัว): python3 -m unittest discover -s tests"""
import os
import sys
import unittest

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import engine  # noqa: E402
import log_decisions as LD  # noqa: E402
import model as M  # noqa: E402
import signals as S  # noqa: E402


def tables(**kw):
    return S.Tables(intraday=pd.DataFrame(kw.get("intraday", [])), headsup=pd.DataFrame(kw.get("headsup", [])),
                    mops_close=pd.DataFrame(kw.get("mops", [])), fund=pd.DataFrame(kw.get("fund", [])))


class FeatureTests(unittest.TestCase):
    def test_no_lookahead_and_latest_trend(self):
        t = tables(intraday=[
            {"msg_date": "2026-10-20", "msg_time": "11:30", "kind": "TREND", "b": 0.5, "d": 1.0},
            {"msg_date": "2026-10-20", "msg_time": "15:00", "kind": "TREND", "b": 0.7, "d": 2.0},
            {"msg_date": "2026-10-20", "msg_time": "16:30", "kind": "HALF", "b": 9.0, "d": 9.0}])
        f = S.build_features(pd.Timestamp("2026-10-20 15:35"), t, "B7")
        self.assertEqual(f["trend"], 2.0)                       # ไม่เห็นแถว 16:30 (อนาคต)
        self.assertEqual(S.build_features(pd.Timestamp("2026-10-20 15:35"), t, "G95")["trend"], 0.7)
        self.assertTrue(np.isnan(S.build_features(pd.Timestamp("2026-10-20 11:00"), t, "B7")["trend"]))
        self.assertTrue(np.isnan(S.build_features(pd.Timestamp("2026-10-21 09:00"), t, "B7")["trend"]))   # คนละวัน/เก่าเกิน

    def test_headsup_distinct_providers_and_scope(self):
        rows = [{"msg_date": "2026-10-20", "msg_time": h, "provider": p, "direction": d, "scope": sc, "stated_time": ""}
                for h, p, d, sc in [("07:00", "SFL", "UP", "ALL"), ("17:30", "SFL", "UP", "ALL"), ("18:00", "IRPC", "UP", "GASOHOL"),
                                    ("18:10", "PTT", "DOWN", "ALL"), ("21:00", "BC", "UP", "ALL")]]
        t = tables(headsup=rows)
        f = S.build_features(pd.Timestamp("2026-10-20 20:35"), t, "B7")
        self.assertEqual((f["up_n"], f["dn_n"]), (1, 1))        # SFL นับครั้งเดียว , IRPC เฉพาะแก๊ส , BC โพสต์หลัง asof
        self.assertEqual(S.build_features(pd.Timestamp("2026-10-20 20:35"), t, "G95")["up_n"], 2)

    def test_fund_next_is_difference_effective_tomorrow(self):
        t = tables(fund=[{"msg_date": "2026-10-20", "msg_time": "20:10", "issue_no": 1, "effective_date": "2026-10-21",
                          "diesel_old": -2.25, "diesel_new": -1.72, "benzene_old": 0.0, "benzene_new": 0.5}])
        self.assertAlmostEqual(S.build_features(pd.Timestamp("2026-10-20 20:35"), t, "B7")["fund_next"], 0.53)
        self.assertEqual(S.build_features(pd.Timestamp("2026-10-20 15:35"), t, "B7")["fund_next"], 0.0)  # ยังไม่ประกาศ

    def test_price_book_min_and_staleness(self):
        p = pd.DataFrame([
            {"provider": "A", "warehouse": "x", "fuel": "B7", "price": 40.0, "effective": "2026-10-20 08:00"},
            {"provider": "B", "warehouse": "y", "fuel": "B7", "price": 39.0, "effective": "2026-10-20 09:00"},
            {"provider": "B", "warehouse": "y", "fuel": "B7", "price": 41.5, "effective": "2026-10-20 23:59"},
            {"provider": "C", "warehouse": "z", "fuel": "B7", "price": 35.0, "effective": "2026-10-10 09:00"}])   # เก่าเกิน 3 วัน
        b = S.PriceBook(p)
        got = b.min_at("B7", [pd.Timestamp("2026-10-20 15:30"), pd.Timestamp("2026-10-21 15:30")])
        self.assertEqual(list(got.round(2)), [39.0, 40.0])


class ModelTests(unittest.TestCase):
    def test_logistic_learns_signal(self):
        rng = np.random.default_rng(0)
        x = rng.normal(size=(600, 8))
        y = (x[:, 0] + 0.3 * rng.normal(size=600) > 0).astype(float)
        m = M.fit_logistic(x, y)
        p = M.predict_proba(m, x)
        self.assertGreater(p[y == 1].mean(), p[y == 0].mean() + 0.3)

    def test_choose_action(self):
        th = {"urgent": 0.45, "gradual": 0.30, "wait_fall": 0.35, "wait_max_rise": 0.20}
        self.assertEqual(M.choose_action(0.6, 0.05, th), "URGENT")
        self.assertEqual(M.choose_action(0.35, 0.1, th), "GRADUAL")
        self.assertEqual(M.choose_action(0.1, 0.5, th), "WAIT")
        self.assertEqual(M.choose_action(0.25, 0.5, th), "NORMAL")      # ขึ้นสูงเกินจะรอ
        self.assertEqual(M.utility("WAIT", -0.4), 0.4)                   # รอแล้วราคาลง = ได้ประโยชน์


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.params = M.load_params()
        self.summary = engine.load_backtest_summary()

    def test_no_params_means_no_data(self):
        d = engine.decide("DIESEL", pd.Timestamp("2026-10-20 15:35"), tables(), None)
        self.assertEqual(d["action_code"], "NO_DATA")

    def test_stale_feed_means_no_data(self):
        t = tables(headsup=[{"msg_date": "2026-09-01", "msg_time": "10:00", "provider": "SFL", "direction": "UP", "scope": "ALL", "stated_time": ""}])
        d = engine.decide("DIESEL", pd.Timestamp("2026-10-20 15:35"), t, self.params, self.summary)
        self.assertEqual(d["action_code"], "NO_DATA")
        self.assertIn("ไม่อัปเดต", d["summary"])

    def test_strong_up_signals_give_urgent_in_evening(self):
        rows = [{"msg_date": "2026-10-20", "msg_time": "17:40", "provider": p, "direction": "UP", "scope": "ALL", "stated_time": "23:30"}
                for p in ("SFL", "IRPC", "PTT")]
        t = tables(headsup=rows, mops=[{"msg_date": "2026-10-20", "msg_time": "18:30", "mops_date": "2026-10-20", "g95": 100, "g95_chg": 3.0,
                                        "diesel": 120, "diesel_chg": 6.0}])
        d = engine.decide("DIESEL", pd.Timestamp("2026-10-20 20:35"), t, self.params, self.summary)
        self.assertEqual(d["phase"], "EVE")
        self.assertEqual(d["action_code"], "URGENT")
        self.assertGreater(d["p_rise"], 0.45)
        self.assertTrue(any("ประกาศ" in r for r in d["reasons"]))

    def test_down_signals_give_wait(self):
        t = tables(mops=[{"msg_date": "2026-10-20", "msg_time": "18:30", "mops_date": "2026-10-20", "g95": 100, "g95_chg": -4.0,
                          "diesel": 120, "diesel_chg": -6.0}],
                   headsup=[{"msg_date": "2026-10-20", "msg_time": "16:00", "provider": "SFL", "direction": "DOWN", "scope": "ALL", "stated_time": "16:05"}])
        d = engine.decide("DIESEL", pd.Timestamp("2026-10-20 20:35"), t, self.params, self.summary)
        self.assertEqual(d["action_code"], "WAIT")


class LogTests(unittest.TestCase):
    def test_merge_and_outcome(self):
        snap = [{"asof": "2026-10-20 15:50", "fuel": "B7", "phase": "PM", "action_code": "URGENT", "action": "เร่งซื้อ", "p_rise": .5, "p_fall": .1},
                {"asof": "2026-10-20 13:00", "fuel": "B7", "phase": "PM", "action_code": "NORMAL", "action": "ซื้อปกติ", "p_rise": .2, "p_fall": .2},   # นอกช่วงบันทึก
                {"asof": "2026-10-20 20:45", "fuel": "B7", "phase": "EVE", "action_code": "NO_DATA", "action": "ข้อมูลไม่พอ", "p_rise": None, "p_fall": None}]
        appends, updates = LD.merge_snapshot([], snap)
        self.assertEqual([(a["date"], a["fuel"], a["phase"]) for a in appends], [("2026-10-20", "B7", "PM")])
        later = [dict(snap[0], asof="2026-10-20 17:00", action_code="GRADUAL")]
        _, upd = LD.merge_snapshot(appends, later)
        self.assertEqual(upd[0]["action_code"], "GRADUAL")                   # รอบหลังทับรอบก่อน (ยังไม่มีผลลัพธ์)
        p = pd.DataFrame([{"provider": "A", "warehouse": "x", "fuel": "B7", "price": 39.5, "effective": "2026-10-20 10:00"},
                          {"provider": "A", "warehouse": "x", "fuel": "B7", "price": 40.2, "effective": "2026-10-21 02:00"}])
        b = S.PriceBook(p)
        self.assertIsNone(LD.compute_outcome(b, "B7", pd.Timestamp("2026-10-20"), pd.Timestamp("2026-10-21 12:00")))   # ยังไม่ถึง 15:30 พรุ่งนี้
        self.assertEqual(LD.compute_outcome(b, "B7", pd.Timestamp("2026-10-20"), pd.Timestamp("2026-10-21 16:30")), (39.5, 40.2, 0.7))


class FreshnessTests(unittest.TestCase):
    def test_eppo_carried_values_are_detected(self):
        import main as MAIN
        rows = []
        for i, d in enumerate(pd.date_range("2026-10-01", "2026-10-10")):
            rows.append({"Date": d, "Oil Type": "H-DIESEL", "ExRefinery": 36.0 + (0.5 if i >= 3 else 0), "OilFund": -2.66})   # ขยับครั้งสุดท้าย 4 ต.ค.
        fresh = MAIN.eppo_fresh_date(pd.DataFrame(rows))
        self.assertEqual(fresh, pd.Timestamp("2026-10-04"))
        marked = pd.DataFrame(rows)
        marked["Source"] = ["FRESH"] * 6 + ["CARRIED"] * 4
        self.assertEqual(MAIN.eppo_fresh_date(marked), pd.Timestamp("2026-10-06"))


if __name__ == "__main__":
    unittest.main()
