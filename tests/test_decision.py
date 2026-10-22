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


class FeedRobustnessTests(unittest.TestCase):
    def test_wrong_schema_table_is_treated_as_empty(self):
        """gviz ส่งชีทแรกมาแทนแท็บที่ไม่มีอยู่ (คอลัมน์ Date/Price...) — ต้องไม่ทำให้ระบบพัง"""
        wrong = pd.DataFrame({"Date": ["2/17/2026"], "Price": [87.75]})
        t = S.Tables(intraday=wrong, headsup=wrong, mops_close=wrong, fund=wrong)
        self.assertTrue(t.intraday.empty and t.headsup.empty and t.mops_close.empty and t.fund.empty)
        d = engine.decide("DIESEL", pd.Timestamp("2026-10-20 15:35"), t, M.load_params(), engine.load_backtest_summary())
        self.assertEqual(d["action_code"], "NO_DATA")

    def test_load_feed_validates_columns(self):
        import main as MAIN
        warnings = []
        MAIN.load_sheet_by_name = lambda name: pd.DataFrame({"Date": ["x"], "Price": [1]})
        self.assertTrue(MAIN.load_feed("Entry_Intraday", warnings).empty)
        self.assertTrue(any("ไม่มีคอลัมน์" in w for w in warnings))
        good = pd.DataFrame(columns=["msg_date", "msg_time", "provider", "direction", "scope"])
        MAIN.load_sheet_by_name = lambda name: good
        self.assertEqual(list(MAIN.load_feed("Entry_HeadsUp", []).columns), list(good.columns))


class IngestConversionTests(unittest.TestCase):
    """Sheets เก็บวัน/เวลาที่ worker เขียน (USER_ENTERED) เป็นเลขลำดับ — ingest ต้องแปลงกลับเป็นข้อความที่ signals.py อ่านได้"""

    def setUp(self):
        sys.modules.setdefault("openpyxl", type(sys)("openpyxl"))
        sys.modules.setdefault("pdfplumber", type(sys)("pdfplumber"))
        sys.modules.setdefault("jwt", type(sys)("jwt"))
        import ingest
        self.I = ingest

    def test_date_time_serials(self):
        self.assertEqual(self.I.cell_date(46316), "2026-10-21")          # 46316 = 2026-10-21 (เลขลำดับ Sheets)
        self.assertEqual(self.I.cell_date("2026-10-21 10:00:00"), "2026-10-21")
        self.assertEqual(self.I.cell_time(0.9791666667), "23:30")
        self.assertEqual(self.I.cell_time("9:05:00"), "09:05")
        self.assertEqual(self.I.cell_time("23:30"), "23:30")
        self.assertEqual(self.I.cell_datetime(46316.4375), "2026-10-21 10:30")
        self.assertEqual(self.I.cell_datetime("2026-10-21 10:30:05"), "2026-10-21 10:30")
        self.assertEqual(self.I.cell_date(""), "")

    def test_converted_feed_is_usable_by_signals(self):
        row = {"msg_date": self.I.cell_date(46316), "msg_time": self.I.cell_time(0.7291666667), "provider": "SFL", "direction": "UP", "scope": "ALL", "stated_time": ""}
        f = S.build_features(pd.Timestamp("2026-10-21 20:35"), S.Tables(headsup=pd.DataFrame([row])), "B7")
        self.assertEqual(f["up_n"], 1)                                   # 0.7291666 = 17:30


class BackfillTests(unittest.TestCase):
    def test_plan_inserts_places_rows_by_date_and_is_idempotent(self):
        import datetime as dt
        import backfill as B
        d = lambda y, m, day: dt.date(y, m, day)
        existing = [d(2026, 7, 7), d(2026, 7, 7), d(2026, 7, 8), d(2026, 7, 8), d(2026, 9, 23), d(2026, 9, 23)]   # 2 แถวต่อวัน (G95/DS)
        new = [[f"7/{x}/2026", "G95", 1.0] for x in (9, 10)] + [[f"7/{x}/2026", "DS", 2.0] for x in (9, 10)]
        plan = B.plan_inserts(existing, new)
        self.assertEqual([(p, len(r)) for p, r in plan], [(4, 4)])                 # แทรกหลังแถวที่ 4 (ก่อน 23 ก.ย.)
        self.assertEqual(B.plan_inserts(existing + [d(2026, 7, 9)] * 2 + [d(2026, 7, 10)] * 2, new), [])   # รันซ้ำ: ไม่มีอะไรต้องแทรก
        self.assertEqual(B.to_date(46316), d(2026, 10, 21))
        self.assertEqual(B.to_date("7/9/2026"), d(2026, 7, 9))

    def test_backfill_files_match_sheet_schema(self):
        import backfill as B
        m = B.read_csv("entry_mops_gap.csv", 6)
        self.assertEqual(len(m), 152)
        self.assertEqual({r[1] for r in m}, {"G95", "DS"})
        self.assertTrue(all(r[2] > 50 for r in m))
        w = B.read_csv("entry_wti_gap.csv", 7)
        self.assertEqual(len(w), 53)
        self.assertEqual(B.to_date(w[0][0]).isoformat(), "2026-07-09")
        self.assertEqual(B.to_date(w[-1][0]).isoformat(), "2026-08-30")


class ModelCheckTests(unittest.TestCase):
    def setUp(self):
        import check_models
        self.C = check_models

    def levels(self, f):
        return [lvl for lvl, _ in f]

    def test_png_is_valid(self):
        import base64
        raw = base64.b64decode(self.C.tiny_png_b64())
        self.assertTrue(raw.startswith(b"\x89PNG\r\n\x1a\n") and raw.endswith(b"IEND\xaeB`\x82"))

    def test_all_good(self):
        f = self.C.judge(200, ["deepseek-chat", "deepseek-flash"], {"deepseek-chat": (200, {}), "deepseek-flash": (200, {})})
        self.assertNotIn("FAIL", self.levels(f)); self.assertNotIn("WARN", self.levels(f))

    def test_model_missing_from_list_fails(self):
        f = self.C.judge(200, ["deepseek-flash"], {"deepseek-chat": (200, {}), "deepseek-flash": (200, {})})
        self.assertIn("FAIL", self.levels(f))
        self.assertTrue(any("ไม่อยู่ในรายชื่อ" in m for _, m in f))

    def test_rejected_probe_fails_but_transient_only_warns(self):
        self.assertIn("FAIL", self.levels(self.C.judge(200, ["a"], {"a": (404, "model not found")})))
        self.assertIn("FAIL", self.levels(self.C.judge(200, ["a"], {"a": (400, "bad request")})))
        t = self.C.judge(None, None, {"a": (None, "timeout")})
        self.assertNotIn("FAIL", self.levels(t)); self.assertIn("WARN", self.levels(t))
        self.assertNotIn("FAIL", self.levels(self.C.judge(503, None, {"a": (429, "rate limit")})))

    def test_bad_key_fails(self):
        self.assertIn("FAIL", self.levels(self.C.judge(401, None, {"a": (401, "unauthorized")})))

    def test_main_exit_codes_with_mocked_http(self):
        import os
        C = self.C
        os.environ["DEEPSEEK_API_KEY"] = "k"
        C.list_models = lambda key: (200, ["deepseek-chat", "deepseek-flash"], {})
        C.probe_text = lambda key, m: (200, {}); C.probe_vision = lambda key, m: (200, {})
        self.assertEqual(C.main(), 0)
        C.probe_vision = lambda key, m: (404, "model not found")
        self.assertEqual(C.main(), 1)
        C.list_models = lambda key: (503, None, "down"); C.probe_vision = lambda key, m: (None, "net")
        self.assertEqual(C.main(), 0)             # ไม่ชี้ขาด -> ไม่ทำให้ workflow ล้ม
        del os.environ["DEEPSEEK_API_KEY"]
        self.assertEqual(C.main(), 1)


class LiveStatsTests(unittest.TestCase):
    def test_live_stats_with_outcomes(self):
        n = engine.MIN_LIVE_OUTCOMES + 2
        log = pd.DataFrame({"asof": pd.date_range("2026-09-01 15:30", periods=n, freq="D").astype(str),
                            "fuel": "B7", "phase": "PM", "action_code": "NORMAL",
                            "outcome_delta": [0.3 if i % 2 else -0.1 for i in range(n)]})
        r = engine.live_stats(log, "B7", "PM", "NORMAL")
        self.assertEqual(r["source"], "live")
        self.assertEqual(r["n_days"], n)


class FundBannerTests(unittest.TestCase):
    import main as MAIN

    def feed(self):
        return pd.DataFrame([{"msg_date": "2026-10-21", "msg_time": "20:05", "issue_no": 5, "effective_date": "2026-10-22",
                              "diesel_old": -1.5, "diesel_new": -1.0, "benzene_old": 0.5, "benzene_new": 0.5}])

    def test_no_banner_when_fund_healthy(self):
        self.assertEqual(self.MAIN.build_fund_risk_banner({"cash": 5000, "runway": 80, "status": "OK"}, self.feed()), "")
        self.assertEqual(self.MAIN.build_fund_risk_banner({}, self.feed()), "")

    def test_danger_banner_with_notice(self):
        html = self.MAIN.build_fund_risk_banner({"cash": -108.76, "runway": -0.3, "subsidy": 345.18, "balance": -103045, "status": "CRITICAL"}, self.feed())
        self.assertIn("fund-risk-danger", html)
        self.assertIn("เงินสดหมดแล้ว", html)
        self.assertIn("-1.50 → -1.00", html)             # ดีเซล: ลดเงินอุดหนุน 0.50
        self.assertIn("22/10/2026", html)
        self.assertIn("ไม่เปลี่ยน", html)                   # เบนซินไม่เปลี่ยน

    def test_warning_banner_without_notice(self):
        html = self.MAIN.build_fund_risk_banner({"cash": 900, "runway": 30, "subsidy": 30, "balance": -5000, "status": "WATCH"}, pd.DataFrame())
        self.assertIn("fund-risk-warning", html)
        self.assertIn("ยังไม่พบประกาศ", html)


class DispatchTests(unittest.TestCase):
    import main as MAIN

    def setUp(self):
        self.params = M.load_params()
        self.summary = engine.load_backtest_summary()
        rows = [{"msg_date": "2026-10-20", "msg_time": "17:40", "provider": p, "direction": "UP", "scope": "ALL", "stated_time": "23:30"}
                for p in ("SFL", "IRPC", "PTT")]
        self.t = tables(headsup=rows, mops=[{"msg_date": "2026-10-20", "msg_time": "18:30", "mops_date": "2026-10-20", "g95": 100,
                                             "g95_chg": 3.0, "diesel": 120, "diesel_chg": 6.0}])

    def test_window_mapping(self):
        w = {"D06": 6.0, "D12": 12.0, "D15": 15.5, "D20": 20.5}
        f = lambda s: engine.dispatch_window(pd.Timestamp(s), w)
        self.assertIsNone(f("2026-10-20 05:59"))
        self.assertEqual(f("2026-10-20 06:00"), "D06")
        self.assertEqual(f("2026-10-20 15:35"), "D15")
        self.assertEqual(f("2026-10-20 23:00"), "D20")

    def test_before_first_window_is_not_evaluated(self):
        d = engine.decide_dispatch("DIESEL", pd.Timestamp("2026-10-20 05:00"), self.t, self.params, self.summary)
        self.assertEqual(d["action_code"], "NO_DATA")

    def test_evening_strong_signals_say_dispatch_now_with_arrival_time(self):
        d = engine.decide_dispatch("DIESEL", pd.Timestamp("2026-10-20 20:35"), self.t, self.params, self.summary, 10)
        self.assertEqual(d["window"], "D20")
        self.assertEqual(d["action_code"], "URGENT")
        self.assertEqual(d["arrive"], pd.Timestamp("2026-10-21 06:35"))
        self.assertIn("06:35", d["headline"])
        self.assertIn("วันถัดไป", d["headline"])

    def test_weak_window_is_forced_to_normal(self):
        # G95 หน้าต่าง 12:00 ผ่านเกณฑ์ไม่ถึง "พอใช้" ในการทดสอบย้อนหลัง → ห้ามแนะนำเร่ง/ชะลอ
        self.assertEqual(engine.dispatch_quality(self.summary, "G95_D12_L10")["grade"], "อ่อน")
        d = engine.decide_dispatch("GASOHOL95", pd.Timestamp("2026-10-20 13:00"), self.t, self.params, self.summary, 10)
        self.assertEqual(d["action_code"], "NORMAL")

    def test_lead_changes_arrival(self):
        a = engine.decide_dispatch("DIESEL", pd.Timestamp("2026-10-20 20:35"), self.t, self.params, self.summary, 8)
        b = engine.decide_dispatch("DIESEL", pd.Timestamp("2026-10-20 20:35"), self.t, self.params, self.summary, 12)
        self.assertEqual((b["arrive"] - a["arrive"]).total_seconds(), 4 * 3600)

    def test_section_renders_all_leads_and_hides_non_default(self):
        asof = pd.Timestamp("2026-10-20 20:35")
        by_lead = {l: {fk: engine.decide_dispatch(fk, asof, self.t, self.params, self.summary, l) for fk in self.MAIN.FUEL_CONFIG} for l in (8, 10, 12)}
        html = self.MAIN.build_dispatch_section(by_lead, 10, asof)
        self.assertEqual(html.count("lead-panel' data-lead"), 3)
        self.assertEqual(html.count(" hidden"), 2)
        self.assertIn("ส่งรถออกเลยตอนนี้", html)
        self.assertIn("pbar", html)
        self.assertEqual(self.MAIN.build_dispatch_section({}, 10, asof), "")


class DispatchLogTests(unittest.TestCase):
    import log_dispatch as LDP
    import main as MAIN

    def snap(self, asof, fuel="B7", lead=10, code="URGENT"):
        a = pd.Timestamp(asof)
        return {"asof": a.strftime("%Y-%m-%d %H:%M"), "window": "D20", "fuel": fuel, "lead": lead, "action_code": code,
                "raw_action_code": code, "grade": "ดี", "p_rise": 0.6, "p_fall": 0.05,
                "arrive": (a + pd.Timedelta(hours=lead)).strftime("%Y-%m-%d %H:%M")}

    def test_new_rows_skips_no_data_and_duplicates(self):
        s = [self.snap("2026-10-22 20:50"), self.snap("2026-10-22 20:50", code="NO_DATA"), self.snap("2026-10-22 20:50", lead=8)]
        rows = self.LDP.new_rows([], s)
        self.assertEqual(len(rows), 2)
        again = self.LDP.new_rows(rows, s)
        self.assertEqual(again, [])
        self.assertEqual(rows[0]["date"], "2026-10-22")
        self.assertEqual(rows[0]["outcome_delta"], "")

    def test_outcome_waits_for_arrival_plus_24h(self):
        pr = pd.DataFrame({"provider": "A", "warehouse": "w", "fuel": "B7",
                           "price": [30.0, 31.0], "effective": ["2026-10-22 00:00", "2026-10-23 06:00"]})
        book = S.PriceBook(pr)
        arrive = pd.Timestamp("2026-10-22 06:50")
        self.assertIsNone(self.LDP.compute_outcome(book, "B7", arrive, pd.Timestamp("2026-10-23 06:00")))
        res = self.LDP.compute_outcome(book, "B7", arrive, pd.Timestamp("2026-10-23 07:30"))
        self.assertEqual(res, (30.0, 31.0, 1.0))

    def log_df(self, n):
        rows = []
        for i in range(n):
            d = pd.Timestamp("2026-10-01 20:50") + pd.Timedelta(days=i)
            r = self.snap(d, code="URGENT" if i % 2 == 0 else "WAIT")
            r["date"] = d.strftime("%Y-%m-%d")
            r["outcome_delta"] = 0.5 if i % 2 == 0 else -0.5      # ถูกทั้งหมด: เร่งแล้วขึ้น , ชะลอแล้วลง
            rows.append(r)
        return pd.DataFrame(rows)

    def test_live_stats_counts_first_row_per_window_and_utility(self):
        df = self.log_df(24)
        dup = df.iloc[[0]].copy()
        dup["asof"] = "2026-10-01 22:50"                          # รอบที่สองของวัน/ช่วงเดียวกัน ต้องไม่นับ
        dup["outcome_delta"] = -9.0
        st = engine.dispatch_live_stats(pd.concat([df, dup], ignore_index=True), "B7", 10)
        self.assertEqual(st["n"], 24)
        self.assertAlmostEqual(st["total_gain"], 24 * 0.5)
        self.assertEqual(st["by_action"]["URGENT"]["rise_rate"], 1.0)

    def test_live_stats_none_without_data_or_columns(self):
        self.assertIsNone(engine.dispatch_live_stats(pd.DataFrame(), "B7", 10))
        self.assertIsNone(engine.dispatch_live_stats(pd.DataFrame({"x": [1]}), "B7", 10))

    def test_live_html_accumulating_vs_summarised(self):
        few = self.MAIN.build_dispatch_live(self.log_df(5), 10)
        self.assertIn("กำลังสะสมผล", few)
        many = self.MAIN.build_dispatch_live(self.log_df(24), 10)
        self.assertIn("เร่งส่ง", many)
        self.assertIn("+0.500", many)
        self.assertIn("เริ่มบันทึกคำแนะนำจริงแล้ว", self.MAIN.build_dispatch_live(pd.DataFrame(), 10))
        ready = self.MAIN.build_dispatch_live(pd.DataFrame(), 10, engine.load_backtest_summary())
        self.assertIn("ความพร้อมแจ้งเตือน", ready)
        self.assertIn("ยังไม่พร้อม", ready)
        self.assertIn("ยังไม่ส่งข้อความใด ๆ", ready)


class SheetSerialTests(unittest.TestCase):
    """อ่านชีทด้วย UNFORMATTED_VALUE วันเวลาเป็นเลข serial — เคยถูกตีเป็นปี 1970 ทำให้ผลลัพธ์ทุกแถวถูกเติมทันทีเป็น 0"""
    import common as COMMON
    import log_decisions as LDEC
    import log_dispatch as LDIS

    @staticmethod
    def serial(ts):
        return (pd.Timestamp(ts) - pd.Timestamp("1899-12-30")).total_seconds() / 86400

    def test_to_ts(self):
        t = self.COMMON.to_ts(self.serial("2026-10-22 15:45"))
        self.assertEqual(t.strftime("%Y-%m-%d %H:%M"), "2026-10-22 15:45")
        self.assertEqual(self.COMMON.to_ts(str(self.serial("2026-10-22 15:45"))).strftime("%H:%M"), "15:45")   # เลขที่มาเป็นข้อความ
        self.assertEqual(self.COMMON.to_ts("2026-10-22 15:45").strftime("%H:%M"), "15:45")
        self.assertTrue(pd.isna(self.COMMON.to_ts("")))
        self.assertTrue(pd.isna(self.COMMON.to_ts(None)))

    def test_price_outcome_with_serial_effective_is_not_garbage(self):
        eff = [self.serial("2026-10-22 00:00"), self.serial("2026-10-23 06:00")]
        prices = pd.DataFrame({"provider": "A", "warehouse": "w", "fuel": "B7", "price": [30.0, 31.0], "effective": eff})
        prices["effective"] = prices["effective"].map(self.COMMON.to_ts)
        book = S.PriceBook(prices)
        res = self.LDIS.compute_outcome(book, "B7", self.COMMON.to_ts(self.serial("2026-10-22 06:50")), pd.Timestamp("2026-10-23 07:30"))
        self.assertEqual(res, (30.0, 31.0, 1.0))                 # เดิมได้ (30,30,0) เพราะทั้งคู่ถูกมองเป็นปี 1970

    def test_premature_dispatch_rows_are_detected(self):
        bad = {"arrive": self.serial("2026-10-22 21:04"), "outcome_delta": 0, "outcome_filled_at": self.serial("2026-10-22 15:45")}
        ok = {"arrive": self.serial("2026-10-22 21:04"), "outcome_delta": 0.3, "outcome_filled_at": self.serial("2026-10-23 22:00")}
        empty = {"arrive": self.serial("2026-10-22 21:04"), "outcome_delta": "", "outcome_filled_at": ""}
        self.assertTrue(self.LDIS.premature(bad))
        self.assertFalse(self.LDIS.premature(ok))
        self.assertFalse(self.LDIS.premature(empty))

    def test_premature_decision_rows_are_detected(self):
        bad = {"date": self.serial("2026-10-21"), "outcome_delta": 0, "outcome_filled_at": self.serial("2026-10-21 17:41")}
        ok = {"date": self.serial("2026-10-21"), "outcome_delta": -0.4, "outcome_filled_at": self.serial("2026-10-22 16:30")}
        self.assertTrue(self.LDEC.premature(bad))
        self.assertFalse(self.LDEC.premature(ok))

    def test_merge_snapshot_does_not_duplicate_when_sheet_returns_serials(self):
        existing = [{"asof": self.serial("2026-10-21 17:14"), "date": self.serial("2026-10-21"), "fuel": "B7", "phase": "PM", "outcome_delta": ""}]
        snap = [{"asof": "2026-10-21 17:41", "fuel": "B7", "phase": "PM", "action_code": "WAIT", "action": "x", "p_rise": 0.1, "p_fall": 0.3}]
        appends, updates = self.LDEC.merge_snapshot(existing, snap)
        self.assertEqual(appends, [])                             # เดิมเพิ่มแถวซ้ำทุกรอบ
        self.assertEqual(list(updates), [0])                       # รอบหลังทับรอบก่อน

    def test_dispatch_dedupe_key_survives_serials(self):
        existing = [{"asof": self.serial("2026-10-22 13:04"), "fuel": "B7", "lead": 10}]
        snap = [{"asof": "2026-10-22 13:04", "fuel": "B7", "lead": 10, "action_code": "WAIT", "arrive": "2026-10-22 23:04"}]
        self.assertEqual(self.LDIS.new_rows(existing, snap), [])

    def test_live_stats_counts_one_row_per_day(self):
        n = engine.MIN_LIVE_OUTCOMES + 2
        base = pd.DataFrame({"asof": pd.date_range("2026-09-01 15:30", periods=n, freq="D").astype(str), "fuel": "B7", "phase": "PM",
                             "action_code": "NORMAL", "outcome_delta": [0.3] * n})
        base["date"] = base["asof"].str[:10]
        dup = base.iloc[[0]].copy(); dup["asof"] = base["asof"].iloc[0].replace("15:30", "17:30"); dup["outcome_delta"] = 9.0
        st = engine.live_stats(pd.concat([base, dup], ignore_index=True), "B7", "PM", "NORMAL")
        self.assertEqual(st["n_days"], n)                         # แถวซ้ำของวันเดียวกันไม่นับสองครั้ง


if __name__ == "__main__":
    unittest.main()
