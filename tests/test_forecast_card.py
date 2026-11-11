import unittest

import numpy as np
import pandas as pd

import forecast_card as fc


def synth(n=140, seed=5):
    rng = np.random.default_rng(seed)
    days = pd.date_range("2026-03-16", periods=n)
    mops = np.cumsum(rng.normal(0, 1.5, n)) + 80
    mp = pd.DataFrame({"day": days, "fuel": "B7", "mops": mops})
    wti = pd.DataFrame({"day": days, "wti": np.cumsum(rng.normal(0, 0.8, n)) + 60})
    dm = np.r_[0, np.diff(mops)]
    ex = np.zeros(n)
    for t in range(1, n):
        ex[t] = ex[t - 1] + 0.12 * dm[t - 1] + rng.normal(0, 0.03)
    fund = np.cumsum(np.where(rng.random(n) < 0.2, rng.normal(0, 0.4, n), 0.0))
    e = pd.DataFrame({"day": days, "fuel": "B7", "exref": ex + 30, "fund": fund - 2, "other": 7.0, "mm": 2.0, "excise": 6.0})
    e["ws"] = e.exref + e.fund + e.other
    e["resid"] = 0.0
    return mp, wti, e


class Decide(unittest.TestCase):
    def test_calls(self):
        self.assertEqual(fc.decide({"p_down": 0.8, "p_up": 0.05, "p_flat": 0.15}), ("ลง", "สูง"))
        self.assertEqual(fc.decide({"p_down": 0.62, "p_up": 0.1, "p_flat": 0.28}), ("ลง", "กลาง"))
        self.assertEqual(fc.decide({"p_down": 0.52, "p_up": 0.1, "p_flat": 0.38}), ("ลง", "ต่ำ"))
        self.assertEqual(fc.decide({"p_down": 0.1, "p_up": 0.7, "p_flat": 0.2}), ("ขึ้น", "กลาง"))
        self.assertEqual(fc.decide({"p_down": 0.45, "p_up": 0.4, "p_flat": 0.15}), ("ไม่ชัด", "ต่ำ"))          # ไม่นำกันพอ
        self.assertEqual(fc.decide({"p_down": 0.3, "p_up": 0.3, "p_flat": 0.4}), ("ไม่ชัด", "ต่ำ"))
        self.assertEqual(fc.decide({}), ("ไม่ชัด", "ต่ำ"))

    def test_probabilities_need_enough_history(self):
        self.assertEqual(fc.probabilities(-0.5, np.zeros(10)), {})
        p = fc.probabilities(-0.5, np.random.default_rng(1).normal(0, 0.1, 100))
        self.assertAlmostEqual(p["p_down"], fc.PROB_CAP)      # คำทายแรงมาก → ถูกจำกัดเพดาน
        self.assertTrue(p["capped"])
        self.assertAlmostEqual(p["p_down"] + p["p_flat"] + p["p_up"], 1.0)
        self.assertAlmostEqual(p["p_down"] + p["p_flat"] + p["p_up"], 1.0)
        self.assertLess(p["lo"], p["hi"])


class Guards(unittest.TestCase):
    def test_carried_mops_dropped_and_target_follows_mops_day(self):
        mops, wti, eppo = synth()
        last = mops.iloc[-1].copy()
        carried = pd.DataFrame([{"day": last.day + pd.Timedelta(days=1), "fuel": "B7", "mops": float(last.mops)}])      # ซ้ำค่าปิดล่าสุดเป๊ะ
        mops2 = pd.concat([mops, carried], ignore_index=True)
        f = fc.current_features(mops2, wti, eppo, "B7")
        self.assertTrue(f["carried"])
        self.assertEqual(f["mops_day"], last.day)
        card = fc.build_card("B7", "21:30", pd.Timestamp("2026-07-20 21:31"), f, None, "x", 0.0, np.zeros(5), "DIRECT", {"ลง": {}, "ขึ้น": {}, "n": {"ลง": 0, "ขึ้น": 0}}, "-")
        self.assertEqual(card["target_day"], (last.day + pd.Timedelta(days=1)).strftime("%Y-%m-%d"))
        self.assertIn("ซ้ำค่าเดิม", card["card_text"])

    def test_out_of_range_caps_confidence(self):
        mops, wti, eppo = synth()
        from diag_wf_components import build_dataset
        ds = build_dataset(mops, wti, eppo)
        f = fc.current_features(mops, wti, eppo, "B7")
        f_big = {**f, "m3": float(ds.m3.abs().max()) * 3}
        self.assertEqual(fc.out_of_range(ds, "B7", f_big), ["m3"])
        self.assertEqual(fc.out_of_range(ds, "B7", f), [])
        self.assertEqual(fc.cap_confidence("สูง", ["m3"]), "ต่ำ")
        self.assertEqual(fc.cap_confidence("สูง", []), "กลาง")             # ยังไม่ผ่านเดินหน้า → ไม่เกิน กลาง
        resid = np.random.default_rng(2).normal(0, 0.05, 100)
        card = fc.build_card("B7", "21:30", pd.Timestamp("2026-07-20 21:31"), f_big, 0.0, "x", -1.0, resid, "C", {"ลง": {}, "ขึ้น": {}, "n": {"ลง": 0, "ขึ้น": 0}}, "-", ["m3"])
        self.assertEqual(card["confidence"], "ต่ำ")
        self.assertIn("นอกช่วงที่โมเดลเคยเห็น", card["card_text"])


class Recommend(unittest.TestCase):
    def test_texts(self):
        self.assertIn("รอซื้อหลัง 07:00", fc.recommend("ลง", "กลาง", "05:00–06:59", "B7"))
        self.assertIn("รอซื้อหลัง 08:30", fc.recommend("ลง", "กลาง", None, "B7"))
        self.assertIn("ควรซื้อก่อน 22:30", fc.recommend("ขึ้น", "สูง", "ก่อนเที่ยงคืน (หลัง 20:30)", "B7"))
        self.assertIn("ความเชื่อมั่นต่ำ", fc.recommend("ลง", "ต่ำ", None, "B7"))
        self.assertIn("ไม่มีสัญญาณชัด", fc.recommend("ไม่ชัด", "ต่ำ", None, "B7"))

    def test_known_fund(self):
        n = pd.DataFrame({"msg_day": pd.to_datetime(["2026-11-10"]), "msg_time": ["19:54"], "eff_day": pd.to_datetime(["2026-11-11"]), "old": [-4.05], "new": [-2.93]})
        self.assertAlmostEqual(fc.known_fund_change(n, "B7", pd.Timestamp("2026-11-11")), 1.12)
        self.assertIsNone(fc.known_fund_change(n, "B7", pd.Timestamp("2026-11-12")))
        self.assertIsNone(fc.known_fund_change(n, "G95", pd.Timestamp("2026-11-11")))      # เบนซินจับคู่ไม่ได้


class Pipeline(unittest.TestCase):
    def test_card_end_to_end(self):
        mops, wti, eppo = synth()
        from diag_wf_components import build_dataset
        ds = build_dataset(mops, wti, eppo)
        f = fc.current_features(mops, wti, eppo, "B7")
        self.assertEqual(f["mops_day"], mops.day.max())
        pred, resid, model = fc.fit_predict(ds, "B7", f, None)
        self.assertEqual(model, "DIRECT")
        self.assertGreater(len(resid), 30)
        pred_c, _, model_c = fc.fit_predict(ds, "B7", f, 0.0)
        self.assertEqual(model_c, "C")
        prof = {"ลง": {"05:00–06:59": 6, "07:00–08:59": 3}, "ขึ้น": {}, "n": {"ลง": 9, "ขึ้น": 0}}
        card = fc.build_card("B7", "21:30", pd.Timestamp("2026-07-14 21:31"), f, 0.0, "x", pred_c, resid, model_c, prof, "ปตท. 41.79 บาท/ลิตร")
        self.assertEqual(card["card_id"], f"{(f['mops_day'] + pd.Timedelta(days=1)):%Y-%m-%d} 21:30 B7")
        self.assertEqual(len(fc.COLUMNS), len(card))
        self.assertIn("ราคาปลีก: ปตท. 41.79", card["card_text"])
        self.assertIn("ยังไม่ผ่านการทดสอบเดินหน้า", card["card_text"])
        self.assertIn(card["call"], ("ลง", "ขึ้น", "ไม่ชัด"))
        if card["call"] == "ไม่ชัด":
            self.assertIn("ไม่ชัดเจน", card["card_text"])

    def test_unclear_when_no_history(self):
        mops, wti, eppo = synth()
        f = fc.current_features(mops, wti, eppo, "B7")
        card = fc.build_card("B7", "16:00", pd.Timestamp("2026-07-14 16:01"), f, None, "ยังไม่ออก", 0.0, np.zeros(5), "DIRECT",
                             {"ลง": {}, "ขึ้น": {}, "n": {"ลง": 0, "ขึ้น": 0}}, "ไม่มีข้อมูล RetailLog")
        self.assertEqual(card["call"], "ไม่ชัด")
        self.assertIn("ข้อมูลย้อนหลังไม่พอ", card["card_text"])


class Timing(unittest.TestCase):
    def test_change_time_profile_and_text(self):
        rows = []
        days = pd.date_range("2026-10-01", periods=8)
        for d in days:
            for pv, wh in fc.DEPOTS.values():
                # ราคา 40.0 ตั้งแต่เย็นวันก่อน ลงเป็น 39.5 เวลา 05:30 ทุกวัน
                rows.append({"jobber": "x", "provider": pv, "warehouse": wh, "fuel": "B7", "effective": d - pd.Timedelta(hours=6), "price": 40.0})
                rows.append({"jobber": "x", "provider": pv, "warehouse": wh, "fuel": "B7", "effective": d + pd.Timedelta(hours=5.5), "price": 39.5})
        prices = pd.DataFrame(rows)
        prof = fc.change_time_profile(prices, "B7", days[1:])
        self.assertEqual(prof["n"]["ลง"], 7)
        self.assertEqual(prof["ลง"].get("05:00–06:59"), 7)
        txt, top = fc.timing_text({**prof, "n": {**prof["n"], "ลง": 7}}, "ลง")
        self.assertEqual(top, "05:00–06:59")
        self.assertIn("100%", txt)
        self.assertEqual(fc.timing_text(prof, "ไม่ชัด")[1], None)


class Outcome(unittest.TestCase):
    def test_fill_and_summary(self):
        e = pd.DataFrame({"day": pd.to_datetime(["2026-11-10", "2026-11-11"]), "fuel": "B7", "ws": [40.0, 39.5]})
        r = fc.fill_outcome({"fuel": "B7", "target_day": "2026-11-11", "call": "ลง"}, e)
        self.assertEqual((r["outcome_cls"], r["right"]), ("ลง", 1))
        r2 = fc.fill_outcome({"fuel": "B7", "target_day": "2026-11-11", "call": "ขึ้น"}, e)
        self.assertEqual(r2["right"], 0)
        self.assertEqual(fc.fill_outcome({"fuel": "B7", "target_day": "2026-11-12", "call": "ลง"}, e), None)
        self.assertEqual(fc.fill_outcome({"fuel": "B7", "target_day": "2026-11-11", "call": "ไม่ชัด"}, e)["right"], "")
        rows = [{"card_id": "2026-11-11 21:30 B7", "call": "ลง", "right": 1, "outcome_cls": "ลง", "outcome_dws": -0.5, "filled_at": "x"},
                {"card_id": "2026-11-12 21:30 B7", "call": "ไม่ชัด", "right": "", "outcome_cls": "นิ่ง", "outcome_dws": 0.0, "filled_at": "x"}]
        out = fc.summary_lines(rows)
        self.assertIn("ฟันธงทิศ 1 ใบ ถูก 1", out[0])


if __name__ == "__main__":
    unittest.main()


class MoveTo11(unittest.TestCase):
    def test_rounds_and_pick(self):
        self.assertEqual(fc.ROUNDS, ("11:00", "21:30"))
        self.assertEqual(fc.pick_round(pd.Timestamp("2026-11-12 11:03")), "11:00")
        self.assertEqual(fc.pick_round(pd.Timestamp("2026-11-12 21:40")), "21:30")

    def test_hindcast_detection(self):
        self.assertTrue(fc.is_hindcast({"asof": "2026-11-11 16:18", "filled_at": "2026-11-11 16:18"}))
        self.assertFalse(fc.is_hindcast({"asof": "2026-11-12 11:00", "filled_at": "2026-11-12 21:40"}))
        self.assertFalse(fc.is_hindcast({"asof": "2026-11-12 11:00", "filled_at": ""}))

    def test_summary_excludes_hindcast(self):
        rows = [{"card_id": "2026-11-11 21:30 B7", "asof": "2026-11-11 16:18", "filled_at": "2026-11-11 16:18", "call": "ลง", "right": 1, "outcome_cls": "ลง", "outcome_dws": -0.7},
                {"card_id": "2026-11-12 11:00 B7", "asof": "2026-11-12 11:00", "filled_at": "2026-11-12 21:40", "call": "ลง", "right": 0, "outcome_cls": "นิ่ง", "outcome_dws": 0.0}]
        text = "\n".join(fc.summary_lines(rows))
        self.assertIn("ไม่นับการ์ดย้อนหลัง 1 ใบ", text)
        self.assertIn("เติมผลแล้ว 1 ใบ", text)

    def test_drop_known_target(self):
        eppo = pd.DataFrame({"fuel": ["B7", "B7"], "day": [pd.Timestamp("2026-11-10"), pd.Timestamp("2026-11-11")], "ws": [40.0, 39.3]})
        known = {"card_id": "a", "fuel": "B7", "target_day": "2026-11-11", "call": "ลง"}
        future = {"card_id": "b", "fuel": "B7", "target_day": "2026-11-12", "call": "ลง"}
        self.assertEqual(fc.drop_known([known, future], eppo), [future])
