import unittest

import numpy as np
import pandas as pd

import diag_wf_components as wf


def synth(n=120, seed=3):
    rng = np.random.default_rng(seed)
    days = pd.date_range("2026-03-16", periods=n)
    mops = np.cumsum(rng.normal(0, 1.5, n)) + 80
    mp = pd.DataFrame({"day": days, "fuel": "B7", "mops": mops})
    wti = pd.DataFrame({"day": days, "wti": np.cumsum(rng.normal(0, 0.8, n)) + 60})
    dmops = np.r_[0, np.diff(mops)]
    # ราคาโรงกลั่นวัน t+1 ขึ้นกับ MOPS Δ ของวัน t ; กองทุนเปลี่ยนแบบสุ่มเป็นช่วง ๆ
    ex = np.zeros(n)
    for t in range(1, n):
        ex[t] = ex[t - 1] + 0.12 * dmops[t - 1] + rng.normal(0, 0.03)
    fund = np.cumsum(np.where(rng.random(n) < 0.2, rng.normal(0, 0.5, n), 0.0))
    eppo = pd.DataFrame({"day": days, "fuel": "B7", "exref": ex + 30, "fund": fund - 2, "other": 7.0, "mm": 2.0})
    eppo["ws"] = eppo.exref + eppo.fund + eppo.other
    eppo["resid"] = 0.0
    eppo.loc[eppo.day >= pd.Timestamp("2026-05-01"), "resid"] = -4.0       # ส่วนลดภาษีที่ไม่อยู่ในคอลัมน์ ตั้งแต่ 1 พ.ค.
    eppo["ws"] = eppo.ws + eppo.resid
    return mp, wti, eppo


class Parse(unittest.TestCase):
    def test_components_sum_to_ws(self):
        h = ["Date", "oil type", "EX-REFIN.", "EXCISE TAX", "M. TAX", "OIL FUND", "CONSV. FUND", "WHOLESALE (WS)", "MARKETING MARGIN", "Source"]
        v = [h, ["10/1/2026", "GASOHOL95 E10", "29.3399", "6.75", "0.675", "-8.37", "0.05", "28.4449", "3.3", "FRESH"],
             ["10/2/2026", "GASOHOL95 E10", "29.3399", "6.75", "0.675", "-8.37", "0.05", "28.4449", "3.3", "CARRIED"]]
        e = wf.eppo_components(v)
        self.assertEqual(len(e), 1)
        self.assertAlmostEqual(float((e.ws - e.exref - e.fund - e.other).abs().iloc[0]), 0.0, places=3)


class Walk(unittest.TestCase):
    def test_dataset_and_no_lookahead(self):
        mops, wti, eppo = synth()
        ds = wf.build_dataset(mops, wti, eppo)
        self.assertGreater(len(ds), 100)
        r = ds.iloc[10]
        e = eppo.set_index("day")
        self.assertAlmostEqual(r.dws, e.loc[r.day + pd.Timedelta(days=1), "ws"] - e.loc[r.day, "ws"])
        out = wf.walk_forward(ds, min_train=40)
        # วันแรกที่ทดสอบต้องมีวันฝึก >= 40 วันก่อนหน้า (<= t-2)
        self.assertGreaterEqual((out.day.min() - ds.day.min()).days, 41)

    def test_known_fund_beats_blind_and_report(self):
        mops, wti, eppo = synth()
        ds = wf.build_dataset(mops, wti, eppo)
        out = wf.walk_forward(ds, min_train=40)
        mae = {m: float((out[m] - out.actual).abs().mean()) for m in wf.MODELS}
        self.assertLess(mae["B"], mae["A"])                 # รู้กองทุนที่ประกาศแล้ว → ดีกว่า
        self.assertLess(mae["B"], mae["ZERO"])
        lines = wf.report(ds, out)
        self.assertTrue(any("[WF-VAR]" in x for x in lines))
        self.assertTrue(any(" B " in x or x.strip().startswith("B ") for x in lines))

    def test_models_c_cr_and_class_report(self):
        mops, wti, eppo = synth()
        ds = wf.build_dataset(mops, wti, eppo)
        out = wf.walk_forward(ds, min_train=40)
        self.assertIn("C", out.columns)
        mae = {m: float((out[m] - out.actual).abs().mean()) for m in ("A", "C", "CR")}
        self.assertLessEqual(mae["C"], mae["A"] + 1e-9)      # เรียนรู้น้ำหนักกองทุนเอง ไม่แย่กว่าไม่รู้
        lines = wf.class_report(out)
        self.assertIn("[WF-CLASS]", lines[0])
        self.assertTrue(any(x.strip().startswith("C ") for x in lines))

    def test_identity_and_notice_reports(self):
        mops, wti, eppo = synth()
        idl = wf.identity_report(eppo)
        self.assertIn("แถวที่สมการไม่ตรง", idl[0])
        self.assertTrue(any("-4.00" in x for x in idl))                   # ช่วงที่ residual = -4
        n = pd.DataFrame({"msg_day": pd.to_datetime(["2026-04-21", "2026-04-22"]), "eff_day": pd.to_datetime(["2026-04-22", "2026-04-23"]),
                          "msg_hour": [19.3, 20.9], "diesel_new": [-3.2, -2.88]})
        e = eppo.copy()
        e.loc[e.day == pd.Timestamp("2026-04-22"), "fund"] = -3.2
        e.loc[e.day == pd.Timestamp("2026-04-23"), "fund"] = -2.0
        out = wf.notice_report(n, e)
        self.assertIn("ประกาศ 2 ฉบับ", out[0])
        self.assertIn("1/2", out[1])

    def test_too_little_data(self):
        mops, wti, eppo = synth(n=30)
        ds = wf.build_dataset(mops, wti, eppo)
        self.assertTrue(wf.walk_forward(ds).empty)
        self.assertTrue(any("ข้อมูลไม่พอ" in x for x in wf.report(ds, wf.walk_forward(ds))))


if __name__ == "__main__":
    unittest.main()
