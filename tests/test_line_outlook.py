"""ข้อความไลน์ส่วนตัว (ช่วงทดสอบ) : เนื้อหาถูกต้อง ส่งได้เฉพาะรหัสผู้ใช้ส่วนตัว ส่งครั้งเดียวต่อคืน และไม่ส่งผิดเวลา"""
import os
import sys
import unittest
from unittest import mock

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import line_outlook as L  # noqa: E402

UID = "U" + "0123456789abcdef" * 2


def _inp(b7="UP", g95="UNCLEAR", asof="2026-11-12 21:35", ref="2026-11-12 20:30", **kw):
    def fuel(tier):
        return {"ref": ref, "source": "E", "stale": False, "note": None,
                "h24": {"tier": tier, "grade": "กลาง", "n": 105, "right10": 0.61, "wrong10": 0.11,
                        "reasons": ["MOPS ปิดล่าสุด +2.20 USD/bbl → เสี่ยงราคาขึ้น", "2 ราย ประกาศ 'มีปรับขึ้น' วันนี้ → เสี่ยงราคาขึ้น",
                                    "ราคาดีเซล ขยับ -0.24 บาท/ลิตร ใน 3 วัน → โน้มราคาลง"]}}
    d = {"asof": asof, "fuels": {"B7": fuel(b7), "G95": fuel(g95)}}
    d["fuels"].update(kw)
    return d


def _log():
    rows = [["2026-11-10 20:45", "2026-11-10", "2026-11-10 20:30", "B7", 24, "UP_STRONG", 0.34, 0],
            ["2026-11-10 20:45", "2026-11-10", "2026-11-10 20:30", "G95", 24, "DOWN", -0.02, 0],
            ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "B7", 24, "UP", 0.86, 0],
            ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "G95", 24, "UNCLEAR", 0.63, 0],
            ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "B7", 48, "DOWN", 0.90, 0],          # 48 ชม. ไม่นับ
            ["2026-11-11 15:45", "2026-11-11", "2026-11-11 15:30", "B7", 24, "DOWN", 0.90, 0],          # รอบ 15:30 ไม่นับ
            ["2026-11-12 20:45", "2026-11-12", "2026-11-12 20:30", "B7", 24, "UP", "", 0]]              # ยังไม่มีผล
    return pd.DataFrame(rows, columns=["asof", "date", "ref", "fuel", "hours", "tier", "outcome_delta", "backfilled"])


class MessageTest(unittest.TestCase):
    def test_call_and_unclear(self):
        text, ref_id = L.build_message(_inp(), _log())
        self.assertEqual(ref_id, "2026-11-12 20:30")
        self.assertIn("ข้อมูล 20:30 น. 12/11 · ใช้ได้ถึง 20:30 น. 13/11", text)
        self.assertIn("ดีเซล: 📈 ขึ้น (คาดขยับ 10–40 สต./ลิตร)", text)
        self.assertIn("• MOPS ปิดล่าสุด +2.20 USD/bbl", text)
        self.assertNotIn("→", text.split("ผลของคำทาย")[0])                        # ตัดคำลงท้ายของเหตุผล และไม่ใส่เหตุผลทิศตรงข้าม
        self.assertNotIn("ราคาดีเซล ขยับ -0.24", text.split("แก๊สโซฮอล์ 95")[0])
        self.assertIn("ย้อนหลังเมื่อระบบบอกแบบนี้ (105 ครั้ง): ถูก 61% ผิดทิศ 11%", text)
        self.assertIn("แก๊สโซฮอล์ 95: ❓ ไม่มีสัญญาณชัด", text)
        self.assertIn("ราคาอาจขยับได้ทั้งสองทาง", text)
        self.assertIn("ผลของคำทายคืน 11/11", text)
        self.assertIn("ดีเซล: ทายขึ้น → จริง +0.86 ✅", text)
        self.assertIn("แก๊สโซฮอล์ 95: ไม่ได้ทาย → จริง +0.63", text)
        self.assertIn("สถิติผลจริงตั้งแต่ 10/11 (2/40 วัน)", text)
        self.assertIn("ฟันธง 3 ครั้ง · ถูก 2 · ผิดทิศ 0 · ราคานิ่ง 1", text)       # 48 ชม. และรอบ 15:30 ไม่ถูกนับ
        self.assertTrue(text.endswith(L.FOOTER))
        self.assertNotIn("48", text)

    def test_strong_wrong_and_both_unclear(self):
        text, _ = L.build_message(_inp(b7="DOWN_STRONG", g95="UNCLEAR"), _log())
        self.assertIn("ดีเซล: 📉📉 ลงแรง (คาดขยับตั้งแต่ 40 สต./ลิตร)", text)
        self.assertIn("• ราคาดีเซล ขยับ -0.24 บาท/ลิตร ใน 3 วัน", text)
        log = _log()
        log.loc[2, ["tier", "outcome_delta"]] = ["DOWN", 0.86]
        self.assertIn("ทายลง → จริง +0.86 ❌", L.build_message(_inp(), log)[0])
        both, _ = L.build_message(_inp(b7="UNCLEAR", g95="UNCLEAR"), _log())
        self.assertEqual(both.count("❓ ไม่มีสัญญาณชัด"), 2)
        self.assertIn("สัญญาณคืนนี้", both)
        self.assertIn("• ชี้ขึ้น: MOPS ปิดล่าสุด +2.20 USD/bbl", both)
        self.assertIn("• ชี้ลง:", both)

    def test_no_forecast_cases(self):
        stale, _ = L.build_message(_inp(ref="2026-11-11 20:30"), _log())                  # ยังเป็นข้อมูลของเมื่อคืน
        self.assertIn("คืนนี้ (12/11) ไม่มีคำทาย", stale)
        self.assertIn("ยังไม่มีข้อมูลรอบ 20:30 ของวันนี้", stale)
        self.assertNotIn("📈", stale)
        pm, _ = L.build_message(_inp(ref="2026-11-12 15:30"), _log())
        self.assertNotIn("📈", pm)
        none, _ = L.build_message(_inp(B7={"ref": "2026-11-12 20:30", "stale": False, "note": "ฟีดสัญญาณจากไลน์ไม่อัปเดต", "h24": None}), _log())
        self.assertIn("ดีเซล: ไม่มีคำทาย — ฟีดสัญญาณจากไลน์ไม่อัปเดต", none)
        empty, _ = L.build_message(_inp(), pd.DataFrame())
        self.assertIn("ยังไม่มีผลจริง", empty)

    def test_on_demand_uses_latest_unexpired_forecast(self):
        day = _inp(asof="2026-11-13 09:10", ref="2026-11-12 20:30")                       # ถามตอนเช้า : ใช้คำทายของเมื่อคืน
        self.assertIn("ไม่มีคำทาย", L.build_message(day, _log())[0])                       # ข้อความประจำคืนยังเข้มเหมือนเดิม
        text, _ = L.build_message(day, _log(), on_demand=True)
        self.assertIn("ข้อมูล 20:30 น. 12/11 · ใช้ได้ถึง 20:30 น. 13/11", text)
        self.assertIn("ดีเซล: 📈 ขึ้น", text)
        pm, _ = L.build_message(_inp(asof="2026-11-13 16:00", ref="2026-11-13 15:30"), _log(), on_demand=True)
        self.assertIn("ข้อมูล 15:30 น. 13/11", pm)
        self.assertIn("รอบบ่าย", pm)
        old, _ = L.build_message(_inp(asof="2026-11-14 21:00", ref="2026-11-12 20:30"), _log(), on_demand=True)
        self.assertIn("ตอนนี้ ไม่มีคำทาย", old)
        self.assertIn("หมดอายุแล้ว", old)

    def test_make_input_from_engine_shape(self):
        d = {"GASOHOL95": {"fuel": "G95", "ref": pd.Timestamp("2026-11-12 20:30"), "source": "E", "stale": False, "note": None,
                           "horizons": [{"hours": 48, "tier": "UP", "reasons": []},
                                        {"hours": 24, "tier": "DOWN", "grade": "สูง", "reasons": ["x → โน้มราคาลง"], "evidence": {"n": 129, "right10": 0.61, "wrong10": 0.06}}]},
             "DIESEL": {"fuel": "B7", "ref": pd.Timestamp("2026-11-12 20:30"), "note": "ยังไม่มีโมเดล", "horizons": []}}
        inp = L.make_input(d, pd.Timestamp("2026-11-12 21:35"))
        self.assertEqual(inp["fuels"]["G95"]["h24"]["tier"], "DOWN")
        self.assertIsNone(inp["fuels"]["B7"]["h24"])
        text, _ = L.build_message(inp, pd.DataFrame())
        self.assertIn("แก๊สโซฮอล์ 95: 📉 ลง", text)
        self.assertIn("ดีเซล: ไม่มีคำทาย — ยังไม่มีโมเดล", text)


def _retail(target="2026-11-13", **kw):
    import procurement_summary as PS
    def row(fuel, **o):
        r = {"card_id": f"{target} 21:30 {fuel}", "asof": "2026-11-12 21:40", "round": "21:30", "fuel": fuel, "target_day": target, "retail_base": 40.79,
             "p_down": 0.12, "p_flat": 0.70, "p_up": 0.18, "call": "นิ่ง", "size_text": float("nan"), "outcome_dretail": float("nan"),
             "outcome_cls": float("nan"), "filled_at": float("nan"), "sig_p_down": 0.05, "sig_p_flat": 0.60, "sig_p_up": 0.35}
        r.update(o)
        return r
    old = {"card_id": "2026-11-11 21:30 B7", "asof": "2026-11-10 21:40", "target_day": "2026-11-11", "call": "ลง", "outcome_dretail": -1.0,
           "outcome_cls": "ลง", "filled_at": "2026-11-11 11:05"}
    return PS.prep_retail(pd.DataFrame([row("B7", **old), row("B7", **kw), row("G95", call="ขึ้น", p_down=0.1, p_flat=0.4, p_up=0.5,
                                                                              size_text="ถ้าปรับ มัธยฐาน 0.60 บาท/ลิตร (ช่วงที่พบบ่อย 0.50–1.00)")]))


class RetailTest(unittest.TestCase):
    """ข้อความตอบคำสั่ง forecast มีพยากรณ์ราคาขายปลีก ; ข้อความประจำคืนไม่มี"""

    def test_on_demand_includes_retail(self):
        text, _ = L.build_message(_inp(), _log(), on_demand=True, retail=_retail())
        self.assertIn("⛽ ราคาขายปลีก [ทดลอง", text)
        self.assertIn("ดีเซล (วันที่ 13/11): ➖ น่าจะไม่ปรับ", text)
        self.assertIn("• โอกาส ลง 12% / ไม่ปรับ 70% / ขึ้น 18%", text)
        self.assertIn("• โมเดลสัญญาณ (คู่ขนาน): ลง 5% / ไม่ปรับ 60% / ขึ้น 35%", text)
        self.assertIn("• ปลีกล่าสุดใน EPPO 40.79\n", text)                                  # size_text ว่าง (NaN จากชีท) ไม่ถูกพิมพ์
        self.assertIn("แก๊สโซฮอล์ 95 (วันที่ 13/11): 📈 น่าจะปรับขึ้น", text)
        self.assertIn("• ปลีกล่าสุดใน EPPO 40.79 · ถ้าปรับ มัธยฐาน 0.60 บาท/ลิตร", text)
        self.assertIn("ผลจริงสะสม: ดีเซล รู้ผล 1 วัน ฟันธง 1 ถูก 1", text)                    # แถวที่ยังไม่มีผล (NaN) ไม่ถูกนับ
        self.assertNotIn("nan", text)
        self.assertLess(text.index("⛽"), text.index("ผลของคำทาย"))
        self.assertTrue(text.endswith(L.FOOTER))

    def test_nightly_message_has_no_retail(self):
        self.assertNotIn("⛽", L.build_message(_inp(), _log())[0])
        self.assertNotIn("⛽", L.build_message(_inp(), _log(), on_demand=True)[0])

    def test_stale_empty_and_outcome(self):
        late, _ = L.build_message(_inp(asof="2026-11-14 10:00", ref="2026-11-13 20:30"), _log(), on_demand=True, retail=_retail())
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี) ; ล่าสุดเป็นของวันที่ 13/11", late)
        self.assertNotIn("น่าจะ", late)
        empty, _ = L.build_message(_inp(), _log(), on_demand=True, retail=pd.DataFrame())
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี)\n", empty)
        today = _retail(call="ลง", outcome_dretail=-0.5, outcome_cls="ลง", filled_at="2026-11-13 11:05")
        done, _ = L.build_message(_inp(asof="2026-11-13 12:00", ref="2026-11-12 20:30"), _log(), on_demand=True, retail=today)
        self.assertIn("ดีเซล (วันที่ 13/11): 📉 น่าจะปรับลง", done)
        self.assertIn("• ผลจริง: ปรับ -0.50 บาท/ลิตร ✅", done)
        miss = _retail(call="ขึ้น", outcome_dretail=0.0, outcome_cls="นิ่ง", filled_at="2026-11-13 11:05")
        self.assertIn("• ผลจริง: ไม่ปรับ ❌", L.build_message(_inp(asof="2026-11-13 12:00", ref="2026-11-12 20:30"), _log(), on_demand=True, retail=miss)[0])


def _cards(target="2026-11-13", **kw):
    import procurement_summary as PS
    def row(fuel, **o):
        r = {"card_id": f"{target} 21:30 {fuel}", "asof": "2026-11-12 21:40", "fuel": fuel, "target_day": target, "lo": -0.05, "hi": 0.45,
             "p_down": 0.08, "p_flat": 0.32, "p_up": 0.60, "call": "ขึ้น", "confidence": "กลาง", "outcome_dws": float("nan"),
             "outcome_cls": float("nan"), "filled_at": float("nan")}
        r.update(o)
        return r
    old = {"card_id": "2026-11-11 21:30 B7", "asof": "2026-11-10 21:40", "target_day": "2026-11-11", "call": "ลง", "outcome_dws": -0.93,
           "outcome_cls": "ลง", "filled_at": "2026-11-11 11:05"}
    return PS.prep_cards(pd.DataFrame([row("B7", **old), row("B7", **kw), row("G95", call="ไม่ชัด", confidence=float("nan"))]))


class WholesaleTest(unittest.TestCase):
    """ข้อความตอบคำสั่ง forecast มีทั้งราคาขายส่ง EPPO และราคาขายปลีก"""

    def test_on_demand_includes_wholesale_then_retail(self):
        text, _ = L.build_message(_inp(), _log(), on_demand=True, retail=_retail(), cards=_cards())
        self.assertIn("🏭 ราคาขายส่ง EPPO [ทดลอง", text)
        self.assertIn("ดีเซล (วันที่ 13/11): 📈 น่าจะขึ้น · ความเชื่อมั่น กลาง", text)
        self.assertIn("• โอกาส ลง 8% / นิ่ง 32% / ขึ้น 60%", text)
        self.assertIn("• คาดเปลี่ยน -0.05 ถึง +0.45 บาท/ลิตร", text)
        self.assertIn("แก๊สโซฮอล์ 95 (วันที่ 13/11): ❓ ยังไม่ชัดเจน\n", text)
        self.assertIn("ผลจริงสะสม: ดีเซล รู้ผล 1 วัน ฟันธง 1 ถูก 1\n\n⛽", text)
        self.assertLess(text.index("🏭"), text.index("⛽"))
        self.assertNotIn("nan", text)

    def test_stale_outcome_and_nightly(self):
        self.assertNotIn("🏭", L.build_message(_inp(), _log())[0])
        late, _ = L.build_message(_inp(asof="2026-11-14 10:00", ref="2026-11-13 20:30"), _log(), on_demand=True, cards=_cards())
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี) ; ล่าสุดเป็นของวันที่ 13/11", late)
        done = _cards(outcome_dws=0.0, outcome_cls="นิ่ง", filled_at="2026-11-13 11:05")
        self.assertIn("• ผลจริง: +0.00 บาท/ลิตร ❌", L.build_message(_inp(asof="2026-11-13 12:00", ref="2026-11-12 20:30"), _log(), on_demand=True, cards=done)[0])
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์", L.build_message(_inp(), _log(), on_demand=True, cards=pd.DataFrame())[0])


class SheetReadTest(unittest.TestCase):
    """OutlookLog อ่านจากชีทแบบ UNFORMATTED_VALUE : วันที่/เวลาเป็นเลขลำดับวัน (พบจริงตอน dry-run 16/11/2026 : สถิติว่างทั้งที่มีผล)"""

    def test_serial_dates_are_converted(self):
        from datetime import datetime
        from common import EXCEL_EPOCH
        ser = lambda s: (datetime.strptime(s, "%Y-%m-%d %H:%M") - EXCEL_EPOCH).total_seconds() / 86400
        head = ["asof", "date", "ref", "fuel", "hours", "tier", "outcome_delta", "backfilled"]
        values = [head,
                  [ser("2026-11-11 20:45"), ser("2026-11-11 00:00"), ser("2026-11-11 20:30"), "B7", 24, "UP", 0.86, 0],
                  ["2026-11-11 20:45", "2026-11-11", "2026-11-11 20:30", "G95", 24, "UNCLEAR", 0.63],          # แถวสั้น + เป็นข้อความ
                  [ser("2026-11-12 20:45"), ser("2026-11-12 00:00"), ser("2026-11-12 20:30"), "B7", 24, "UP", "", 0]]
        df = L.log_frame(values)
        self.assertEqual(list(df.ref), ["2026-11-11 20:30", "2026-11-11 20:30", "2026-11-12 20:30"])
        self.assertEqual(df.date[0], "2026-11-11")
        text, _ = L.build_message(_inp(), df)
        self.assertIn("ดีเซล: ทายขึ้น → จริง +0.86 ✅", text)
        self.assertIn("ฟันธง 1 ครั้ง · ถูก 1", text)
        self.assertTrue(L.log_frame([]).empty)


class SendTest(unittest.TestCase):
    def test_only_personal_user_id(self):
        self.assertTrue(L.valid_user_id(UID))
        for bad in ("C" + UID[1:], "R" + UID[1:], UID[:-1], "", None, UID.upper()):
            self.assertFalse(L.valid_user_id(bad), bad)

    def test_window_and_dedupe(self):
        asof = pd.Timestamp("2026-11-12 21:35")
        self.assertTrue(L.in_send_window(pd.Timestamp("2026-11-12 21:36"), asof))
        self.assertFalse(L.in_send_window(pd.Timestamp("2026-11-13 04:30"), pd.Timestamp("2026-11-13 04:29")))   # รอบที่ GitHub เลื่อนมาเช้ามืด
        self.assertFalse(L.in_send_window(pd.Timestamp("2026-11-12 11:00"), pd.Timestamp("2026-11-12 11:00")))
        rows = [L.LOG_COLUMNS, ["2026-11-11 20:30", "x", "sent", "t"], ["2026-11-12 20:30", "x", "failed:500", "t"]]
        self.assertTrue(L.already_sent(rows, "2026-11-11 20:30"))
        self.assertFalse(L.already_sent(rows, "2026-11-12 20:30"))                 # ส่งไม่สำเร็จ → ลองใหม่ได้

    def test_push_payload_and_failure(self):
        with mock.patch("line_outlook.requests.post", return_value=mock.Mock(status_code=200)) as post:
            self.assertEqual(L.push("hi", "tok", UID), "sent")
        self.assertEqual(post.call_args.kwargs["json"], {"to": UID, "messages": [{"type": "text", "text": "hi"}]})
        self.assertTrue(post.call_args.args[0].endswith("/v2/bot/message/push"))
        with mock.patch("line_outlook.requests.post", return_value=mock.Mock(status_code=401)):
            self.assertEqual(L.push("hi", "tok", UID), "failed:401")
        with mock.patch("line_outlook.requests.post", side_effect=L.requests.ConnectionError("x")):
            self.assertEqual(L.push("hi", "tok", UID), "failed:ConnectionError")

    def _run(self, env, now="2026-11-12 21:36", sent_rows=None, argv=()):
        import json
        import tempfile
        calls = {"append": [], "post": 0}
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "in.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(_inp(), fh, ensure_ascii=False)
            def post(*a, **k):
                calls["post"] += 1
                return mock.Mock(status_code=200)
            fake_now = mock.Mock(wraps=L.datetime)
            fake_now.now.return_value = pd.Timestamp(now).to_pydatetime()
            with mock.patch.object(L, "INPUT_FILE", path), mock.patch.object(L, "get_token", return_value="t"), \
                    mock.patch.object(L, "sheets_get", side_effect=lambda tok, sid, rng: (sent_rows or [L.LOG_COLUMNS]) if rng.startswith(L.LOG_TAB) else []), \
                    mock.patch.object(L, "ensure_tab", return_value=False), mock.patch.object(L, "sheets_update"), \
                    mock.patch.object(L, "sheets_append", side_effect=lambda *a: calls["append"].append(a[3])), \
                    mock.patch.object(L, "datetime", fake_now), mock.patch("line_outlook.requests.post", side_effect=post), \
                    mock.patch.dict(os.environ, env, clear=False):
                for k in ("LINE_CHANNEL_ACCESS_TOKEN", "LINE_TEST_USER_ID"):
                    if k not in env:
                        os.environ.pop(k, None)
                self.assertEqual(L.main(list(argv)), 0)
        return calls

    def test_main_gates(self):
        ok = {"LINE_CHANNEL_ACCESS_TOKEN": "tok", "LINE_TEST_USER_ID": UID}
        c = self._run(ok)
        self.assertEqual(c["post"], 1)
        self.assertEqual(c["append"][0][0][0], "2026-11-12 20:30")
        self.assertEqual(c["append"][0][0][2], "sent")
        self.assertEqual(self._run({})["post"], 0)                                                            # ไม่มี Secret
        self.assertEqual(self._run({**ok, "LINE_TEST_USER_ID": "C" + UID[1:]})["post"], 0)                    # รหัสกลุ่ม
        self.assertEqual(self._run(ok, argv=("--dry",))["post"], 0)
        self.assertEqual(self._run(ok, now="2026-11-13 04:30")["post"], 0)                                    # นอกช่วงเวลา
        self.assertEqual(self._run(ok, sent_rows=[L.LOG_COLUMNS, ["2026-11-12 20:30", "x", "sent"]])["post"], 0)  # ส่งแล้ว


if __name__ == "__main__":
    unittest.main()


class GeneratedTimeTest(unittest.TestCase):
    """outlook.json ช่อง generated ต้องเป็นเวลาไทย (เครื่องรันของ GitHub เป็นเวลาสากล : เคยแสดงช้าไป 7 ชม.)"""

    def test_generated_is_bangkok_time(self):
        import json
        import tempfile
        from datetime import datetime, timezone
        import main
        with tempfile.TemporaryDirectory() as tmp:
            pub = os.path.join(tmp, "o.json")
            with mock.patch.dict(os.environ, {"TZ": "UTC"}):
                main.write_outlook_files({}, snap_path=os.path.join(tmp, "s.json"), pub_path=pub)
            with open(pub, encoding="utf-8") as fh:
                gen = pd.Timestamp(json.load(fh)["generated"])
        bkk = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None)) + pd.Timedelta(hours=7)
        self.assertLess(abs((gen - bkk).total_seconds()), 180)


class TimingProfileTest(unittest.TestCase):
    """ช่วงเวลาซื้อของวันถัดไป : ใช้เฉพาะทิศที่ผ่านเกณฑ์ walk-forward และบอกวันที่ชัดเจน"""

    def _f(self, *profile):
        return {"ref": "2026-11-12 20:30", "profile": [dict(zip(("hours", "at", "call", "ok", "n", "right"), p)) for p in profile]}

    def test_priority_and_ok_filter(self):
        up_noon = self._f((16, "12:30", "UP", True, 221, 0.75), (20, "16:30", "DOWN", True, 186, 0.69))
        self.assertIn("ขึ้นภายในเที่ยงวันที่ 13/11", L.timing_line(up_noon))
        self.assertIn("ถูก 75% จาก 221 ครั้ง", L.timing_line(up_noon))
        down_pm = self._f((16, "12:30", "DOWN", False, 102, 0.56), (20, "16:30", "DOWN", True, 186, 0.69))    # ลงภายในเที่ยงไม่ผ่านเกณฑ์ → ไม่ใช้
        self.assertIn("ลงภายใน 16:30 น. วันที่ 13/11", L.timing_line(down_pm))
        self.assertIn("การรอไม่ช่วย", L.timing_line(self._f((16, "12:30", None, False, None, None), (20, "16:30", "UP", True, 154, 0.68))))
        self.assertEqual(L.timing_line(self._f((16, "12:30", "DOWN", False, 102, 0.56), (20, "16:30", "UP", False, 110, 0.63))), "")
        self.assertEqual(L.timing_line({"ref": "2026-11-12 20:30"}), "")

    def test_message_section(self):
        inp = _inp()
        inp["fuels"]["B7"]["profile"] = self._f((16, "12:30", "UP", True, 221, 0.75))["profile"]
        text, _ = L.build_message(inp, _log())
        self.assertIn("เวลาซื้อวันที่ 13/11 [ทดลอง ยังไม่มีผลจริง]", text)
        self.assertIn("⏰ ดีเซล: ราคามักขึ้นภายในเที่ยงวันที่ 13/11", text)
        self.assertIn(L.AVG_PM_DISCOUNT, text)
        plain, _ = L.build_message(_inp(), _log())                                   # ไม่มีสัญญาณเรื่องเวลา : ยังบอกข้อเท็จจริงเฉลี่ย
        self.assertIn("ไม่มีสัญญาณเรื่องเวลา", plain)
        pm, _ = L.build_message(_inp(asof="2026-11-13 16:00", ref="2026-11-13 15:30"), _log(), on_demand=True)
        self.assertNotIn("เวลาซื้อวันที่", pm)                                         # รอบบ่ายไม่มีส่วนนี้
        none, _ = L.build_message(_inp(ref="2026-11-11 20:30"), _log())
        self.assertNotIn("เวลาซื้อวันที่", none)                                       # ไม่มีคำทาย → ไม่มีส่วนนี้


class ProfileModelTest(unittest.TestCase):
    """โมเดลรายช่วงเวลาที่ใช้งานจริง + engine.decide_profile"""

    @classmethod
    def setUpClass(cls):
        import json
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "model_params.json"), encoding="utf-8") as fh:
            cls.params = json.load(fh)
        with open(os.path.join(root, "backtest_summary.json"), encoding="utf-8") as fh:
            cls.summary = json.load(fh)

    def test_shipped_profile_models_and_gates(self):
        import model as M
        prof, res = self.params["outlook"]["profile"], self.summary["profile_results"]
        self.assertEqual(set(prof), {f"{f}_T{h}" for f in ("B7", "G95") for h in M.OUTLOOK_PROFILE_HOURS})
        for key, mm in prof.items():
            self.assertEqual(len(mm["r10"]["w"]), len(M.FEATURES_V2))
            for d in ("up", "down"):
                st = res[key][d]
                want = st["n"] >= M.PROFILE_MIN_N and st["right"] >= M.PROFILE_MIN_RIGHT and st["wrong"] <= M.PROFILE_MAX_WRONG and min(st["right_h1"], st["right_h2"]) >= 0.55
                self.assertEqual(st["ok"], bool(want), key + d)
        self.assertFalse(res["B7_T16"]["down"]["ok"])            # "ลงภายในเที่ยง" ไม่ผ่าน (ถูก 56%) ต้องไม่ถูกใช้
        self.assertTrue(res["B7_T16"]["up"]["ok"] and res["B7_T20"]["down"]["ok"] and res["G95_T20"]["down"]["ok"])

    def test_decide_profile_gating(self):
        import engine as E
        import signals as S
        prices = pd.DataFrame([["A", "x", "B7", 39.0, "2026-11-10 05:00", "2026-11-09 17:00"], ["A", "x", "B7", 39.6, "2026-11-11 05:00", "2026-11-10 18:00"],
                               ["B", "y", "B7", 39.2, "2026-11-10 05:00", "2026-11-09 17:10"]], columns=["provider", "warehouse", "fuel", "price", "effective", "posted"])
        ref = pd.Timestamp("2026-11-10 20:30")
        cfg = self.params["outlook"]
        got = E.decide_profile("B7", ref, "E", S.Tables().set_prices(prices), cfg, self.summary)
        self.assertEqual([g["hours"] for g in got], [16, 20])
        self.assertEqual(str(got[0]["at"]), "2026-11-11 12:30:00")
        for g in got:
            self.assertTrue(0 <= g["p_up"] <= 1 and 0 <= g["p_down"] <= 1)
            self.assertEqual(g["ok"], bool(g["call"]) and self.summary["profile_results"][f"B7_T{g['hours']}"][{"UP": "up", "DOWN": "down"}[g["call"]]]["ok"] if g["call"] else False)
        self.assertEqual(E.decide_profile("B7", ref, "P", S.Tables().set_prices(prices), cfg, self.summary), [])     # รอบบ่ายไม่ใช้
        self.assertEqual(E.decide_profile("B7", ref, "E", S.Tables(), cfg, self.summary), [])                        # ไม่มีฟีดราคา
        self.assertEqual(E.decide_profile("B7", ref, "E", S.Tables().set_prices(prices), {}, self.summary), [])      # ไม่มีโมเดล

    def test_snapshot_rows_for_log(self):
        import json
        import tempfile
        import main
        d = {"fuel": "B7", "asof": pd.Timestamp("2026-11-10 21:35"), "ref": pd.Timestamp("2026-11-10 20:30"), "horizons": [],
             "profile": [{"hours": 16, "call": "UP", "p_up": 0.7, "p_down": 0.1, "ok": True}, {"hours": 20, "call": None, "p_up": 0.3, "p_down": 0.3, "ok": False}]}
        with tempfile.TemporaryDirectory() as tmp:
            snap = os.path.join(tmp, "s.json")
            main.write_outlook_files({"DIESEL": d}, "v", snap_path=snap, pub_path=os.path.join(tmp, "o.json"))
            rows = json.load(open(snap, encoding="utf-8"))
        self.assertEqual([(r["hours"], r["tier"], r["p_rise10"]) for r in rows], [(16, "UP", 0.7), (20, "UNCLEAR", 0.3)])
