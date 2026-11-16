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
        self.assertTrue(text.startswith("🛢 พยากรณ์ราคาน้ำมัน\n🧭 ควรซื้อเมื่อไร"))
        self.assertIn(L.SEP + "\n① ราคาส่ง (Jobber) · 24 ชม.ข้างหน้า", text)
        self.assertIn(L.SEP + "\n② ราคาขายปลีก (หน้าปั๊ม) · พรุ่งนี้ (13/11)", text)                 # ถามคืน 12/11 หลังการ์ดออก
        self.assertIn("ดีเซล (วันที่ 13/11): ➖ น่าจะไม่ปรับ\n⬇ ลง      ▰▱▱▱▱▱▱▱▱▱ 12%\n➖ ไม่ปรับ ▰▰▰▰▰▰▰▱▱▱ 70%\n⬆ ขึ้น     ▰▰▱▱▱▱▱▱▱▱ 18%", text)
        self.assertNotIn("คู่ขนาน", text)                                                    # ข้อความไลน์แสดงเฉพาะคำทายหลัก
        self.assertNotIn("โมเดลสัญญาณใช้ไม่ได้", text)                                        # แถวก่อนการสลับ ไม่ใช่กรณีโมเดลสัญญาณล้ม
        old = L.build_message(_inp(), _log(), on_demand=True, retail=_retail(model="OLD"))[0]
        self.assertIn("• คืนนี้โมเดลสัญญาณใช้ไม่ได้ จึงใช้วิธีเดิมเป็นคำทายหลัก", old)
        self.assertIn("• ปลีกล่าสุดใน EPPO 40.79\n", text)                                  # size_text ว่าง (NaN จากชีท) ไม่ถูกพิมพ์
        self.assertIn("แก๊สโซฮอล์ 95 (วันที่ 13/11): 📈 น่าจะปรับขึ้น", text)
        self.assertIn("• ปลีกล่าสุดใน EPPO 40.79 · ถ้าปรับ มัธยฐาน 0.60 บาท/ลิตร", text)
        self.assertIn("ผลจริงสะสม (ราคาปลีก): ดีเซล รู้ผล 1 วัน ฟันธง 1 ถูก 1", text)          # แถวที่ยังไม่มีผล (NaN) ไม่ถูกนับ
        self.assertNotIn("nan", text)
        # ผลงานของราคาส่งอยู่ในส่วนราคาส่ง ก่อนส่วนราคาปลีก — ไม่ปนกัน
        self.assertLess(text.index("ผลงานที่ผ่านมา (ราคาส่ง Jobber)"), text.index("② ราคาขายปลีก"))
        self.assertLess(text.index("ถูก ▰▰▰▰▰▰▰▱▱▱ 2/3"), text.index("② ราคาขายปลีก"))
        self.assertTrue(text.endswith(L.FOOTER))

    def test_signal_model_is_primary_when_logged(self):
        sig = _retail(model="SIG", call="ขึ้น", p_down=0.05, p_flat=0.60, p_up=0.35, old_p_down=0.12, old_p_flat=0.70, old_p_up=0.18, old_call="นิ่ง")
        text, _ = L.build_message(_inp(), _log(), on_demand=True, retail=sig)
        self.assertIn("ดีเซล (วันที่ 13/11): 📈 น่าจะปรับขึ้น\n⬇ ลง      ▰▱▱▱▱▱▱▱▱▱ 5%\n➖ ไม่ปรับ ▰▰▰▰▰▰▱▱▱▱ 60%\n⬆ ขึ้น     ▰▰▰▰▱▱▱▱▱▱ 35%", text)
        self.assertNotIn("โมเดลสัญญาณใช้ไม่ได้", text)

    def test_bars_and_jobber_probabilities(self):
        self.assertEqual((L.bar(0), L.bar(0.004), L.bar(0.62), L.bar(1.7)), ("▱" * 10, "▰" + "▱" * 9, "▰" * 6 + "▱" * 4, "▰" * 10))
        inp = _inp()
        inp["fuels"]["B7"]["h24"].update(p_up=0.62, p_down=0.08)
        text, _ = L.build_message(inp, _log())
        self.assertIn("ดีเซล: 📈 ขึ้น (คาดขยับ 10–40 สต./ลิตร)\n⬆ ขึ้น ▰▰▰▰▰▰▱▱▱▱ 62%\n⬇ ลง  ▰▱▱▱▱▱▱▱▱▱ 8%\nความเชื่อมั่น: กลาง", text)
        self.assertNotIn("①", text)                                                          # ข้อความประจำคืนมีส่วนเดียว ไม่ใส่เลขลำดับ
        self.assertIn("แก๊สโซฮอล์ 95: ❓ ไม่มีสัญญาณชัด\nราคาอาจขยับ", text)                  # ไม่มีค่าความน่าจะเป็น = ไม่มีแถบ
        d = {"DIESEL": {"fuel": "B7", "ref": pd.Timestamp("2026-11-12 20:30"), "horizons": [
            {"hours": 24, "tier": "UNCLEAR", "reasons": [], "p": {"r10": 0.31, "f10": 0.36, "r40": 0.1, "f40": 0.1}}]}}
        h = L.make_input(d, pd.Timestamp("2026-11-12 21:35"))["fuels"]["B7"]["h24"]
        self.assertEqual((h["p_up"], h["p_down"]), (0.31, 0.36))

    def test_nightly_message_has_no_retail(self):
        self.assertNotIn("ราคาขายปลีก", L.build_message(_inp(), _log())[0])
        self.assertNotIn("ราคาขายปลีก", L.build_message(_inp(), _log(), on_demand=True)[0])

    def test_stale_empty_and_outcome(self):
        late, _ = L.build_message(_inp(asof="2026-11-14 10:00", ref="2026-11-13 20:30"), _log(), on_demand=True, retail=_retail())
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี) ; ล่าสุดเป็นของวันที่ 13/11", late)
        self.assertNotIn("น่าจะ", late)
        self.assertIn("② ราคาขายปลีก (หน้าปั๊ม) · รอพยากรณ์ของวันถัดไป", late)
        empty, _ = L.build_message(_inp(), _log(), on_demand=True, retail=pd.DataFrame())
        self.assertIn("ดีเซล: ยังไม่มีพยากรณ์ของวันถัดไป (ออกหลัง 21:30 น. คืนจันทร์–พฤหัสบดี)\n", empty)
        today = _retail(call="ลง", outcome_dretail=-0.5, outcome_cls="ลง", filled_at="2026-11-13 11:05")
        done, _ = L.build_message(_inp(asof="2026-11-13 12:00", ref="2026-11-12 20:30"), _log(), on_demand=True, retail=today)
        self.assertIn("ดีเซล (วันที่ 13/11): 📉 น่าจะปรับลง", done)
        self.assertIn("② ราคาขายปลีก (หน้าปั๊ม) · วันนี้ (13/11)", done)                          # ถามกลางวัน 13/11 : เป็นพยากรณ์ของวันนี้ ไม่ใช่วันถัดไป
        self.assertIn("• ผลจริง: ปรับ -0.50 บาท/ลิตร ✅", done)
        miss = _retail(call="ขึ้น", outcome_dretail=0.0, outcome_cls="นิ่ง", filled_at="2026-11-13 11:05")
        self.assertIn("• ผลจริง: ไม่ปรับ ❌", L.build_message(_inp(asof="2026-11-13 12:00", ref="2026-11-12 20:30"), _log(), on_demand=True, retail=miss)[0])


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


def _headsup(*rows):
    import signals as S
    cols = ["msg_date", "msg_time", "provider", "direction", "scope", "stated_time", "has_rate", "rates"]
    return S.add_ts(pd.DataFrame([dict(zip(cols, ("2026-11-12",) + r)) for r in rows], columns=cols))


# ประกาศจริงของ 16/11/2026 (ย่อ) : ผู้ค้าโพสต์หลายข้อความต่อการปรับหนึ่งครั้ง
HEADSUP = (("9:12", "SFL", "DOWN", "ALL", "9:00", "N", None), ("12:18", "SFL", "DOWN", "ALL", "12:20", "N", None),
           ("15:21", "PTT", "UP", "ALL", None, "N", None), ("16:48", "PTT", "UP", "ALL", None, "Y", "DIESEL:+0.75"),
           ("17:30", None, "UP", "ALL", "0:00", "N", None), ("17:39", "IRPC", "UP", "ALL", None, "N", None),
           ("17:44", "IRPC", "UP", "ALL", None, "Y", "DIESEL:+1.00 GASOHOL:+1.00"), ("17:44", "IRPC", "UP", "ALL", "23:30", "N", None),
           ("22:10", "BC", "UP", "ALL", None, "N", None))                                   # โพสต์หลัง asof ต้องไม่ถูกใช้


class NoticeTest(unittest.TestCase):
    """ประกาศของผู้ค้าส่งเป็นข้อเท็จจริงที่รู้ก่อนพยากรณ์ : แสดงบนสุด พร้อมเรทและเวลามีผล"""

    def test_grouping_pending_and_lookahead(self):
        ns = {(n["provider"], n["dir"]): n for n in L.notices(_headsup(*HEADSUP), pd.Timestamp("2026-11-12 20:47"))}
        self.assertEqual(set(ns), {("SFL", "DOWN"), ("PTT", "UP"), ("", "UP"), ("IRPC", "UP")})            # BC 22:10 ยังไม่โพสต์ ณ 20:47
        ir = ns[("IRPC", "UP")]
        self.assertEqual((ir["n"], ir["rates"], ir["stated"], ir["at"], ir["pending"]), (3, "DIESEL:+1.00 GASOHOL:+1.00", "23:30", "17:44", True))
        self.assertEqual((ns[("SFL", "DOWN")]["n"], ns[("SFL", "DOWN")]["pending"]), (2, False))          # มีผลไปแล้วตอน 12:20
        self.assertTrue(ns[("", "UP")]["pending"])                                                        # 00:00 = เที่ยงคืนนี้ ยังไม่ถึง
        late = {(n["provider"], n["dir"]): n for n in L.notices(_headsup(*HEADSUP), pd.Timestamp("2026-11-12 23:45"))}
        self.assertFalse(late[("IRPC", "UP")]["pending"])                                                 # เลย 23:30 แล้ว
        self.assertEqual(L.notices(pd.DataFrame(), pd.Timestamp("2026-11-12 20:47")), [])
        self.assertEqual(L.parse_rates("DIESEL:+1.00 GASOHOL:-0.50"), {"DIESEL": 1.0, "GASOHOL": -0.5})

    def test_lines_and_pending_rise(self):
        d = {"DIESEL": {"fuel": "B7", "ref": pd.Timestamp("2026-11-12 20:30"), "horizons": []}}
        inp = L.make_input(d, pd.Timestamp("2026-11-12 20:47"), _headsup(*HEADSUP))
        text = "\n".join(L.notice_lines(inp))
        self.assertIn("📢 ประกาศของผู้ค้าส่งวันนี้ (ข้อเท็จจริง ไม่ใช่คำทาย)", text)
        self.assertIn("⬆ ไออาร์ (IRPC): ขึ้น ดีเซล +1.00 · แก๊สโซฮอล์ +1.00 · มีผล 23:30 น. (ประกาศ 17:44)", text)
        self.assertIn("⬆ ปตท.: ขึ้น ดีเซล +0.75 (ประกาศ 16:48)", text)
        self.assertIn("⬆ ไม่ระบุราย: ขึ้น ทุกผลิตภัณฑ์ · มีผล 00:00 น. (ประกาศ 17:30)", text)
        self.assertIn("⬇ ลงระหว่างวัน: สตาร์ฟูเอลส์ 2 ครั้ง", text)
        self.assertIn("(ข้อมูลถึง 20:47 น. · ไม่รวมข้อยกเว้นรายคลัง)", text)
        self.assertEqual(L.pending_rise(inp, "B7"), {"amt": 1.0, "when": "23:30"})                         # เรทสูงสุด และเวลามีผลที่เร็วที่สุด
        self.assertEqual(L.pending_rise(inp, "G95"), {"amt": 1.0, "when": "23:30"})
        self.assertIsNone(L.pending_rise({"notices": []}, "B7"))
        self.assertEqual(L.notice_lines(_inp()), [])                                                       # ไม่มีช่อง notices = ไม่แสดงส่วนนี้
        self.assertIn("ยังไม่มีประกาศปรับราคา", "\n".join(L.notice_lines({"asof": "2026-11-12 09:00", "notices": []})))


class AdviceTest(unittest.TestCase):
    """ควรซื้อเมื่อไร : ซื้อก่อนขึ้นคืนนี้ หรือรอบ่ายพรุ่งนี้ — ใช้เฉพาะหลักฐานที่ผ่านเกณฑ์ และไม่แนะนำเมื่อสัญญาณขัดกัน"""
    RISE = {"amt": 1.0, "when": "23:30"}

    def _f(self, tier="UNCLEAR", *profile):
        return {"ref": "2026-11-12 20:30", "h24": {"tier": tier, "grade": "กลาง"},
                "profile": [dict(zip(("hours", "at", "call", "ok", "n", "right"), p)) for p in profile]}

    def test_wait_buy_conflict_and_none(self):
        wait = L.advice_line(self._f("DOWN_STRONG", (20, "16:30", "DOWN", True, 163, 0.64)), self.RISE)
        self.assertTrue(wait.startswith("⏳ รอซื้อบ่ายพรุ่งนี้ (หลัง 16:00 น.)"))
        self.assertIn("• แนวโน้ม 24 ชม. ลงแรง (ความเชื่อมั่น กลาง)", wait)
        self.assertIn("• ราคา 16:30 น. พรุ่งนี้มักต่ำกว่าคืนนี้ (ย้อนหลังถูก 64% จาก 163 ครั้ง)", wait)
        self.assertIn("⚠ คืนนี้ราคาจะขึ้น +1.00 เวลา 23:30 น. ตามประกาศ", wait)
        self.assertIn("เลี่ยงซื้อช่วงเช้าหลังราคาขึ้น", wait)
        buy = L.advice_line(self._f("UP", (20, "16:30", "UP", True, 154, 0.68)), self.RISE)
        self.assertTrue(buy.startswith("🛒 ซื้อก่อน 23:30 น. คืนนี้ ก่อนราคาขึ้น +1.00"))
        self.assertTrue(L.advice_line(self._f("UP"), None).startswith("🛒 ซื้อเร็ว ไม่ควรรอถึงบ่ายพรุ่งนี้"))
        both = L.advice_line(self._f("UP_STRONG", (20, "16:30", "DOWN", True, 186, 0.69)), self.RISE)
        self.assertTrue(both.startswith("⚖ สัญญาณขัดกัน ยังไม่แนะนำ"))
        self.assertIn("ชี้ให้รีบ: แนวโน้ม 24 ชม. ขึ้นแรง", both)
        self.assertIn("ชี้ให้รอ: ราคา 16:30 น.", both)
        none = L.advice_line(self._f("UNCLEAR", (20, "16:30", "DOWN", False, 110, 0.56)), self.RISE)      # ทิศที่ไม่ผ่านเกณฑ์ไม่ถูกใช้
        self.assertTrue(none.startswith("❔ ยังบอกไม่ได้ว่าควรรีบหรือรอ"))
        self.assertIn("ถ้าต้องใช้น้ำมันก่อนบ่ายพรุ่งนี้ ซื้อก่อน 23:30 น. คืนนี้", none)
        self.assertEqual(L.advice_line(self._f(), None), "❔ ยังบอกไม่ได้ว่าควรรีบหรือรอ")
        noon = L.advice_line(self._f("UNCLEAR", (16, "12:30", "UP", True, 221, 0.75)), None)              # ขึ้นภายในเที่ยงอย่างเดียว = รีบแบบอ่อน
        self.assertIn("ราคามักขึ้นภายในเที่ยงพรุ่งนี้ (ย้อนหลังถูก 75% จาก 221 ครั้ง)", noon)
        noon_then_down = L.advice_line(self._f("UNCLEAR", (16, "12:30", "UP", True, 221, 0.75), (20, "16:30", "DOWN", True, 186, 0.69)), None)
        self.assertTrue(noon_then_down.startswith("⏳ รอซื้อบ่ายพรุ่งนี้"))                              # ขึ้นเช้าแล้วลงบ่าย = รอ

    def test_message_section(self):
        inp = _inp(b7="UNCLEAR", g95="DOWN_STRONG")
        inp["fuels"]["G95"]["profile"] = self._f("x", (20, "16:30", "DOWN", True, 163, 0.64))["profile"]
        inp["notices"] = L.notices(_headsup(*HEADSUP), pd.Timestamp(inp["asof"]))
        text, _ = L.build_message(inp, _log())
        self.assertTrue(text.startswith("🛢 พยากรณ์ราคาน้ำมัน\n📢 ประกาศของผู้ค้าส่งวันนี้"))
        self.assertLess(text.index("📢"), text.index("🧭 ควรซื้อเมื่อไร (คืนนี้ หรือ 13/11) [ทดลอง ยังไม่มีผลจริง]"))
        self.assertLess(text.index("🧭"), text.index("ราคาส่ง (Jobber) · 24 ชม.ข้างหน้า"))
        self.assertIn("ดีเซล: ❔ ยังบอกไม่ได้ว่าควรรีบหรือรอ\n   ⚠ คืนนี้ราคาจะขึ้น +1.00 เวลา 23:30 น. ตามประกาศ", text)
        self.assertIn("แก๊สโซฮอล์ 95: ⏳ รอซื้อบ่ายพรุ่งนี้ (หลัง 16:00 น.)", text)
        self.assertIn(L.AVG_PM_DISCOUNT, text)
        self.assertNotIn("เวลาซื้อวันที่", text)                                           # ส่วนเดิมถูกแทนด้วยส่วนนี้
        pm, _ = L.build_message(_inp(asof="2026-11-13 16:00", ref="2026-11-13 15:30"), _log(), on_demand=True)
        self.assertNotIn("🧭", pm)                                                       # รอบบ่ายไม่มีโมเดลรายช่วงเวลา
        none, _ = L.build_message(_inp(ref="2026-11-11 20:30"), _log())
        self.assertNotIn("🧭", none)                                                     # ไม่มีคำทาย → ไม่มีส่วนนี้


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
