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
