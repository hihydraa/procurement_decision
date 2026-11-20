"""การ์ด Flex จากข้อมูลจริง : โครงสร้างถูกตามสเปก LINE , ตัวเลขตรงกับข้อมูล , สีหมวด , ขนาดไม่เกินเพดาน"""
import json
import os
import sys
import unittest

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import line_flex as F  # noqa: E402
import line_outlook as L  # noqa: E402
import test_line_outlook as T  # noqa: E402


def walk(node, out):
    if isinstance(node, dict):
        out.append(node)
        for v in node.values():
            walk(v, out)
    elif isinstance(node, list):
        for v in node:
            walk(v, out)
    return out


def texts(flex):
    return [n["text"] for n in walk(flex, []) if n.get("type") == "text"]


class FlexTest(unittest.TestCase):
    def _retail(self):
        head = ["card_id", "asof", "round", "fuel", "target_day", "retail_base", "p_down", "p_flat", "p_up", "call", "size_text", "model", "sig_reason"]
        rows = [head, ["a", "2026-11-12 21:40", "21:30", "B7", "2026-11-13", 40.79, 0.05, 0.6, 0.35, "ขึ้น", "", "SIG", "ผู้ค้าประกาศปรับขึ้น 3 ราย / ลง 0 ราย"],
                ["b", "2026-11-12 21:40", "21:30", "G95", "2026-11-13", 39.34, 0.05, 0.85, 0.1, "นิ่ง", "", "SIG", "คืนนี้ไม่มีประกาศปรับราคา และ MOPS ขยับน้อย"]]
        return L.retail_frame(rows)

    def test_full_carousel(self):
        inp = T._inp(asof="2026-11-12 21:35")
        for f in inp["fuels"].values():
            f["h24"].update(p_up=0.77, p_down=0.06)
        flex = F.build_flex(inp, T._log(), self._retail(), on_demand=True)
        car = flex["contents"]
        self.assertEqual(car["type"], "carousel")
        self.assertEqual([b["header"]["contents"][0]["text"] for b in car["contents"]],
                         ["🧭 ควรโหลดเมื่อไร", "🏭 ราคาหน้าคลัง (Jobber) · 24 ชม.", "⛽ ราคาขายปลีก (หน้าปั๊ม)", "📊 ผลงานที่ผ่านมา"])
        self.assertEqual(car["contents"][1]["header"]["backgroundColor"], F.THEME["jobber"][0])
        self.assertEqual(car["contents"][2]["header"]["backgroundColor"], F.THEME["retail"][0])
        allt = "\n".join(texts(flex))
        for want in ("📈 ขึ้น 10–40 สต.", "77%", "MOPS ปิดล่าสุด +2.20 USD/bbl", "ย้อนหลังระบบบอกแบบนี้ 105 ครั้ง: ถูก 61%", "❓ ไม่ชัด",
                     "📈 น่าจะปรับขึ้น", "• เหตุผล: ผู้ค้าประกาศปรับขึ้น 3 ราย", "คำทายคืน 11/11", "+0.86"):
            self.assertIn(want, allt)
        self.assertIn("ดีเซล", flex["altText"])
        self.assertLessEqual(len(flex["altText"]), 400)
        self.assertLess(len(json.dumps(flex, ensure_ascii=False).encode()), F.MAX_BYTES)

    def test_unclear_with_one_sided_reasons(self):
        """15:30 20/11/2026 : ไม่ชัดเจนแต่มีเหตุผลฝั่งเดียว → เดิม IndexError ทำให้ไม่มีการ์ด (Worker ตอบเป็นข้อความแทน)"""
        inp = T._inp(asof="2026-11-20 15:47", ref="2026-11-20 15:30")
        for f in inp["fuels"].values():
            f["h24"].update(tier="UNCLEAR", reasons=["MOPS 3 ครั้งล่าสุดรวม -6.99 USD/bbl → " + L.DOWN_MARK])
        inp["fuels"]["G95"]["h24"]["reasons"] = []
        flex = F.build_flex(inp, T._log(), self._retail())
        allt = "\n".join(texts(flex))
        self.assertIn("ชี้ลง: MOPS 3 ครั้งล่าสุดรวม -6.99 USD/bbl", allt)
        self.assertNotIn("ชี้ขึ้น", allt)

    def test_spec_rules(self):
        flex = F.build_flex(T._inp(), T._log(), self._retail())
        for n in walk(flex, []):
            if n.get("type") == "text":
                self.assertTrue(n["text"].strip(), n)                    # LINE ปฏิเสธข้อความว่าง
            if n.get("type") == "box":
                self.assertIn(n["layout"], ("vertical", "horizontal", "baseline"))
                self.assertTrue(n["contents"], n)
                if "width" in n:
                    self.assertRegex(n["width"], r"^\d{1,3}%$")
            if "backgroundColor" in n:
                self.assertRegex(n["backgroundColor"], r"^#[0-9A-Fa-f]{6}$")
        self.assertLessEqual(len(flex["contents"]["contents"]), 12)

    def test_degraded_inputs(self):
        stale = F.build_flex(T._inp(asof="2026-11-14 21:00", ref="2026-11-12 20:30"), pd.DataFrame(), None)
        names = [b["header"]["contents"][0]["text"] for b in stale["contents"]["contents"]]
        self.assertEqual(names, ["🏭 ราคาหน้าคลัง (Jobber) · 24 ชม."])          # ไม่มีคำแนะนำ/ปลีก/ผลงาน ก็ยังมีการ์ดหน้าคลังบอกเหตุผล
        self.assertIn("หมดอายุแล้ว", "\n".join(texts(stale)))
        pm = F.build_flex(T._inp(asof="2026-11-13 16:00", ref="2026-11-13 15:30"), T._log(), None)
        self.assertNotIn("🧭 ควรโหลดเมื่อไร", [b["header"]["contents"][0]["text"] for b in pm["contents"]["contents"]])
        self.assertIn("รอบบ่าย", "\n".join(texts(pm)))
        old = self._retail().assign(target_dt=pd.Timestamp("2026-11-01"))
        self.assertIn("ยังไม่มีพยากรณ์", "\n".join(texts(F.build_flex(T._inp(), T._log(), old))))


if __name__ == "__main__":
    unittest.main()
