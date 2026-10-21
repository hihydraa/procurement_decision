"""
ตัวตรวจโมเดล DeepSeek (รันรายสัปดาห์โดย .github/workflows/check_deepseek_models.yml) — รู้ล่วงหน้าก่อนที่ worker/ข่าวจะพังเงียบ ๆ
ถ้าชื่อโมเดลที่ใช้อยู่ถูกเลิก/เปลี่ยน

ตรวจ 2 ชั้น (ชั้นเดียวไม่พอ: อยู่ในรายชื่อ != เรียกได้จริง):
  1. GET /models  -> ชื่อโมเดลที่ใช้อยู่ต้องอยู่ในรายชื่อที่ key นี้ใช้ได้
  2. เรียกจริงแบบเล็กที่สุด: ข้อความด้วย TEXT_MODEL (ที่ worker แยกราคา + news.py ใช้) และรูปด้วย VISION_MODEL (ที่ worker อ่านรูป Mops)
ผลชี้ขาด (exit 1 = workflow ล้มให้เห็นชัด): โมเดลไม่อยู่ในรายชื่อ / ถูกปฏิเสธ 400,404,422 / key ใช้ไม่ได้ 401,403
ผลไม่ชี้ขาด (เตือนแต่ exit 0): เน็ตล่ม, 429, 5xx — สัปดาห์หน้าลองใหม่

ชื่อเริ่มต้นต้องตรงกับ worker.js (DEEPSEEK_MODEL / DEEPSEEK_VISION_MODEL) และ news.py ; ถ้าตั้งทับใน Cloudflare ด้วยตัวแปร env
ให้ตั้งตัวแปร repository เดียวกัน (Settings > Variables) ชื่อ DEEPSEEK_MODEL / DEEPSEEK_VISION_MODEL
"""
import base64
import os
import struct
import sys
import zlib

import requests

BASE = "https://api.deepseek.com"
TEXT_MODEL = os.environ.get("DEEPSEEK_MODEL") or "deepseek-chat"
VISION_MODEL = os.environ.get("DEEPSEEK_VISION_MODEL") or "deepseek-flash"
FAIL_STATUSES = {400, 401, 403, 404, 422}


def tiny_png_b64(size: int = 32) -> str:
    """PNG สีแดงล้วน (stdlib ล้วน ไม่พึ่ง Pillow) ใช้เป็นรูปทดสอบ"""
    def chunk(tag, data):
        c = struct.pack(">I", len(data)) + tag + data
        return c + struct.pack(">I", zlib.crc32(tag + data) & 0xFFFFFFFF)
    raw = b"".join(b"\x00" + b"\xff\x00\x00" * size for _ in range(size))
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")
    return base64.b64encode(png).decode()


def call(method, path, key, **kw):
    """คืน (status, ข้อความ/ JSON) — status=None เมื่อเชื่อมต่อไม่ได้"""
    try:
        r = requests.request(method, BASE + path, headers={"Authorization": f"Bearer {key}"}, timeout=40, **kw)
        try:
            return r.status_code, r.json()
        except ValueError:
            return r.status_code, r.text[:300]
    except requests.RequestException as e:
        return None, str(e)[:300]


def list_models(key):
    status, body = call("GET", "/models", key)
    ids = [m.get("id") for m in body.get("data", [])] if status == 200 and isinstance(body, dict) else None
    return status, ids, body


def probe_text(key, model):
    return call("POST", "/chat/completions", key, json={"model": model, "max_tokens": 5, "temperature": 0,
                                                         "messages": [{"role": "user", "content": "ตอบคำว่า OK"}]})


def probe_vision(key, model):
    return call("POST", "/chat/completions", key, json={"model": model, "max_tokens": 5, "temperature": 0, "messages": [{"role": "user", "content": [
        {"type": "text", "text": "รูปนี้สีอะไร ตอบสั้น ๆ"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64," + tiny_png_b64()}}]}]})


def judge(list_status, ids, probes):
    """คืนรายการ (ระดับ, ข้อความ) ระดับ = OK | WARN | FAIL ; probes = {ชื่อ: (status, body)}"""
    out = []
    if list_status == 200 and ids is not None:
        out.append(("OK", f"รายชื่อโมเดลของ key นี้: {', '.join(sorted(i for i in ids if i))}"))
        for name in probes:
            if name not in ids:
                out.append(("FAIL", f"โมเดล '{name}' ไม่อยู่ในรายชื่อที่ key นี้ใช้ได้ (อาจถูกเลิก/เปลี่ยนชื่อ)"))
    elif list_status in (401, 403):
        out.append(("FAIL", f"GET /models ถูกปฏิเสธ ({list_status}) — API key ใช้ไม่ได้"))
    else:
        out.append(("WARN", f"GET /models ไม่ชี้ขาด (status={list_status}) — ข้ามการเทียบรายชื่อ"))
    for name, (status, body) in probes.items():
        if status == 200:
            out.append(("OK", f"เรียก '{name}' สำเร็จ"))
        elif status in FAIL_STATUSES:
            out.append(("FAIL", f"เรียก '{name}' ถูกปฏิเสธ ({status}): {str(body)[:200]}"))
        else:
            out.append(("WARN", f"เรียก '{name}' ไม่ชี้ขาด (status={status}): {str(body)[:120]}"))
    return out


def main():
    key = os.environ.get("DEEPSEEK_API_KEY")
    if not key:
        print("FAIL: ไม่มี DEEPSEEK_API_KEY")
        return 1
    list_status, ids, _ = list_models(key)
    probes = {TEXT_MODEL: probe_text(key, TEXT_MODEL)}
    probes[VISION_MODEL] = probe_vision(key, VISION_MODEL)      # ชื่อเดียวกันกับ TEXT_MODEL ก็ถูกทดสอบรูปแทน (เข้มกว่า)
    findings = judge(list_status, ids, probes)
    lines = [f"[{lvl}] {msg}" for lvl, msg in findings]
    print("\n".join(lines))
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write("### ตรวจโมเดล DeepSeek\n" + "\n".join(f"- {l}" for l in lines) + "\n")
    return 1 if any(lvl == "FAIL" for lvl, _ in findings) else 0


if __name__ == "__main__":
    sys.exit(main())
