# Fuel Procurement Decision

Dashboard ช่วยตัดสินใจซื้อน้ำมัน (ดีเซล / แก๊สโซฮอล์ 95) — สร้างหน้าเว็บทุกรอบด้วย GitHub Actions (`.github/workflows/update_dashboard.yml`)

## หลักการตัดสินใจ

คำแนะนำมาจาก **ความน่าจะเป็นที่ราคาต่ำสุดของตลาดจะขึ้น/ลงข้ามคืน** (เทียบ 15:30 วันนี้ กับ 15:30 พรุ่งนี้ เพราะราคาเป็นฟันเลื่อย:
ขึ้นก้อนใหญ่ข้ามคืนแล้วค่อย ๆ ลดทั้งวัน) ไม่ใช่ rule if/else ตามสถานะ MOPS/กองทุน เหมือนเดิม

| ไฟล์ | หน้าที่ |
|---|---|
| `signals.py` | ฟีเจอร์ที่ "รู้แล้ว ณ เวลาตัดสินใจ" (Trend Mops ระหว่างวัน, ประกาศ "มีปรับขึ้น/ลง", MOPS ปิด, ประกาศกองทุน) + `PriceBook` (ราคาต่ำสุดตลาด) |
| `model.py` | โลจิสติกส์เล็ก ๆ (numpy) + เกณฑ์แปลงเป็น เร่งซื้อ / ทยอยซื้อ / ซื้อปกติ / ชะลอ |
| `engine.py` | ตรวจความสดของฟีด → เลือกโมเดลตามช่วง (PM ก่อน ~19:00 / EVE หลังรู้ MOPS ปิดและกองทุน) → เหตุผล + หลักฐานย้อนหลัง; ฟีดหาย/เก่า = "ข้อมูลไม่พอ" ไม่ใช้ค่าค้าง |
| `fit_model.py` | ฝึกและวัด walk-forward → `model_params.json`, `backtest_summary.json` |
| `ingest.py` | คัดลอกฟีดจากชีท OIL BOT (`Entry_Intraday`, `Entry_HeadsUp`, `Entry_FundNotice`, `Entry_MopsClose`, `Entry_Prices`) + EPPO/MOPS/กองทุนเดิม (EPPO มีคอลัมน์ `Source` = FRESH/CARRIED) |
| `log_decisions.py` | บันทึกคำแนะนำจริงลงแท็บ `DecisionLog` แล้วเติมผลลัพธ์เมื่อรู้ราคาวันถัดไป |
| `check_models.py` | ตรวจรายสัปดาห์ (workflow `check_deepseek_models.yml`) ว่าโมเดล DeepSeek ที่ worker/news.py ใช้ยังอยู่ในรายชื่อและเรียกได้จริง (ข้อความ + รูป) ล้มให้เห็นชัดเมื่อโมเดลหาย |
| `main.py` | สร้าง `index.html` (+ `decision_snapshot.json`) ; ตรวจความสดของข้อมูลแล้วแสดงคำเตือน |

## ฝึกโมเดลใหม่

```
# ข้อมูลย้อนหลังสร้างจาก export แชท Jobber ด้วย tools/backtest/parse_line_export.py (repo oil-bot) — ไม่เก็บใน repo นี้
python3 fit_model.py <data_dir>        # เขียน model_params.json + backtest_summary.json แล้ว commit
python3 -m unittest discover -s tests  # ทดสอบหน่วย (ข้อมูลสังเคราะห์)
python3 tests/smoke_offline.py <data_dir> <eppo_raw.json> "2026-10-20 20:35"   # รัน main.py ทั้งตัวแบบออฟไลน์ ณ เวลาที่กำหนด
```

เมื่อ `DecisionLog` สะสมผลลัพธ์พอ (>= 20 วันต่อชนิด/ช่วง/คำแนะนำ) หน้าเว็บจะแสดงสถิติใช้งานจริงแทน backtest

## Secrets

`SA_EMAIL`, `SA_PRIVATE_KEY` (ชีท), `OILPRICEAPI_KEY` (WTI), `DEEPSEEK_API_KEY` (NEWS + ตรวจโมเดล)
ตัวแปร repository (ไม่บังคับ): `DEEPSEEK_MODEL`, `DEEPSEEK_VISION_MODEL` — ตั้งให้ตรงกับที่ตั้งทับใน Cloudflare (ว่าง = deepseek-chat / deepseek-flash)
