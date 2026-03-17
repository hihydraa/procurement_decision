import pandas as pd
import datetime

SHEET_ID = "1JNrYTJtmgpOGjdfhIdJKz15i8Jxp-u8r1UFAXuQRs8s"
# ตรวจสอบ GID เหล่านี้อีกครั้งจาก URL ของแต่ละแท็บนะครับ
GIDS = {
    'NYMEX': '1915076819',
    'MOPS': '739092564',
    'EPPO': '625314144', # ลบตัว "เ" ออกแล้ว
    'SETTING': '352342695',
    'Inventory': '1983607313',
    'WTI': '1880473564'
}

def load_sheet(gid):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    return pd.read_csv(url)

try:
    df_eppo = load_sheet(GIDS['EPPO'])
    
    # --- จุดตรวจสอบ Error ---
    print(f"คอลัมน์ที่ตรวจพบในหน้า EPPO: {df_eppo.columns.tolist()}")
    
    # ลบช่องว่างส่วนเกินในชื่อคอลัมน์ (ป้องกันกรณีพิมพ์ใน Sheet เกินมา)
    df_eppo.columns = df_eppo.columns.str.strip()
    
    # ตรวจสอบว่ามีคอลัมน์ที่ต้องการไหม
    if 'Marketing Margin' not in df_eppo.columns:
        raise ValueError(f"ไม่เจอคอลัมน์ 'Marketing Margin' เจอแต่ {df_eppo.columns.tolist()} เช็ค GID ด่วน!")

    # ดึงข้อมูลน้ำมัน 95
    df_95 = df_eppo[df_eppo['Oil Type'].astype(str).str.contains('95')].copy()
    
    if not df_95.empty:
        latest_data = df_95.iloc[0]
        latest_margin = float(latest_data['Marketing Margin'])
        # ... (โค้ดส่วนที่เหลือเหมือนเดิม)
        print("วิเคราะห์สำเร็จ!")
    else:
        print("ไม่พบข้อมูลน้ำมัน 95")

except Exception as e:
    print(f"❌ Error เกิดจาก: {e}")
    # สร้างไฟล์ index.html เพื่อบอกสาเหตุบนหน้าเว็บ
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(f"<h1>พบข้อผิดพลาดในการรันระบบ</h1><p>{str(e)}</p>")
