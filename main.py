import pandas as pd

# 1. ตั้งค่าการเชื่อมต่อ (แทนที่ GID ด้วยค่าจริงที่คุณจดไว้)
SHEET_ID = "1JNrYTJtmgpOGjdfhIdJKz15i8Jxp-u8r1UFAXuQRs8s"
GIDS = {
    'NYMEX': '1915076819',
    'MOPS': '739092564',
    'EPPO': 'เ625314144',
    'SETTING': '352342695',
    'Inventory': '1983607313',
    'WTI':'1880473564'
}

def load_sheet(gid):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    return pd.read_csv(url)

# 2. เริ่มการวิเคราะห์
df_eppo = load_sheet(GIDS['EPPO'])
# ดึงค่าล่าสุดมาตรวจสอบ (เช่น Marketing Margin ของน้ำมัน G95)
latest_margin = df_eppo[df_eppo['Oil Type'] == '95']['Marketing Margin'].iloc[0]

# 3. ตรรกะการให้คำแนะนำ (Recommendation Logic)
if latest_margin < 1.5:
    signal = "🟢 เร่งจัดซื้อ (ค่าการตลาดต่ำ มีโอกาสราคาขึ้น)"
    color = "success"
else:
    signal = "🟡 ชะลอการซื้อ (ค่าการตลาดยังสูง)"
    color = "warning"

# 4. สร้างหน้า HTML (Dashboard)
html_template = f"""
<div class="alert alert-{color}">
    <h2>คำแนะนำวันนี้: {signal}</h2>
    <p>Marketing Margin ปัจจุบัน: {latest_margin:.4f} บาท/ลิตร</p>
</div>
"""
# (ใส่โค้ด HTML เต็มรูปแบบที่นี่)
with open("index.html", "w", encoding="utf-8") as f:
    f.write(html_template)
