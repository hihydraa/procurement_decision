import pandas as pd
import datetime

# 1. ตั้งค่าการเชื่อมต่อ
SHEET_ID = "1JNrYTJtmgpOGjdfhIdJKz15i8Jxp-u8r1UFAXuQRs8s"
GIDS = {
    'NYMEX': '1915076819',
    'MOPS': '739092564',
    'EPPO': '625314144',
    'SETTING': '352342695',
    'Inventory': '1983607313',
    'WTI': '1880473564'
}

def load_sheet(gid):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    # เพิ่ม error_bad_lines เพื่อป้องกันกรณีมีแถวว่าง
    return pd.read_csv(url, on_bad_lines='skip')

try:
    # 2. ดึงข้อมูล
    df_eppo = load_sheet(GIDS['EPPO'])
    df_mops = load_sheet(GIDS['MOPS'])
    
    # ดึงข้อมูลล่าสุดของเบนซิน 95 (G95)
    # ใช้ .copy() เพื่อป้องกัน SettingWithCopyWarning
    df_95 = df_eppo[df_eppo['Oil Type'].astype(str) == '95'].copy()
    
    if not df_95.empty:
        latest_data = df_95.iloc[0]
        latest_margin = float(latest_data['Marketing Margin'])
        ex_refinery = float(latest_data['Ex-Refinery Price'])
        retail_price = float(latest_data['Retail'])
        
        # 3. ตรรกะการให้คำแนะนำ (Decision Logic)
        if latest_margin < 1.5:
            signal = "🟢 เร่งจัดซื้อ (ค่าการตลาดต่ำ มีโอกาสราคาขึ้น)"
            color_class = "alert-success"
            action_icon = "🚀"
        elif latest_margin > 2.2:
            signal = "🔴 ชะลอการซื้อ (ค่าการตลาดยังสูง มีโอกาสราคาลง)"
            color_class = "alert-danger"
            action_icon = "🛑"
        else:
            signal = "🟡 ซื้อตามความจำเป็น (สถานการณ์ปกติ)"
            color_class = "alert-warning"
            action_icon = "⚖️"
    else:
        signal = "⚠️ ไม่พบข้อมูลน้ำมัน 95 ในตาราง"
        color_class = "alert-secondary"
        latest_margin = 0
        ex_refinery = 0
        retail_price = 0
        action_icon = "❓"

    # 4. สร้างหน้า HTML Dashboard ที่สวยงาม (Bootstrap 5)
    update_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
    
    html_template = f"""
    <!DOCTYPE html>
    <html lang="th">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <title>Oil Procurement Dashboard</title>
        <style>
            body {{ background-color: #f8f9fa; font-family: 'Sarabun', sans-serif; }}
            .card {{ border: none; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
            .recommendation-box {{ font-size: 1.5rem; font-weight: bold; }}
        </style>
    </head>
    <body>
        <div class="container py-5">
            <div class="d-flex justify-content-between align-items-center mb-4">
                <h1>{action_icon} Decision Support Dashboard</h1>
                <span class="badge bg-dark">อัปเดตล่าสุด: {update_time}</span>
            </div>

            <div class="row g-4">
                <div class="col-12">
                    <div class="card p-4 {color_class} recommendation-box">
                        คำแนะนำวันนี้: {signal}
                    </div>
                </div>

                <div class="col-md-4">
                    <div class="card p-3 text-center">
                        <h6 class="text-muted">Marketing Margin</h6>
                        <h2 class="text-primary">{latest_margin:.4f} <small>บ/ลิตร</small></h2>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card p-3 text-center">
                        <h6 class="text-muted">Ex-Refinery (ต้นทุน)</h6>
                        <h2 class="text-dark">{ex_refinery:.4f} <small>บ/ลิตร</small></h2>
                    </div>
                </div>
                <div class="col-md-4">
                    <div class="card p-3 text-center">
                        <h6 class="text-muted">Retail Price (ราคาปลีก)</h6>
                        <h2 class="text-success">{retail_price:.2f} <small>บ/ลิตร</small></h2>
                    </div>
                </div>
            </div>
            
            <footer class="mt-5 text-center text-muted">
                <p>ข้อมูลจาก Google Sheets: Procurement Support Decision</p>
            </footer>
        </div>
    </body>
    </html>
    """

    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html_template)
    print("Dashboard generated successfully!")

except Exception as e:
    print(f"เกิดข้อผิดพลาด: {e}")
    # สร้างหน้า HTML แจ้ง Error เพื่อให้ Dashboard ไม่ว่างเปล่า
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(f"<h1>Error:</h1><p>{str(e)}</p>")
