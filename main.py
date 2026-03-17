import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

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
    df = pd.read_csv(url)
    df.columns = df.columns.str.strip()
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
        df = df.dropna(subset=['Date'])
    return df

def write_html(content):
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(content)

try:
    df_eppo = load_sheet(GIDS['EPPO'])

    required_cols = ['Date', 'Oil Type', 'Marketing Margin', 'Ex-Refinery Price', 'Oil Fund', 'Retail']
    missing = [c for c in required_cols if c not in df_eppo.columns]
    if missing:
        raise ValueError(f"ไม่พบคอลัมน์ที่จำเป็น: {missing}")

    df_eppo = df_eppo.sort_values(by='Date', ascending=False)

    target_oil = '95'
    df_target = df_eppo[df_eppo['Oil Type'].astype(str).str.contains(target_oil, na=False)]

    if df_target.empty:
        raise ValueError(f"ไม่พบข้อมูล Oil Type ที่ตรงกับ {target_oil}")

    latest_row = df_target.iloc[0]
    data_date = latest_row['Date']
    margin = pd.to_numeric(latest_row['Marketing Margin'], errors='coerce')
    ex_refinery = pd.to_numeric(latest_row['Ex-Refinery Price'], errors='coerce')
    oil_fund = pd.to_numeric(latest_row['Oil Fund'], errors='coerce')
    retail = pd.to_numeric(latest_row['Retail'], errors='coerce')

    if pd.isna(margin) or pd.isna(ex_refinery) or pd.isna(oil_fund) or pd.isna(retail):
        raise ValueError("พบข้อมูลตัวเลขที่แปลงค่าไม่ได้ในแถวล่าสุด")

    if margin < 1.5:
        signal = "🟢 เร่งจัดซื้อทันที"
        reason = f"ค่าการตลาดต่ำมาก ({margin:.4f}) ผู้ค้ามีโอกาสปรับขึ้นราคาเพื่อรักษา Margin"
        color = "success"
    elif margin > 2.2:
        signal = "🔴 ชะลอการซื้อ"
        reason = f"ค่าการตลาดอยู่ในระดับสูง ({margin:.4f}) มีโอกาสที่ราคาจะทรงตัวหรือปรับลดลง"
        color = "danger"
    else:
        signal = "🟡 ซื้อตามความจำเป็น"
        reason = "ราคาอยู่ในเกณฑ์ปกติ สอดคล้องกับโครงสร้างต้นทุน"
        color = "warning"

    update_time = datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%d/%m/%Y %H:%M")

    html_content = f"""
    <!DOCTYPE html>
    <html lang="th">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <title>Fuel Procurement Dashboard</title>
        <style>
            body {{ background-color: #f4f7f6; font-family: Arial, sans-serif; }}
            .main-card {{ border-radius: 15px; border: none; box-shadow: 0 10px 20px rgba(0,0,0,0.05); }}
            .stat-value {{ font-size: 1.8rem; font-weight: bold; color: #2c3e50; }}
            .stat-label {{ color: #7f8c8d; font-size: 0.9rem; text-transform: uppercase; }}
        </style>
    </head>
    <body>
        <div class="container py-5">
            <div class="d-flex justify-content-between align-items-end mb-4">
                <div>
                    <h1 class="fw-bold text-primary">Oil Decision Support</h1>
                    <p class="text-muted mb-0">ข้อมูลอ้างอิงงวดวันที่: <strong>{data_date}</strong></p>
                </div>
                <div class="text-end">
                    <span class="badge bg-secondary">System Update: {update_time}</span>
                </div>
            </div>

            <div class="row g-4">
                <div class="col-12">
                    <div class="card main-card bg-{color} text-white p-4">
                        <h2 class="display-6 fw-bold mb-2">{signal}</h2>
                        <p class="fs-5 mb-0">เหตุผล: {reason}</p>
                    </div>
                </div>

                <div class="col-md-3">
                    <div class="card main-card p-4 text-center">
                        <div class="stat-label">Marketing Margin</div>
                        <div class="stat-value text-primary">{margin:.4f}</div>
                        <div class="small">บาท/ลิตร</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card main-card p-4 text-center">
                        <div class="stat-label">Ex-Refinery Price</div>
                        <div class="stat-value">{ex_refinery:.4f}</div>
                        <div class="small">บาท/ลิตร</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card main-card p-4 text-center">
                        <div class="stat-label">Oil Fund</div>
                        <div class="stat-value {'text-danger' if oil_fund < 0 else ''}">{oil_fund:.2f}</div>
                        <div class="small">บาท/ลิตร</div>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="card main-card p-4 text-center">
                        <div class="stat-label">Retail Price</div>
                        <div class="stat-value text-success">{retail:.2f}</div>
                        <div class="small">บาท/ลิตร</div>
                    </div>
                </div>
            </div>

            <footer class="mt-5 text-center text-muted border-top pt-3">
                <p>ระบบพยากรณ์และวิเคราะห์ราคาน้ำมันเชื้อเพลิงล่วงหน้า v1.0</p>
            </footer>
        </div>
    </body>
    </html>
    """

    write_html(html_content)
    print(f"Dashboard updated using data from: {data_date}")

except Exception as e:
    error_html = f"""
    <html lang="th">
    <head><meta charset="UTF-8"><title>Dashboard Error</title></head>
    <body style="font-family: Arial, sans-serif; padding: 24px;">
        <h1>พบข้อผิดพลาด</h1>
        <p>{str(e)}</p>
    </body>
    </html>
    """
    write_html(error_html)
    raise
