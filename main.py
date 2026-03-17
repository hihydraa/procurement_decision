import pandas as pd

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
    return pd.read_csv(url)

def write_html(title, body):
    html = f"""
    <!doctype html>
    <html lang="th">
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>{title}</title>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; background:#f7f7f7; }}
        .card {{ background:white; padding:24px; border-radius:16px; box-shadow:0 2px 10px rgba(0,0,0,.08); max-width:900px; margin:auto; }}
        h1 {{ margin-top:0; }}
      </style>
    </head>
    <body>
      <div class="card">
        {body}
      </div>
    </body>
    </html>
    """
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(html)

try:
    df_eppo = load_sheet(GIDS['EPPO'])
    df_eppo.columns = df_eppo.columns.str.strip()

    if 'Marketing Margin' not in df_eppo.columns:
        raise ValueError(f"ไม่เจอคอลัมน์ 'Marketing Margin' เจอแต่ {df_eppo.columns.tolist()}")

    df_95 = df_eppo[df_eppo['Oil Type'].astype(str).str.contains('95', na=False)].copy()

    if df_95.empty:
        raise ValueError("ไม่พบข้อมูลน้ำมัน 95")

    latest_data = df_95.iloc[0]
    latest_margin = float(latest_data['Marketing Margin'])

    body = f"""
    <h1>Oil Procurement Dashboard</h1>
    <p>ระบบอัปเดตสำเร็จ</p>
    <p><strong>Latest Marketing Margin (95):</strong> {latest_margin}</p>
    <p><strong>Columns:</strong> {df_eppo.columns.tolist()}</p>
    """
    write_html("Oil Procurement Dashboard", body)

except Exception as e:
    body = f"""
    <h1>พบข้อผิดพลาดในการรันระบบ</h1>
    <p>{str(e)}</p>
    """
    write_html("Dashboard Error", body)
    raise
