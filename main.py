import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

# =========================
# CONFIG
# =========================
SHEET_ID = "1JNrYTJtmgpOGjdfhIdJKz15i8Jxp-u8r1UFAXuQRs8s"
GIDS = {
    "NYMEX": "1915076819",
    "MOPS": "739092564",
    "EPPO": "625314144",
    "SETTING": "352342695",
    "WTI": "1880473564",
}

FUELS = ["95", "DS"]


# =========================
# HELPERS
# =========================
def write_html(content: str):
    with open("index.html", "w", encoding="utf-8") as f:
        f.write(content)


def load_sheet(gid: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    df = pd.read_csv(url)
    df.columns = df.columns.str.strip()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date

    return df


def to_numeric_series(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(",", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def format_change(val: float, digits: int = 2) -> str:
    if pd.isna(val):
        return "-"
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.{digits}f}"


def badge_class_by_change(val: float) -> str:
    if pd.isna(val) or val == 0:
        return "secondary"
    return "danger" if val > 0 else "success"


def card_border_by_signal(color: str) -> str:
    mapping = {
        "success": "#198754",
        "warning": "#ffc107",
        "danger": "#dc3545",
        "secondary": "#6c757d",
    }
    return mapping.get(color, "#0d6efd")


def get_latest_two_rows_by_oil(df: pd.DataFrame, oil_type_col: str, oil_type_val: str) -> pd.DataFrame:
    if oil_type_col not in df.columns or "Date" not in df.columns:
        return pd.DataFrame()

    filtered = df[
        df[oil_type_col].astype(str).str.contains(oil_type_val, case=False, na=False)
    ].copy()

    filtered = filtered.dropna(subset=["Date"]).sort_values("Date", ascending=False)
    return filtered.head(2)


def get_latest_two_rows_no_oil(df: pd.DataFrame) -> pd.DataFrame:
    if "Date" not in df.columns:
        return pd.DataFrame()
    return df.dropna(subset=["Date"]).sort_values("Date", ascending=False).head(2)


def get_latest_with_change_by_oil(
    df: pd.DataFrame,
    oil_type_col: str,
    oil_type_val: str,
    target_col: str
):
    rows = get_latest_two_rows_by_oil(df, oil_type_col, oil_type_val)

    if rows.empty or target_col not in rows.columns:
        return None, None, None

    curr = rows.iloc[0][target_col]
    curr_date = rows.iloc[0]["Date"]

    if pd.isna(curr):
        return None, None, curr_date

    if len(rows) >= 2 and not pd.isna(rows.iloc[1][target_col]):
        prev = rows.iloc[1][target_col]
        return float(curr), float(curr - prev), curr_date

    return float(curr), 0.0, curr_date


def get_latest_with_change_no_oil(df: pd.DataFrame, target_col: str):
    rows = get_latest_two_rows_no_oil(df)

    if rows.empty or target_col not in rows.columns:
        return None, None, None

    curr = rows.iloc[0][target_col]
    curr_date = rows.iloc[0]["Date"]

    if pd.isna(curr):
        return None, None, curr_date

    if len(rows) >= 2 and not pd.isna(rows.iloc[1][target_col]):
        prev = rows.iloc[1][target_col]
        return float(curr), float(curr - prev), curr_date

    return float(curr), 0.0, curr_date


def safe_value(v, digits=2):
    if v is None or pd.isna(v):
        return "-"
    return f"{v:.{digits}f}"


def generate_recommendation(fuel: str, mm, mm_chg, nymex_chg, wti_chg):
    threshold = 3.0 if fuel == "95" else 1.6

    nymex_chg = 0 if nymex_chg is None or pd.isna(nymex_chg) else nymex_chg
    wti_chg = 0 if wti_chg is None or pd.isna(wti_chg) else wti_chg
    mm = None if mm is None or pd.isna(mm) else mm
    mm_chg = 0 if mm_chg is None or pd.isna(mm_chg) else mm_chg

    if mm is None:
        return "⚪ ข้อมูลไม่เพียงพอ", "secondary", "ไม่พบข้อมูลค่าการตลาดเพียงพอสำหรับการวิเคราะห์"

    market_up = (nymex_chg > 0) or (wti_chg > 0)
    market_down = (nymex_chg < 0) and (wti_chg < 0)

    if market_up and mm < threshold:
        return (
            "🟢 เร่งจัดซื้อ",
            "success",
            f"ตลาดโลกมีแรงขึ้น และค่าการตลาดอยู่ต่ำกว่าระดับเฝ้าระวัง ({mm:.2f} < {threshold:.2f})"
        )

    if market_down and mm > (threshold + 0.5):
        return (
            "🔴 ชะลอการซื้อ",
            "danger",
            f"ตลาดโลกอ่อนตัว ขณะที่ค่าการตลาดอยู่สูง ({mm:.2f}) มีโอกาสรอจังหวะได้"
        )

    if mm_chg > 0.15 and mm > threshold:
        return (
            "🟡 ซื้อตามปกติ",
            "warning",
            "ค่าการตลาดขยับขึ้น แต่ยังไม่ใช่สัญญาณเร่งซื้อ ควรติดตามต่อเนื่อง"
        )

    return (
        "🟡 ซื้อตามปกติ",
        "warning",
        "โครงสร้างราคายังอยู่ในกรอบปกติ ควรจัดซื้อตามแผนและติดตามการเปลี่ยนแปลงรายวัน"
    )


def build_metric_card(title, value, unit, change=None, value_class=""):
    change_html = ""
    if change is not None and change != "-":
        badge_class = badge_class_by_change(change if isinstance(change, (int, float)) else 0)
        if isinstance(change, (int, float)):
            change_text = format_change(change, 2)
        else:
            change_text = str(change)
        change_html = f'<div class="mt-2"><span class="badge bg-{badge_class}">Δ {change_text}</span></div>'

    return f"""
    <div class="col-md-3 col-sm-6">
        <div class="card stat-card h-100">
            <div class="card-body text-center">
                <div class="stat-label">{title}</div>
                <div class="stat-value {value_class}">{value}</div>
                <div class="small text-muted">{unit}</div>
                {change_html}
            </div>
        </div>
    </div>
    """


def build_fuel_section(fuel: str, data: dict) -> str:
    signal_border = card_border_by_signal(data["color"])

    return f"""
    <div class="col-12">
        <div class="fuel-section">
            <div class="d-flex justify-content-between align-items-center flex-wrap gap-2 mb-3">
                <div>
                    <h2 class="mb-1">น้ำมัน {fuel}</h2>
                    <div class="text-muted">ข้อมูลอ้างอิงล่าสุด: <strong>{data['date']}</strong></div>
                </div>
                <div>
                    <span class="badge bg-dark px-3 py-2">{data['signal']}</span>
                </div>
            </div>

            <div class="card decision-card mb-4" style="border-left: 8px solid {signal_border};">
                <div class="card-body">
                    <h4 class="mb-2">{data['signal']}</h4>
                    <p class="mb-0">{data['reason']}</p>
                </div>
            </div>

            <div class="row g-3 mb-3">
                {build_metric_card("Marketing Margin", safe_value(data["mm"], 4), "บาท/ลิตร", data["mm_chg"], "text-primary")}
                {build_metric_card("Ex-Refinery Price", safe_value(data["ex"], 4), "บาท/ลิตร", data["ex_chg"])}
                {build_metric_card("Oil Fund", safe_value(data["fund"], 2), "บาท/ลิตร", data["fund_chg"], "text-danger" if (data["fund"] is not None and data["fund"] < 0) else "")}
                {build_metric_card("Retail Price", safe_value(data["retail"], 2), "บาท/ลิตร", data["retail_chg"], "text-success")}
            </div>

            <div class="row g-3">
                {build_metric_card("MOPS", safe_value(data["mops"], 2), "USD/BBL", data["mops_chg"])}
                {build_metric_card("NYMEX", safe_value(data["nymex"], 2), "USD", data["nymex_chg"])}
                {build_metric_card("WTI", safe_value(data["wti"], 2), "USD", data["wti_chg"])}
                {build_metric_card("Threshold", safe_value(data["threshold"], 2), "บาท/ลิตร", None)}
            </div>
        </div>
    </div>
    """


# =========================
# MAIN
# =========================
try:
    df_eppo = load_sheet(GIDS["EPPO"])
    df_mops = load_sheet(GIDS["MOPS"])
    df_wti = load_sheet(GIDS["WTI"])
    df_nymex = load_sheet(GIDS["NYMEX"])

    df_eppo = to_numeric_series(df_eppo, ["Marketing Margin", "Ex-Refinery Price", "Oil Fund", "Retail"])
    df_mops = to_numeric_series(df_mops, ["Price (USD/BBL)", "Price"])
    df_wti = to_numeric_series(df_wti, ["Price"])
    df_nymex = to_numeric_series(df_nymex, ["Price"])

    required_eppo = ["Date", "Oil Type", "Marketing Margin", "Ex-Refinery Price", "Oil Fund", "Retail"]
    for col in required_eppo:
        if col not in df_eppo.columns:
            raise ValueError(f"ไม่พบคอลัมน์ {col} ในชีต EPPO")

    if "Date" not in df_mops.columns or "Oil Type" not in df_mops.columns:
        raise ValueError("ชีต MOPS ต้องมีคอลัมน์ Date และ Oil Type")

    update_time = datetime.now(ZoneInfo("Asia/Bangkok")).strftime("%d/%m/%Y %H:%M")

    nymex, nymex_chg, nymex_date = get_latest_with_change_no_oil(df_nymex, "Price")
    wti, wti_chg, wti_date = get_latest_with_change_no_oil(df_wti, "Price")

    results = {}

    for fuel in FUELS:
        mm, mm_chg, data_date = get_latest_with_change_by_oil(df_eppo, "Oil Type", fuel, "Marketing Margin")
        ex, ex_chg, _ = get_latest_with_change_by_oil(df_eppo, "Oil Type", fuel, "Ex-Refinery Price")
        retail, retail_chg, _ = get_latest_with_change_by_oil(df_eppo, "Oil Type", fuel, "Retail")
        fund, fund_chg, _ = get_latest_with_change_by_oil(df_eppo, "Oil Type", fuel, "Oil Fund")
        mops, mops_chg, _ = get_latest_with_change_by_oil(df_mops, "Oil Type", fuel, "Price (USD/BBL)")

        signal, color, reason = generate_recommendation(fuel, mm, mm_chg, nymex_chg, wti_chg)

        threshold = 3.0 if fuel == "95" else 1.6

        results[fuel] = {
            "mm": mm,
            "mm_chg": mm_chg,
            "ex": ex,
            "ex_chg": ex_chg,
            "retail": retail,
            "retail_chg": retail_chg,
            "fund": fund,
            "fund_chg": fund_chg,
            "mops": mops,
            "mops_chg": mops_chg,
            "nymex": nymex,
            "nymex_chg": nymex_chg,
            "wti": wti,
            "wti_chg": wti_chg,
            "signal": signal,
            "color": color,
            "reason": reason,
            "date": data_date if data_date else "-",
            "threshold": threshold,
        }

    fuel_sections = "".join(build_fuel_section(fuel, results[fuel]) for fuel in FUELS)

    html_content = f"""
    <!DOCTYPE html>
    <html lang="th">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Oil Procurement Decision Dashboard</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body {{
                background: #f4f7fb;
                font-family: Arial, sans-serif;
                color: #1f2937;
            }}
            .page-title {{
                font-size: 2.2rem;
                font-weight: 800;
                margin-bottom: 0.25rem;
            }}
            .subtitle {{
                color: #6b7280;
            }}
            .header-card, .stat-card, .decision-card, .fuel-section {{
                border: none;
                border-radius: 18px;
                box-shadow: 0 10px 25px rgba(0,0,0,0.06);
                background: #fff;
            }}
            .header-card {{
                padding: 28px;
            }}
            .fuel-section {{
                padding: 24px;
                margin-bottom: 28px;
            }}
            .stat-card {{
                transition: transform 0.15s ease;
            }}
            .stat-card:hover {{
                transform: translateY(-2px);
            }}
            .stat-label {{
                font-size: 0.85rem;
                color: #6b7280;
                text-transform: uppercase;
                letter-spacing: .04em;
                margin-bottom: 8px;
            }}
            .stat-value {{
                font-size: 1.8rem;
                font-weight: 800;
                line-height: 1.2;
            }}
            .market-box {{
                background: linear-gradient(135deg, #0d6efd, #3b82f6);
                color: white;
                border-radius: 16px;
                padding: 20px;
                height: 100%;
            }}
            .mini-label {{
                font-size: 0.8rem;
                opacity: 0.9;
                text-transform: uppercase;
            }}
            .mini-value {{
                font-size: 1.6rem;
                font-weight: 800;
            }}
            .footer-note {{
                color: #6b7280;
                font-size: 0.92rem;
            }}
        </style>
    </head>
    <body>
        <div class="container py-5">
            <div class="header-card mb-4">
                <div class="row g-4 align-items-center">
                    <div class="col-lg-8">
                        <div class="page-title">Oil Procurement Decision Dashboard</div>
                        <div class="subtitle">
                            ระบบช่วยผู้บริหารตัดสินใจในการจัดซื้อน้ำมันจากข้อมูล EPPO, MOPS และตลาดโลก
                        </div>
                    </div>
                    <div class="col-lg-4">
                        <div class="market-box">
                            <div class="mb-2"><strong>Global Market Snapshot</strong></div>
                            <div class="row g-3">
                                <div class="col-6">
                                    <div class="mini-label">NYMEX</div>
                                    <div class="mini-value">{safe_value(nymex, 2)}</div>
                                    <div>Δ {format_change(nymex_chg or 0, 2)}</div>
                                </div>
                                <div class="col-6">
                                    <div class="mini-label">WTI</div>
                                    <div class="mini-value">{safe_value(wti, 2)}</div>
                                    <div>Δ {format_change(wti_chg or 0, 2)}</div>
                                </div>
                            </div>
                            <div class="mt-3 small">
                                อัปเดตระบบ: {update_time}
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            {fuel_sections}

            <div class="text-center mt-4 footer-note">
                ระบบวิเคราะห์ราคาน้ำมันเชื้อเพลิงล่วงหน้าและสนับสนุนการตัดสินใจจัดซื้อ
            </div>
        </div>
    </body>
    </html>
    """

    write_html(html_content)
    print("Dashboard Created Successfully")

except Exception as e:
    error_html = f"""
    <!DOCTYPE html>
    <html lang="th">
    <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <title>Dashboard Error</title>
    </head>
    <body style="font-family: Arial, sans-serif; padding: 32px; background:#f8f9fa;">
        <h1>พบข้อผิดพลาดในการสร้าง Dashboard</h1>
        <p>{str(e)}</p>
    </body>
    </html>
    """
    write_html(error_html)
    raise
