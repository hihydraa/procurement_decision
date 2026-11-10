"""
อ่านแท็บรายวันของไฟล์ PO รายเดือน (Google Sheet "PO 2569-MM") เป็นตารางรายการซื้อ (หนึ่งแถว = ใบสั่งหนึ่งใบ × ชนิดน้ำมัน) — ฟังก์ชันล้วน ไม่เรียก API

โครงสร้างบล็อกใบสั่ง (จุดยึด = แถวที่คอลัมน์ C เป็น "จำนวนลิตร" แถว r) ตามที่เห็นในไฟล์ ก.ย.–พ.ย. 2569 :
  r-1 : C="จ๊อบเปอร์", D..H = ผู้ขายของแต่ละชนิด (B7,B20,91,95,E20)
  r   : A=จ๊อบเปอร์, B=เบอร์รถ, D..H=จำนวนลิตร, M=หมายเหตุราคา (เช่น "ราคาวันที่ 14 โหลดน้ำมันหลังเที่ยงคืน"), O=ข้อความประกาศปรับราคา
  r+1 : B=คนขับ, C="ราคา/ลิตร", D..H=ราคา, O=ผู้ผลิต (เช่น ⭐สตาร์ ฟูเอลส์ (SFL), 🚩ไออาร์ - iRPC)
  r+2 : A=คลัง (เช่น IR ระยอง, ดาว สระบุรี), B=ทะเบียน, C="จำนวนเงิน"
  r+3..r+6 : C=ปลายทาง (มุกดาหาร/ยางตลาด/กระนวน/วานรนิวาส), D..H=ลิตรที่ส่ง, M="ค่าขนส่ง x.xx"
  คอลัมน์ O ของ r+2..r+6 มีบรรทัดรหัสน้ำมัน (เช่น D/G91/G95) และบรรทัดราคาประกาศของคลัง (เช่น "ระยอง : 39.23/37.44/37.62")
ตัวเลขในชีทมักเป็นข้อความ เช่น "  17,000 " ; "-" = ไม่มี
"""
import re
from datetime import date

FUEL_COLS = ["B7", "B20", "G91", "G95", "E20"]            # คอลัมน์ D..H ตามหัวตาราง B7,B20,91,95,E20
LIST_CODE = {"D": "B7", "B7": "B7", "B20": "B20", "G91": "G91", "G95": "G95", "E20": "E20", "ULG": "ULG"}
DESTS = ("มุกดาหาร", "ยางตลาด", "กระนวน", "วานรนิวาส")


def num(v):
    """'  17,000 ' → 17000.0 ; '-', '', None → None"""
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).replace(",", "").strip()
    if s in ("", "-"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def g(rows, r, c):
    return rows[r][c] if 0 <= r < len(rows) and c < len(rows[r]) else ""


def tab_date(rows, month_label: str, tab: str):
    """วันที่จาก A1 'วันที่ 08/11/2569' (พ.ศ.) ; ถ้าไม่มีใช้ชื่อไฟล์เดือน 'YYYY-MM' (พ.ศ.) + ชื่อแท็บ ; ไม่ได้ → None"""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", str(g(rows, 0, 0)))
    try:
        if m:
            return date(int(m.group(3)) - 543, int(m.group(2)), int(m.group(1)))
        y, mo = month_label.split("-")
        return date(int(y) - 543, int(mo), int(re.match(r"\d+", tab).group()))
    except (ValueError, AttributeError):
        return None


def parse_list_line(codes_text: str, prices_text: str) -> dict:
    """'⛽ : D/G91/G95' + 'ระยอง : 39.23/37.44/37.62' → {'B7': 39.23, 'G91': 37.44, 'G95': 37.62} ; ไม่ตรงกัน → {}"""
    codes = [LIST_CODE.get(c.strip().upper()) for c in re.sub(r"^[^:]*:", "", codes_text).strip().split("/")] if ":" in codes_text else \
            [LIST_CODE.get(c.strip().upper()) for c in codes_text.strip().split("/")]
    nums = [num(x) for x in re.sub(r"^[^:]*:", "", prices_text).strip().split("/")]
    if not codes or len(codes) != len(nums) or any(c is None for c in codes) or any(n is None for n in nums):
        return {}
    return dict(zip(codes, nums))


def depot_list(rows, r) -> dict:
    """ราคาประกาศของคลังในบล็อก (คอลัมน์ O แถว r+2..r+6) : หาบรรทัดรหัสน้ำมันแล้วบรรทัดราคาถัดไป"""
    texts = [str(g(rows, i, 14)).strip() for i in range(r + 2, r + 7)]
    for k, t in enumerate(texts[:-1]):
        body = re.sub(r"^[^:]*:", "", t).strip() if ":" in t else t
        if body and re.fullmatch(r"[A-Za-z0-9]+(/[A-Za-z0-9]+)*", body) and any(c in LIST_CODE for c in body.upper().split("/")):
            return parse_list_line(t, texts[k + 1])
    return {}


def parse_block(rows, r, d, tab: str, month_label: str) -> list:
    """แถวรายการซื้อของบล็อกที่ยึดที่แถว r ; [] ถ้าไม่มีลิตร"""
    liters = [num(g(rows, r, 3 + i)) for i in range(5)]
    if not any(liters):
        return []
    price = [num(g(rows, r + 1, 3 + i)) for i in range(5)]
    seller = [str(g(rows, r - 1, 3 + i)).strip() for i in range(5)]
    lst = depot_list(rows, r)
    transport, dest_rows = None, []
    for i in range(r + 3, r + 7):
        name = str(g(rows, i, 2)).strip()
        if name in DESTS:
            dest_rows.append((name, [num(g(rows, i, 3 + k)) for k in range(5)]))
        m = re.search(r"ค่าขนส่ง\s*([0-9.]+)", str(g(rows, i, 12)))
        if m:
            transport = float(m.group(1))
    out = []
    for k, fuel in enumerate(FUEL_COLS):
        if not liters[k]:
            continue
        dests = [n for n, ls in dest_rows if ls[k]]
        out.append({"date": d, "tab": tab, "month": month_label, "row": r + 1, "jobber": str(g(rows, r, 0)).strip(), "seller": seller[k],
                    "truck": str(g(rows, r, 1)).strip(), "depot": str(g(rows, r + 2, 0)).strip(), "plate": str(g(rows, r + 2, 1)).strip(),
                    "provider": str(g(rows, r + 1, 14)).strip(), "fuel": fuel, "liters": liters[k], "price": price[k], "list_price": lst.get(fuel),
                    "dest": dests[0] if len(dests) == 1 else ("+".join(dests) if dests else ""), "transport": transport,
                    "price_note": str(g(rows, r, 12)).strip(), "change_note": str(g(rows, r, 14)).strip()})
    return out


def parse_tab(rows, month_label: str, tab: str) -> list:
    d = tab_date(rows, month_label, tab)
    if d is None:
        return []
    out = []
    for r in range(len(rows)):
        if str(g(rows, r, 2)).strip() == "จำนวนลิตร":
            out.extend(parse_block(rows, r, d, tab, month_label))
    return out
