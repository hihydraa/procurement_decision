"""
ฟีเจอร์สำหรับตัดสินใจซื้อ — คำนวณจากข้อมูลที่ "รู้แล้ว ณ เวลาตัดสินใจ" เท่านั้น (ไม่มี look-ahead)
ใช้ร่วมกันทั้ง backtest (fit_model.py) และของจริง (main.py) เพื่อให้ตัวเลขที่วัดย้อนหลังกับที่ใช้จริงเป็นโค้ดชุดเดียวกัน

สคีมาของตารางนำเข้า (เหมือนกันทั้งแท็บในชีทและ CSV ย้อนหลัง):
  intraday    msg_date, msg_time, kind(TREND|HALF), b, d, wti, wti_chg, brent, brent_chg
              b/d = การประเมินการขยับของ MOPS เบนซิน/ดีเซลระหว่างวัน (USD/bbl, มีเครื่องหมาย)
  headsup     msg_date, msg_time, provider, direction(UP|DOWN), scope(ALL|DIESEL|GASOHOL), stated_time, has_rate, rates
  mops_close  msg_date, msg_time, mops_date, g95, g95_chg, diesel, diesel_chg    (MOPS ปิดตลาด ~18:00)
  fund        msg_date, msg_time, issue_no, effective_date, diesel_old, diesel_new, benzene_old, benzene_new
              (อัตราสุทธิ: เก็บเข้ากองทุน = บวก, กองทุนอุดหนุน = ลบ  บาท/ลิตร)
  prices      provider, warehouse, fuel(B7|G95), price, effective(datetime)
"""
import numpy as np
import pandas as pd

FUELS = ("B7", "G95")
# ฟีเจอร์ต่อชนิด: ดีเซล (B7) ใช้ค่า d ของ Trend Mops / diesel_chg ; แก๊สโซฮอล์ (G95) ใช้ค่า b / g95_chg
TREND_COL = {"B7": "d", "G95": "b"}
CLOSE_COL = {"B7": "diesel_chg", "G95": "g95_chg"}
HEADS_SCOPES = {"B7": ("DIESEL", "ALL"), "G95": ("GASOHOL", "ALL")}
FUND_COL = {"B7": "diesel", "G95": "benzene"}
HEADSUP_FROM_HOUR = 6       # ประกาศที่โพสต์ตั้งแต่ 06:00 ของวันตัดสินใจนับเป็นสัญญาณ (วันหยุดมักประกาศตอนสาย ไม่ใช่เย็น; วัดแล้วดีกว่าเริ่มนับ 12:00)
TREND_MAX_AGE_H = 8         # Trend ที่เก่ากว่านี้ไม่นับ (ปกติโพสต์ ~11:30 และ ~15:00)
MARKET_MAX_AGE_DAYS = 3     # ราคาของคลังที่ไม่อัปเดตเกินนี้ไม่นำมาหาราคาต่ำสุด


def add_ts(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty or "msg_date" not in out.columns or "msg_time" not in out.columns:
        # ว่าง หรือไม่ใช่ตารางฟีดที่คาด (เช่น gviz ส่งชีทอื่นมาแทนแท็บที่ไม่มีอยู่) — ถือว่าไม่มีข้อมูล ไม่ให้ทั้งระบบพัง
        return pd.DataFrame({"ts": pd.Series(dtype="datetime64[ns]")})
    out["ts"] = pd.to_datetime(out["msg_date"].astype(str) + " " + out["msg_time"].astype(str), errors="coerce")
    return out.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)


def _num(v):
    try:
        f = float(v)
        return f if np.isfinite(f) else np.nan
    except (TypeError, ValueError):
        return np.nan


class Tables:
    """รวมตารางทั้งหมดไว้ที่เดียว (ทำ ts ล่วงหน้าครั้งเดียว) — ส่งให้ build_features ซ้ำได้หลายครั้งตอน backtest"""

    def __init__(self, intraday=None, headsup=None, mops_close=None, fund=None):
        self.intraday = add_ts(intraday if intraday is not None else pd.DataFrame())
        self.headsup = add_ts(headsup if headsup is not None else pd.DataFrame())
        self.mops_close = add_ts(mops_close if mops_close is not None else pd.DataFrame())
        f = fund if fund is not None else pd.DataFrame()
        self.fund = add_ts(f)
        if not self.fund.empty:
            self.fund["effective_date"] = pd.to_datetime(self.fund["effective_date"], errors="coerce")


def build_features(asof: pd.Timestamp, t: Tables, fuel: str) -> dict:
    """ฟีเจอร์ของชนิดน้ำมัน fuel ณ เวลา asof — ใช้เฉพาะแถวที่โพสต์ก่อน/เท่ากับ asof"""
    day = asof.normalize()
    f = {"asof": asof}

    # 1) Trend Mops / Mops ครึ่งวัน ล่าสุดของวันนี้
    f["trend"], f["trend_age_h"] = np.nan, np.nan
    if not t.intraday.empty:
        w = t.intraday[(t.intraday.ts >= day) & (t.intraday.ts <= asof)]
        w = w[w[TREND_COL[fuel]].map(_num).notna()]
        if len(w):
            last = w.iloc[-1]
            age = (asof - last.ts).total_seconds() / 3600
            if age <= TREND_MAX_AGE_H:
                f["trend"], f["trend_age_h"] = _num(last[TREND_COL[fuel]]), age

    # 2) ประกาศล่วงหน้า "มีปรับขึ้น/ลง" ของวันนี้ นับเป็นจำนวนเจ้าที่ไม่ซ้ำ
    f["up_n"], f["dn_n"] = 0, 0
    if not t.headsup.empty:
        w = t.headsup[(t.headsup.ts >= day + pd.Timedelta(hours=HEADSUP_FROM_HOUR)) & (t.headsup.ts <= asof)]
        w = w[w.scope.isin(HEADS_SCOPES[fuel]) & (w.provider.astype(str) != "")]
        f["up_n"] = int(w[w.direction == "UP"].provider.nunique())
        f["dn_n"] = int(w[w.direction == "DOWN"].provider.nunique())

    # 3) MOPS ปิดตลาด: ค่าที่ประกาศล่าสุด และผลรวม 3 ครั้งล่าสุด
    f["close_last"], f["close_3"], f["close_age_h"] = np.nan, np.nan, np.nan
    if not t.mops_close.empty:
        w = t.mops_close[t.mops_close.ts <= asof]
        w = w[w[CLOSE_COL[fuel]].map(_num).notna()]
        if len(w):
            v = w[CLOSE_COL[fuel]].map(_num)
            f["close_last"], f["close_3"] = float(v.iloc[-1]), float(v.iloc[-3:].sum())
            f["close_age_h"] = (asof - w.iloc[-1].ts).total_seconds() / 3600

    # 4) กองทุน: ส่วนต่างที่ "ประกาศแล้ว" และมีผลพรุ่งนี้ (ประกาศออก ~20:00) กับระดับล่าสุด
    f["fund_next"], f["fund_level"] = 0.0, np.nan
    if not t.fund.empty:
        w = t.fund[t.fund.ts <= asof]
        col = FUND_COL[fuel]
        w = w[w[col + "_new"].map(_num).notna()]
        if len(w):
            f["fund_level"] = _num(w.iloc[-1][col + "_new"])
            nxt = w[w.effective_date == day + pd.Timedelta(days=1)]
            f["fund_next"] = float((nxt[col + "_new"].map(_num) - nxt[col + "_old"].map(_num)).sum())
    return f


class PriceBook:
    """ราคาต่ำสุดของตลาด ณ เวลาที่กำหนด = ต่ำสุดของราคาล่าสุดทุก (provider, warehouse) ที่ไม่เก่าเกิน MARKET_MAX_AGE_DAYS"""

    def __init__(self, prices: pd.DataFrame):
        p = prices.copy()
        p["effective"] = pd.to_datetime(p["effective"], errors="coerce")
        p["price"] = pd.to_numeric(p["price"], errors="coerce")
        p = p.dropna(subset=["effective", "price"])
        p = p[p.price.between(15, 70)]
        p["key"] = p.provider.astype(str) + "|" + p.warehouse.astype(str)
        self.groups = {}
        for (fuel, key), g in p.groupby(["fuel", "key"]):
            self.groups.setdefault(fuel, []).append(
                g.sort_values("effective")[["effective", "price"]].drop_duplicates("effective", keep="last"))

    def min_at(self, fuel: str, times) -> pd.Series:
        times = pd.DatetimeIndex(times)
        grid = pd.DataFrame({"t": times})
        order = grid.t.argsort()
        grid = grid.iloc[order]
        cols = [pd.merge_asof(grid, g, left_on="t", right_on="effective",
                              tolerance=pd.Timedelta(days=MARKET_MAX_AGE_DAYS)).price.to_numpy()
                for g in self.groups.get(fuel, [])]
        if not cols:
            return pd.Series(np.nan, index=times)
        with np.errstate(all="ignore"):
            v = np.nanmin(np.vstack(cols), axis=0)
        out = pd.Series(np.nan, index=times)
        out.iloc[order.to_numpy()] = v
        return out
