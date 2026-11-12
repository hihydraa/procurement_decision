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
        self._prices, self._known = None, {}

    def set_prices(self, prices: pd.DataFrame) -> "Tables":
        """ผูกตารางราคา (ข้อมูลย้อนหลังหรือฟีด Entry_Prices) เพื่อใช้ฟีเจอร์ชุด v2"""
        self._prices, self._known = prices, {}
        return self

    def known(self, fuel: str) -> "KnownPrices":
        if fuel not in self._known:
            self._known[fuel] = KnownPrices(self._prices, fuel)
        return self._known[fuel]

    def has_prices(self, fuel: str, asof: pd.Timestamp, max_age_h: float = 36.0) -> bool:
        """มีราคาที่โพสต์ภายใน max_age_h ก่อน asof หรือไม่ (ฟีดราคาสดพอสำหรับชุด v2)"""
        kp = self.known(fuel)
        if kp.empty:
            return False
        snap = kp.snapshot(asof)
        return (not snap.empty) and (asof - snap.msg.max()) <= pd.Timedelta(hours=max_age_h)


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


def _posted_ts(p: pd.DataFrame) -> pd.Series:
    """เวลาที่ราคาถูก "โพสต์" : msg_date+msg_time (ข้อมูลย้อนหลัง) → posted (ฟีดในชีท) → ถ้าไม่มีทั้งคู่ใช้เวลามีผล (ประมาณ : ราคาที่ประกาศล่วงหน้าจะถูกมองว่ายังไม่รู้)"""
    ts = pd.Series(pd.NaT, index=p.index, dtype="datetime64[ns]")
    if "msg_date" in p.columns and "msg_time" in p.columns:
        ts = pd.to_datetime(p["msg_date"].astype(str) + " " + p["msg_time"].astype(str), errors="coerce")
    if "posted" in p.columns:
        ts = ts.fillna(pd.to_datetime(p["posted"], errors="coerce"))
    return ts.fillna(pd.to_datetime(p["effective"], errors="coerce"))


class KnownPrices:
    """ราคาที่ "รู้ ณ เวลา asof" ของชนิดน้ำมันหนึ่ง : ใช้เฉพาะแถวที่โพสต์ <= asof แล้วถามราคาล่าสุดต่อ (ผู้ขาย, คลัง) ณ เวลา t ใด ๆ
    (รวมเวลาในอนาคตที่ประกาศไว้แล้ว เช่น โพสต์ 17:30 มีผล 23:59) — ต่างจาก PriceBook ที่ดูเฉพาะเวลามีผลและใช้เป็นผลจริง"""

    def __init__(self, prices: pd.DataFrame, fuel: str):
        cols = ["msg", "eff", "price", "key"]
        if prices is None or prices.empty or not {"provider", "warehouse", "fuel", "price", "effective"}.issubset(prices.columns):
            self.p = pd.DataFrame(columns=cols)
        else:
            p = prices[prices.fuel.astype(str) == fuel].copy()
            p["msg"] = _posted_ts(p)
            p["eff"] = pd.to_datetime(p["effective"], errors="coerce")
            p["price"] = pd.to_numeric(p["price"], errors="coerce")
            p = p.dropna(subset=["msg", "eff", "price"])
            p = p[p.price.between(15, 70)]
            p["key"] = p.provider.astype(str) + "|" + p.warehouse.astype(str)
            self.p = p.sort_values("msg")[cols].reset_index(drop=True)
        self.msg = self.p.msg.to_numpy(dtype="datetime64[ns]")

    @property
    def empty(self) -> bool:
        return self.p.empty

    def snapshot(self, asof: pd.Timestamp) -> pd.DataFrame:
        n = int(np.searchsorted(self.msg, np.datetime64(asof), side="right"))
        lo = int(np.searchsorted(self.msg, np.datetime64(asof - pd.Timedelta(days=14)), side="left"))
        return self.p.iloc[lo:n]

    @staticmethod
    def latest(snap: pd.DataFrame, t: pd.Timestamp) -> pd.Series:
        w = snap[(snap.eff <= t) & (snap.eff >= t - pd.Timedelta(days=MARKET_MAX_AGE_DAYS))]
        if w.empty:
            return pd.Series(dtype=float)
        return w.sort_values(["eff", "msg"]).groupby("key").price.last()


PRICE_FEATURES = ("pend", "pend_up_n", "pend_dn_n", "mom1", "mom3", "mom5", "mom10", "intra", "disp", "n_series")


def price_features(kp: KnownPrices, asof: pd.Timestamp, hz: int) -> dict:
    """ฟีเจอร์จากราคาที่รู้ ณ asof :
    pend = ราคาต่ำสุดของตลาด ณ asof+hz ตามที่ประกาศไว้แล้ว − ราคาต่ำสุดตอนนี้ ; pend_up_n/dn_n = จำนวน (ผู้ขาย, คลัง) ที่ประกาศขึ้น/ลงแล้ว
    mom1/3/5/10 = ราคาตอนนี้ − ราคาเมื่อ 1/3/5/10 วันก่อน ; intra = ราคาตอนนี้ − ราคา 06:00 วันนี้ ; disp = ค่ากลางของตลาด − ราคาต่ำสุด"""
    out = {k: np.nan for k in PRICE_FEATURES}
    snap = kp.snapshot(asof)
    now = kp.latest(snap, asof)
    if now.empty:
        return out
    p0 = float(now.min())
    fut = kp.latest(snap, asof + pd.Timedelta(hours=hz))
    out["pend"] = float(fut.min()) - p0 if len(fut) else 0.0
    both = pd.concat([now.rename("a"), fut.rename("b")], axis=1).dropna()
    d = both.b - both.a
    out["pend_up_n"], out["pend_dn_n"] = int((d >= 0.05).sum()), int((d <= -0.05).sum())
    for name, days in (("mom1", 1), ("mom3", 3), ("mom5", 5), ("mom10", 10)):
        prev = kp.latest(snap, asof - pd.Timedelta(days=days))
        out[name] = p0 - float(prev.min()) if len(prev) else np.nan
    morn = kp.latest(snap, asof.normalize() + pd.Timedelta(hours=6))
    out["intra"] = p0 - float(morn.min()) if len(morn) else np.nan
    out["disp"] = float(now.median()) - p0
    out["n_series"] = int(len(now))
    return out


def build_features_v2(asof: pd.Timestamp, t: "Tables", fuel: str, hz: int) -> dict:
    """ฟีเจอร์ชุด v2 ของโมเดลแนวโน้ม = ชุดเดิม + ราคาที่รู้ ณ asof + ผลรวม MOPS 5/10 ครั้ง + วันในสัปดาห์ ; ต้องมี t.known(fuel)"""
    f = build_features(asof, t, fuel)
    f.update(price_features(t.known(fuel), asof, hz))
    f["close_5"], f["close_10"] = np.nan, np.nan
    if not t.mops_close.empty:
        w = t.mops_close[t.mops_close.ts <= asof]
        v = w[CLOSE_COL[fuel]].map(_num).dropna()
        if len(v):
            f["close_5"], f["close_10"] = float(v.iloc[-5:].sum()), float(v.iloc[-10:].sum())
    f["dow"] = int(asof.dayofweek)
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
