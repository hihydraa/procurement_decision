"""
โมเดลความน่าจะเป็นของ "ราคาขึ้น/ลงข้ามคืน" + กฎแปลงเป็นคำแนะนำ

ออกแบบให้อธิบายได้: โลจิสติกส์เล็ก ๆ ที่มีไม่กี่ฟีเจอร์ (ดู FEATURES) สัมประสิทธิ์เก็บใน model_params.json
ที่ fit_model.py สร้างจากข้อมูลย้อนหลังและวัดแบบ walk-forward — ไม่ใช้ไลบรารีเพิ่ม (numpy ล้วน)
"""
import json
import os

import numpy as np

FEATURES = ["trend", "trend_missing", "up_n", "dn_n", "close_last", "close_sign", "close_3", "fund_next"]
CLIPS = {"trend": (-4.0, 4.0), "up_n": (0, 4), "dn_n": (0, 4), "close_last": (-8.0, 8.0), "close_3": (-15.0, 15.0), "fund_next": (-3.0, 3.0)}
RISE_THB = 0.20      # ขยับเกินนี้ (บาท/ลิตร) ถึงนับว่า "ขึ้น/ลง" จริง
PARAMS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_params.json")


def vectorize(f: dict) -> np.ndarray:
    def val(k, default=0.0):
        v = f.get(k)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return default
        lo, hi = CLIPS.get(k, (-np.inf, np.inf))
        return float(min(max(v, lo), hi))

    trend_missing = 1.0 if (f.get("trend") is None or (isinstance(f.get("trend"), float) and np.isnan(f.get("trend")))) else 0.0
    cl = val("close_last")
    close_sign = 0.0 if abs(cl) < 0.3 else (1.0 if cl > 0 else -1.0)      # ทิศทาง MOPS ปิดล่าสุด (ไม่สนขนาด)
    return np.array([val("trend"), trend_missing, val("up_n"), val("dn_n"), cl, close_sign, val("close_3"), val("fund_next")], dtype=float)


def fit_logistic(X: np.ndarray, y: np.ndarray, l2: float = 2.0, iters: int = 60):
    """ridge logistic regression (Newton) — คืน (w, b, mu, sd) โดย standardize ฟีเจอร์ที่ไม่ใช่ binary"""
    mu, sd = X.mean(axis=0), X.std(axis=0)
    sd[sd < 1e-9] = 1.0
    Z = (X - mu) / sd
    A = np.column_stack([Z, np.ones(len(Z))])
    w = np.zeros(A.shape[1])
    R = np.eye(A.shape[1]) * l2
    R[-1, -1] = 0.0
    for _ in range(iters):
        p = 1.0 / (1.0 + np.exp(-np.clip(A @ w, -30, 30)))
        g = A.T @ (p - y) + R @ w
        H = A.T @ (A * (p * (1 - p))[:, None]) + R + 1e-6 * np.eye(A.shape[1])
        step = np.linalg.solve(H, g)
        w -= step
        if np.abs(step).max() < 1e-7:
            break
    return {"w": w[:-1].tolist(), "b": float(w[-1]), "mu": mu.tolist(), "sd": sd.tolist()}


def predict_proba(m: dict, X: np.ndarray) -> np.ndarray:
    Z = (np.atleast_2d(X) - np.array(m["mu"])) / np.array(m["sd"])
    return 1.0 / (1.0 + np.exp(-np.clip(Z @ np.array(m["w"]) + m["b"], -30, 30)))


def load_params(path: str = PARAMS_PATH):
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


# ------------------------------------------------------------------ คำแนะนำ
ACTIONS = {
    "URGENT": ("เร่งซื้อ", "success"),
    "GRADUAL": ("ทยอยซื้อ", "warning"),
    "NORMAL": ("ซื้อปกติ", "primary"),
    "WAIT": ("ชะลอการซื้อ", "danger"),
    "NO_DATA": ("ข้อมูลไม่พอ", "secondary"),
}
DEFAULT_THRESHOLDS = {"urgent": 0.45, "gradual": 0.30, "wait_fall": 0.35, "wait_max_rise": 0.20}


def choose_action(p_rise: float, p_fall: float, th: dict = None) -> str:
    th = th or DEFAULT_THRESHOLDS
    if p_rise >= th["urgent"]:
        return "URGENT"
    if p_rise >= th["gradual"]:
        return "GRADUAL"
    if p_fall >= th["wait_fall"] and p_rise <= th["wait_max_rise"]:
        return "WAIT"
    return "NORMAL"


def tune_thresholds(p_rise, p_fall, gain_buy_now):
    """เลือกเกณฑ์ที่ให้ผลรวมกำไรสูงสุดบนชุดจูน
    gain_buy_now = ราคาพรุ่งนี้ - ราคาวันนี้ (บาท/ลิตร): ถ้าเร่งซื้อวันนี้ได้ประโยชน์เท่ากับค่านี้ (ติดลบถ้าราคาลงจริง)
    ถ้าชะลอ (ซื้อพรุ่งนี้) ได้ประโยชน์ = -gain_buy_now ; ทยอยซื้อ = ครึ่งหนึ่งของเร่งซื้อ ; ซื้อปกติ = 0"""
    best, best_u = dict(DEFAULT_THRESHOLDS), -np.inf
    for urgent in (0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60):
        for gradual in (0.15, 0.20, 0.25, 0.30, 0.35, 0.40):
            if gradual >= urgent:
                continue
            for wait_fall in (0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50):
                for wait_max in (0.10, 0.15, 0.20, 0.25, 0.30):
                    th = {"urgent": urgent, "gradual": gradual, "wait_fall": wait_fall, "wait_max_rise": wait_max}
                    u = 0.0
                    for pr, pf, g in zip(p_rise, p_fall, gain_buy_now):
                        a = choose_action(pr, pf, th)
                        u += g if a == "URGENT" else 0.5 * g if a == "GRADUAL" else -g if a == "WAIT" else 0.0
                    if u > best_u:
                        best, best_u = th, u
    return best, best_u


def utility(action: str, gain_buy_now: float) -> float:
    return gain_buy_now if action == "URGENT" else 0.5 * gain_buy_now if action == "GRADUAL" else -gain_buy_now if action == "WAIT" else 0.0
