#!/usr/bin/env python3
"""
قمع وسام — ثلاث مراحل متتابعة على عملات USDT الفورية في بينانس.

    1) 4 ساعات : macdRSI < THR_4H
    2) ساعة    : تقاطع EMA12 فوق EMA26 خلال آخر MAX_BARS_1H شمعة
    3) 15 دقيقة: |macdRSIx − macdRSI| < MI، والاثنان صاعدان، والاثنان تحت CAP

كلّ إطار يُجلب من بينانس مستقلّاً، والشمعة الجارية تُهمَل دائماً.
النتائج تُرسل إلى تلغرام. هذا السكربت يُرسل ولا يقرأ أوامر البوت:
السكانر الآخر هو من يقرأ getUpdates، ولو قرأها الاثنان لضاعت الأوامر بينهما.
"""

import json
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import requests

# ════════════════════════════════════════════════════════════
#  الإعدادات — من متغيّرات البيئة في ملفّ العمل
# ════════════════════════════════════════════════════════════
TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT  = os.getenv("TG_CHAT", "")

THR_4H       = float(os.getenv("THR_4H", "50"))        # سقف macdRSI على 4 ساعات
MAX_BARS_1H  = int(os.getenv("MAX_BARS_1H", "10"))     # عمر التقاطع بالشمعات
MI           = float(os.getenv("CONV_MI", "10"))       # أقصى |mama| للتقارب
CAP          = float(os.getenv("CONV_CAP", "70"))      # سقف الخطّين في 15 دقيقة
LOOK         = int(os.getenv("CONV_LOOKBACK", "2"))    # كم شمعة نبحث فيها عن التقارب
MIN_VOLUME_M = float(os.getenv("MIN_VOLUME_M", "0.5")) # حجم 24 ساعة بالمليون
COOLDOWN_H   = float(os.getenv("COOLDOWN_HOURS", "6")) # لا نكرّر نفس العملة قبل ذلك

EMA_FAST, EMA_SLOW = 12, 26
WORKERS    = int(os.getenv("WORKERS", "8"))
STATE_FILE = os.getenv("FUNNEL_STATE", "funnel_state.json")

session = requests.Session()
session.headers["User-Agent"] = "wissam-funnel/1.0"


# ════════════════════════════════════════════════════════════
#  تلغرام — إرسال فقط
# ════════════════════════════════════════════════════════════
def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        print("[تلغرام غير مهيّأ]\n" + text)
        return "not configured"
    try:
        r = session.get(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            params={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML",
                    "disable_web_page_preview": "true"},
            timeout=25)
        j = r.json()
        return "sent" if j.get("ok") else str(j.get("description"))
    except Exception as e:
        return f"failed: {e}"


# ════════════════════════════════════════════════════════════
#  المؤشّرات — منقولة حرفياً عن Pine
# ════════════════════════════════════════════════════════════
def ema_series(v, p):
    out = [None] * len(v)
    if len(v) < p:
        return out
    prev = sum(v[:p]) / p
    out[p - 1] = prev
    k = 2 / (p + 1)
    for i in range(p, len(v)):
        prev = v[i] * k + prev * (1 - k)
        out[i] = prev
    return out


def rma_series(v, p):
    out = [None] * len(v)
    s = 0
    while s < len(v) and v[s] is None:
        s += 1
    if len(v) - s < p:
        return out
    prev = sum(v[s:s + p]) / p
    out[s + p - 1] = prev
    for i in range(s + p, len(v)):
        prev = (prev * (p - 1) + v[i]) / p
        out[i] = prev
    return out


def rsi_from(delta, length):
    gain = [None if d is None else max(d, 0.0) for d in delta]
    loss = [None if d is None else max(-d, 0.0) for d in delta]
    ag, al = rma_series(gain, length), rma_series(loss, length)
    out = []
    for i in range(len(delta)):
        if ag[i] is None or al[i] is None:
            out.append(None)
        else:
            rs = 100.0 if al[i] == 0 else ag[i] / al[i]
            out.append(100 - (100 / (1 + rs)))
    return out


def _macd(c, fast, slow):
    ef, es = ema_series(c, fast), ema_series(c, slow)
    return [None if (ef[i] is None or es[i] is None) else ef[i] - es[i] for i in range(len(c))]


def macd_rsi(c):                        # RSI(14) لخطّ MACD 15/30
    m = _macd(c, 15, 30)
    d = [None if (i == 0 or m[i] is None or m[i - 1] is None) else m[i] - m[i - 1]
         for i in range(len(m))]
    return rsi_from(d, 14)


def macd_rsi_x(c):                      # RSI(15) لخطّ MACD 12/26، مع nz على أوّل فرق
    m = _macd(c, 12, 26)
    d = []
    for i in range(len(m)):
        if m[i] is None:
            d.append(None)
        elif i > 0 and m[i - 1] is not None:
            d.append(m[i] - m[i - 1])
        else:
            d.append(0.0)
    return rsi_from(d, 15)


def cross_info(closes, fast, slow):
    ef, es = ema_series(closes, fast), ema_series(closes, slow)
    n = len(closes) - 1
    if ef[n] is None or es[n] is None:
        return None
    bars = level = None
    for i in range(n, 0, -1):
        if None in (ef[i], es[i], ef[i - 1], es[i - 1]):
            continue
        if ef[i] > es[i] and ef[i - 1] <= es[i - 1]:
            bars, level = n - i, es[i]          # سعر نقطة التقاطع
            break
    return {"bars": bars, "level": level, "above": ef[n] > es[n]}


def bg_parts(mr, mrx, i):
    """الأصفر = الخطّ صاعد، كما في المؤشّر: [1] > [2]"""
    a, b = i - 1, i - 2
    if b < 0:
        return None
    if None in (mr[i], mrx[i], mr[a], mr[b], mrx[a], mrx[b]):
        return None
    return {"mama": mrx[i] - mr[i], "mr": mr[i], "mrx": mrx[i],
            "yMr": mr[a] > mr[b], "yMrx": mrx[a] > mrx[b]}


def conv_ok(p):
    return (abs(p["mama"]) < MI and p["yMr"] and p["yMrx"]
            and p["mrx"] < CAP and p["mr"] < CAP)


# ════════════════════════════════════════════════════════════
#  بينانس
# ════════════════════════════════════════════════════════════
HOSTS = ["https://data-api.binance.vision",   # مرآة البيانات العامّة، لا تحجب خوادم GitHub
         "https://api.binance.com",
         "https://api-gcp.binance.com"]
API = HOSTS[0]


def pick_host():
    global API
    for h in HOSTS:
        try:
            if session.get(h + "/api/v3/ping", timeout=10).status_code == 200:
                API = h
                print("host:", h)
                return h
        except Exception:
            pass
    print("host: تعذّر الوصول إلى أيّ مضيف، نجرّب", API)
    return API


def jget(path, tries=3):
    last = None
    for a in range(tries):
        try:
            r = session.get(API + path, timeout=25)
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}"
            if r.status_code in (418, 429):
                time.sleep(2 + a * 3)
        except Exception as e:
            last = str(e)
            time.sleep(1 + a)
    raise RuntimeError(last or "فشل الطلب")


def closes_of(symbol, tf, limit):
    kl = jget(f"/api/v3/klines?symbol={symbol}&interval={tf}&limit={limit}")
    return [float(k[4]) for k in kl[:-1]]        # الشمعة الجارية تُهمَل


def universe():
    info = jget("/api/v3/exchangeInfo?permissions=SPOT")
    tick = jget("/api/v3/ticker/24hr")
    ok = {s["symbol"] for s in info["symbols"]
          if s["status"] == "TRADING" and s["quoteAsset"] == "USDT"
          and not s["baseAsset"].endswith(("UP", "DOWN", "BULL", "BEAR"))}
    tmap, pairs = {}, []
    for t in tick:
        if t["symbol"] in ok and float(t["quoteVolume"]) >= MIN_VOLUME_M * 1e6:
            tmap[t["symbol"]] = t
            pairs.append(t["symbol"])
    return pairs, tmap


def run_pool(items, work):
    out = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for r in ex.map(work, items):
            if r:
                out.append(r)
    return out


# ════════════════════════════════════════════════════════════
#  المراحل الثلاث
# ════════════════════════════════════════════════════════════
def stage_4h(sym):
    try:
        c = closes_of(sym, "4h", 300)
        if len(c) < 80:
            return None
        mr = macd_rsi(c)
        v = mr[len(c) - 1]
        if v is not None and v < THR_4H:
            return {"sym": sym, "mr4": v}
    except Exception:
        pass
    return None


def stage_1h(r):
    try:
        c = closes_of(r["sym"], "1h", 200)
        if len(c) < 60:
            return None
        ci = cross_info(c, EMA_FAST, EMA_SLOW)
        if ci and ci["above"] and ci["bars"] is not None and ci["bars"] <= MAX_BARS_1H:
            return {**r, "bars": ci["bars"], "stop": ci["level"]}
    except Exception:
        pass
    return None


def stage_15m(r):
    try:
        c = closes_of(r["sym"], "15m", 300)
        if len(c) < 80:
            return None
        mr, mrx = macd_rsi(c), macd_rsi_x(c)
        n = len(c) - 1
        for i in range(n, max(1, n - LOOK), -1):
            p = bg_parts(mr, mrx, i)
            if p and conv_ok(p):
                cur = bg_parts(mr, mrx, n) or p
                return {**r, "mama": abs(cur["mama"]), "mr15": cur["mr"], "mrx15": cur["mrx"]}
    except Exception:
        pass
    return None


# ════════════════════════════════════════════════════════════
#  الحالة — حتى لا تتكرّر نفس العملة كلّ ربع ساعة
# ════════════════════════════════════════════════════════════
def load_state():
    try:
        with open(STATE_FILE) as f:
            d = json.load(f)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def save_state(st):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(st, f)
    except Exception as e:
        print("لم تُحفظ الحالة:", e)


# ════════════════════════════════════════════════════════════
def main():
    started = time.time()
    pick_host()

    pairs, tmap = universe()
    print(f"العملات: {len(pairs)}  (حجم ≥ {MIN_VOLUME_M}M)")

    keep1 = run_pool(pairs, stage_4h)
    print(f"  1) 4س  macdRSI < {THR_4H}      → {len(keep1)}")

    keep2 = run_pool(keep1, stage_1h)
    print(f"  2) 1س  تقاطع ≤ {MAX_BARS_1H} شمعة  → {len(keep2)}")

    hits = run_pool(keep2, stage_15m)
    print(f"  3) 15د تقارب < {MI} تحت {CAP}  → {len(hits)}  ({time.time() - started:.0f}ث)")

    now = time.time()
    st = load_state()
    fresh = [h for h in hits
             if st.get(h["sym"]) is None or (now - st[h["sym"]]) >= COOLDOWN_H * 3600]
    for h in hits:
        st[h["sym"]] = now
    save_state({k: v for k, v in st.items() if now - v < 7 * 86400})

    if not fresh:
        print("لا جديد يُرسَل")
        return

    fresh.sort(key=lambda h: -float(tmap[h["sym"]]["quoteVolume"]))
    lines = []
    for h in fresh[:20]:
        t = tmap[h["sym"]]
        vol = float(t["quoteVolume"]) / 1e6
        chg = float(t["priceChangePercent"])
        entry = float(t["lastPrice"])
        risk = (h["stop"] - entry) / entry * 100 if entry and h.get("stop") else 0.0
        link = (f'<a href="https://www.tradingview.com/chart/?symbol=BINANCE:{h["sym"]}">'
                f'{h["sym"][:-4]}</a>')
        lines.append(
            f"• {link}  <code>${vol:,.1f}M · {chg:+.1f}%</code>\n"
            f"   دخول <code>{entry:.10g}</code> · وقف <code>{h['stop']:.10g}</code>"
            f" <code>({risk:+.1f}%)</code>\n"
            f"   <code>4h {h['mr4']:.0f} · {h['bars']}b · |mama| {h['mama']:.1f}"
            f" · 15m {h['mr15']:.0f}/{h['mrx15']:.0f}</code>")

    more = f"\n…و{len(fresh) - 20} أخرى" if len(fresh) > 20 else ""
    msg = (f"🔻 <b>قمع وسام</b>  <code>{len(pairs)} عملة → {len(hits)}</code>\n\n"
           + "\n".join(lines) + more)
    print("تلغرام:", tg_send(msg))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        tg_send("⚠️ <b>خطأ في قمع وسام</b>\n<code>"
                + traceback.format_exc()[-600:].replace("<", "&lt;") + "</code>")
        sys.exit(1)
