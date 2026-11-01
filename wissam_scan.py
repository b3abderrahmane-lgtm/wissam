#!/usr/bin/env python3
"""
Wissam Cascade — server-side scanner for GitHub Actions.

Scans every Binance USDT spot pair on 15m, 1h, 4h and 1d, finds the five cascades
(a convergence on one timeframe plus a fresh EMA cross on a higher one) and sends
whatever is new to Telegram.

Runs anywhere Python runs. Only dependency: requests.
"""

import json
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor

import requests

# ════════════════════════════════════════════════════════════
#  CONFIG — environment first, so nothing secret lives in the file
# ════════════════════════════════════════════════════════════

TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT  = os.getenv("TG_CHAT", "")

MIN_VOLUME_USD = float(os.getenv("MIN_VOLUME", "500000"))   # 0.5M default

EMA_FAST      = int(os.getenv("EMA_FAST", "12"))
EMA_SLOW      = int(os.getenv("EMA_SLOW", "26"))
MAX_BARS      = int(os.getenv("MAX_BARS", "10"))            # 1h and 4h crosses
MAX_BARS_1D   = int(os.getenv("MAX_BARS_1D", "2"))          # daily cross
CONV_LOOKBACK = int(os.getenv("CONV_LOOKBACK", "2"))
CONV_MI       = float(os.getenv("CONV_MI", "10"))
CONV_CAP      = float(os.getenv("CONV_CAP", "40"))

COOLDOWN_HOURS = float(os.getenv("COOLDOWN_HOURS", "6"))
WORKERS        = int(os.getenv("WORKERS", "8"))
SEND_EMPTY     = os.getenv("SEND_EMPTY", "0") == "1"
STATE_FILE     = os.getenv("STATE_FILE", "state.json")

# Binance blocks many datacentre IPs on api.binance.com; the public data mirror does not.
HOSTS = [h for h in os.getenv(
    "BINANCE_HOSTS",
    "https://data-api.binance.vision,https://api.binance.com,https://api-gcp.binance.com"
).split(",") if h]

LEV_RE = re.compile(r"(UP|DOWN|BULL|BEAR)$")
STABLE = {
    "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "USD1", "RLUSD",
    "EUR", "TRY", "BRL", "AEUR", "XUSD", "U", "USDS",
}

session = requests.Session()
session.headers["User-Agent"] = "wissam-cascade/1.0"
HOST = None          # resolved on first use


# ════════════════════════════════════════════════════════════
#  Pine-equivalent series math
# ════════════════════════════════════════════════════════════

def ema_series(v, p):
    """ta.ema — seeded with SMA(p)."""
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
    """ta.rma — Wilder smoothing, seeded with SMA(p), skipping leading None."""
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
    for g, l in zip(ag, al):
        if g is None or l is None:
            out.append(None)
            continue
        rs = 100.0 if l == 0 else g / l
        out.append(100 - (100 / (1 + rs)))
    return out


def macd_line(c, fast, slow):
    ef, es = ema_series(c, fast), ema_series(c, slow)
    return [(a - b) if (a is not None and b is not None) else None for a, b in zip(ef, es)]


def macd_rsi(c):
    """macdRSI — RSI(14) of the 15/30 MACD line."""
    m = macd_line(c, 15, 30)
    d = [(m[i] - m[i - 1]) if (i > 0 and m[i] is not None and m[i - 1] is not None) else None
         for i in range(len(m))]
    return rsi_from(d, 14)


def macd_rsi_x(c):
    """macdRSIx — RSI(15) of the 12/26 MACD line, nz() on the first delta."""
    m = macd_line(c, 12, 26)
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
    bars = None
    for i in range(n, 0, -1):
        if None in (ef[i], es[i], ef[i - 1], es[i - 1]):
            continue
        if ef[i] > es[i] and ef[i - 1] <= es[i - 1]:
            bars = n - i
            break
    return {"bars": bars, "gap": (ef[n] - es[n]) / es[n] * 100, "above": ef[n] > es[n]}


def bg_parts(mr, mrx, i):
    """The four bgCondition inputs on bar i, comparing [1] against [2] as the Pine source does."""
    a, b = i - 1, i - 2
    if b < 0:
        return None
    if None in (mr[i], mrx[i], mr[a], mr[b], mrx[a], mrx[b]):
        return None
    return {"mama": mrx[i] - mr[i], "mr": mr[i], "mrx": mrx[i],
            "c_mr": mr[a] > mr[b], "c_mrx": mrx[a] > mrx[b]}


def bg_all(p):
    return abs(p["mama"]) < CONV_MI and p["c_mr"] and p["c_mrx"] and p["mrx"] < CONV_CAP


# ════════════════════════════════════════════════════════════
#  Binance
# ════════════════════════════════════════════════════════════

def pick_host():
    """First host that answers; Binance geo-blocks some of them from datacentres."""
    global HOST
    if HOST:
        return HOST
    for h in HOSTS:
        try:
            r = session.get(h + "/api/v3/ping", timeout=12)
            if r.ok:
                HOST = h
                print(f"host: {h}")
                return HOST
        except Exception as e:
            print(f"host {h} unreachable: {e}")
    raise RuntimeError("no Binance host reachable: " + ", ".join(HOSTS))


def jget(path, tries=3):
    base = pick_host()
    for attempt in range(tries):
        try:
            r = session.get(base + path, timeout=25)
            if r.status_code in (429, 418):
                time.sleep(5 * (attempt + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


def universe():
    info = jget("/api/v3/exchangeInfo")
    tick = jget("/api/v3/ticker/24hr")
    tmap = {t["symbol"]: t for t in tick}

    pairs = []
    for s in info["symbols"]:
        if (s["quoteAsset"] != "USDT" or s["status"] != "TRADING"
                or not s.get("isSpotTradingAllowed")):
            continue
        base = s["baseAsset"]
        if LEV_RE.search(base) or base in STABLE:
            continue
        t = tmap.get(s["symbol"])
        if not t or float(t["quoteVolume"]) < MIN_VOLUME_USD:
            continue
        pairs.append(s["symbol"])

    pairs.sort(key=lambda p: -float(tmap[p]["quoteVolume"]))
    return pairs, tmap


def closes_of(symbol, tf, limit):
    kl = jget(f"/api/v3/klines?symbol={symbol}&interval={tf}&limit={limit}")
    return [float(k[4]) for k in kl[:-1]]        # drop the candle still forming


# ════════════════════════════════════════════════════════════
#  One stage = one timeframe, fetched on its own
# ════════════════════════════════════════════════════════════

def run_stage(tf, want, pairs):
    need_conv = "conv" in want
    need_cross = "cross" in want
    limit = max(300 if need_conv else 0, max(EMA_SLOW * 4, 60) if need_cross else 0)
    out = {"conv": {}, "cross": {}}

    def work(sym):
        try:
            c = closes_of(sym, tf, limit)
            if need_cross:
                ci = cross_info(c, EMA_FAST, EMA_SLOW)
                cap = MAX_BARS_1D if tf == "1d" else MAX_BARS
                if ci and ci["bars"] is not None and ci["bars"] <= cap and ci["above"]:
                    out["cross"][sym] = {"bars": ci["bars"], "gap": ci["gap"]}
            if need_conv and len(c) >= 80:
                mr, mrx = macd_rsi(c), macd_rsi_x(c)
                n = len(c) - 1
                hit = False
                for i in range(n, max(1, n - CONV_LOOKBACK), -1):
                    p = bg_parts(mr, mrx, i)
                    if p and bg_all(p):
                        hit = True
                        break
                if hit:
                    cur = bg_parts(mr, mrx, n)
                    out["conv"][sym] = {"mama": abs(cur["mama"]), "mrx": cur["mrx"]}
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, pairs))
    return out


# ════════════════════════════════════════════════════════════
#  Cascades
# ════════════════════════════════════════════════════════════

CASCADES = [
    ("15m→1h", "conv15", "cross1h"),
    ("1h→4h",  "conv1h", "cross4h"),
    ("4h→1d",  "conv4h", "cross1d"),
    ("15m→4h", "conv15", "cross4h"),
    ("1h→1d",  "conv1h", "cross1d"),
]


def find_cascades(sets):
    found = {}
    for tag, ckey, xkey in CASCADES:
        for sym in set(sets[ckey]) & set(sets[xkey]):
            found.setdefault(sym, []).append(tag)
    return found


# ════════════════════════════════════════════════════════════
#  Telegram — plain GET, nothing exotic to be blocked
# ════════════════════════════════════════════════════════════

def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        print("[telegram not configured]\n" + text)
        return "not configured"
    try:
        r = session.get(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            params={"chat_id": TG_CHAT, "text": text,
                    "parse_mode": "HTML", "disable_web_page_preview": "true"},
            timeout=20)
        j = r.json()
        return "sent" if j.get("ok") else str(j.get("description"))
    except Exception as e:
        return "failed: " + str(e)


# ════════════════════════════════════════════════════════════
#  State — stops the same pair arriving every quarter of an hour
# ════════════════════════════════════════════════════════════

def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(st):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(st, f)
    except Exception as e:
        print("state not saved:", e)


# ════════════════════════════════════════════════════════════

def main():
    started = time.time()
    pairs, tmap = universe()
    print(f"universe: {len(pairs)} pairs")

    s15 = run_stage("15m", ["conv"], pairs)
    s1h = run_stage("1h", ["conv", "cross"], pairs)
    s4h = run_stage("4h", ["conv", "cross"], pairs)
    s1d = run_stage("1d", ["cross"], pairs)

    sets = {
        "conv15":  s15["conv"],
        "conv1h":  s1h["conv"],
        "cross1h": s1h["cross"],
        "conv4h":  s4h["conv"],
        "cross4h": s4h["cross"],
        "cross1d": s1d["cross"],
    }
    for k, v in sets.items():
        print(f"  {k}: {len(v)}")

    hits = find_cascades(sets)
    print(f"cascades: {len(hits)}  ({time.time() - started:.0f}s)")

    st = load_state()
    now = time.time()
    fresh = []
    pending = {}
    for sym, tags in hits.items():
        key = "v2|" + sym + "|" + ",".join(sorted(tags))
        last = st.get(key)
        if last is None or (now - last) >= COOLDOWN_HOURS * 3600:
            fresh.append((sym, tags))
            pending[key] = now

    # forget anything older than a day so the file cannot grow without end
    st = {k: v for k, v in st.items() if now - v < 86400}

    if fresh:
        fresh.sort(key=lambda x: -float(tmap[x[0]]["quoteVolume"]))
        lines = []
        for sym, tags in fresh[:20]:
            t = tmap[sym]
            vol = float(t["quoteVolume"]) / 1e6
            chg = float(t["priceChangePercent"])
            name = sym[:-4]
            link = f'<a href="https://www.tradingview.com/chart/?symbol=BINANCE:{sym}">{name}</a>'
            lines.append(f"• {link}  {' · '.join(tags)}\n   ${vol:,.1f}M · {chg:+.1f}% · {t['lastPrice']}")
        more = f"\n…and {len(fresh) - 20} more" if len(fresh) > 20 else ""
        msg = (f"🟢 <b>Wissam Cascade</b>  <code>{len(pairs)} pairs</code>\n\n"
               + "\n".join(lines) + more)
        result = tg_send(msg)
        print("telegram:", result)
        if result == "sent":
            st.update(pending)      # remember only pairs that really reached Telegram
        else:
            print("not marked as sent - will retry next run")
    elif SEND_EMPTY:
        print("telegram:", tg_send(f"<i>Wissam Cascade — nothing new ({len(hits)} active)</i>"))
    else:
        print("nothing new to send")

    save_state(st)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        tg_send("⚠️ <b>Wissam Cascade error</b>\n<code>"
                + traceback.format_exc()[-400:] + "</code>")
        sys.exit(1)
