#!/usr/bin/env python3
"""
Wissam Cascade — server-side scanner, controlled from Telegram.

Scans every Binance USDT spot pair on 15m, 1h, 4h and 1d, finds the five cascades
(a convergence on one timeframe plus a fresh EMA cross on a higher one) and sends
whatever is new to Telegram.

Settings live in config.json next to this file and can be changed from the phone:

    /set mi=8 cap=45 vol=1 bars=6 bars1d=2 look=2 cooldown=4
    /status      show the current settings
    /pause       stop scanning (alerts off, the job still runs)
    /resume      start again
    /scan        ignore the cooldown on the next run

Only dependency: requests.
"""

import csv
import json
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import requests

TG_TOKEN = os.getenv("TG_TOKEN", "")
TG_CHAT  = os.getenv("TG_CHAT", "")

CONFIG_FILE = os.getenv("CONFIG_FILE", "config.json")
STATE_FILE  = os.getenv("STATE_FILE", "state.json")
JOURNAL_FILE = os.getenv("JOURNAL_FILE", "journal.csv")
WORKERS     = int(os.getenv("WORKERS", "8"))

# Binance geo-blocks many datacentre IPs on api.binance.com; the public data mirror does not.
HOSTS = [h for h in os.getenv(
    "BINANCE_HOSTS",
    "https://data-api.binance.vision,https://api.binance.com,https://api-gcp.binance.com"
).split(",") if h]

# every setting the phone can change, with its default and how it is read
SETTINGS = {
    "mi":       ("CONV_MI",       10.0,  float),   # |mama| must be below this
    "cap":      ("CONV_CAP",      40.0,  float),   # macdRSIx must be below this
    "look":     ("CONV_LOOKBACK",  2,    int),     # convergence within the last N closed bars
    "bars":     ("MAX_BARS",      10,    int),     # bars since cross, 1h and 4h
    "bars1d":   ("MAX_BARS_1D",    2,    int),     # bars since cross, daily
    "vol":      ("MIN_VOLUME_M",   0.5,  float),   # minimum 24h volume, in millions
    "cooldown": ("COOLDOWN_HOURS", 6.0,  float),   # hours before the same pair repeats
}
EMA_FAST, EMA_SLOW = 12, 26

LEV_RE = re.compile(r"(UP|DOWN|BULL|BEAR)$")
STABLE = {"USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USDP", "USD1", "RLUSD",
          "EUR", "TRY", "BRL", "AEUR", "XUSD", "U", "USDS"}

session = requests.Session()
session.headers["User-Agent"] = "wissam-cascade/2.0"
HOST = None


# ════════════════════════════════════════════════════════════
#  config
# ════════════════════════════════════════════════════════════

def default_config():
    cfg = {k: v[1] for k, v in SETTINGS.items()}
    cfg.update({"paused": False, "force": False, "tg_offset": 0})
    return cfg


def load_config():
    cfg = default_config()
    try:
        with open(CONFIG_FILE) as f:
            cfg.update(json.load(f))
    except Exception:
        pass
    # environment still wins on a fresh repo, so the workflow can seed values
    for key, (env, _d, cast) in SETTINGS.items():
        if os.getenv(env):
            try:
                cfg[key] = cast(os.getenv(env))
            except ValueError:
                pass
    return cfg


def save_config(cfg):
    try:
        with open(CONFIG_FILE, "w") as f:
            json.dump(cfg, f, indent=2, sort_keys=True)
    except Exception as e:
        print("config not saved:", e)


def config_text(cfg):
    return ("<b>Settings</b>\n<code>"
            f"mi       {cfg['mi']:g}      |mama| below\n"
            f"cap      {cfg['cap']:g}      macdRSIx below\n"
            f"look     {cfg['look']}       conv lookback (bars)\n"
            f"bars     {cfg['bars']}      cross 1h / 4h\n"
            f"bars1d   {cfg['bars1d']}       cross 1d\n"
            f"vol      {cfg['vol']:g}M     min 24h volume\n"
            f"cooldown {cfg['cooldown']:g}h      repeat delay\n"
            f"state    {'PAUSED' if cfg['paused'] else 'running'}"
            "</code>")


# ════════════════════════════════════════════════════════════
#  Telegram
# ════════════════════════════════════════════════════════════

def tg_call(method, **params):
    if not TG_TOKEN:
        return None
    try:
        r = session.get(f"https://api.telegram.org/bot{TG_TOKEN}/{method}",
                        params=params, timeout=25)
        return r.json()
    except Exception as e:
        print(f"telegram {method} failed:", e)
        return None


def tg_send(text):
    if not TG_TOKEN or not TG_CHAT:
        print("[telegram not configured]\n" + text)
        return "not configured"
    j = tg_call("sendMessage", chat_id=TG_CHAT, text=text,
                parse_mode="HTML", disable_web_page_preview="true")
    if not j:
        return "failed"
    return "sent" if j.get("ok") else str(j.get("description"))


def parse_commands(cfg):
    """Read whatever was sent to the bot since last time and apply it."""
    j = tg_call("getUpdates", offset=cfg.get("tg_offset", 0), timeout=0, limit=50)
    if not j or not j.get("ok"):
        return cfg, []

    notes = []
    for upd in j.get("result", []):
        cfg["tg_offset"] = upd["update_id"] + 1
        msg = upd.get("message") or upd.get("edited_message") or {}
        text = (msg.get("text") or "").strip()
        if not text.startswith("/"):
            continue
        # only obey the owner's own chat
        if TG_CHAT and str(msg.get("chat", {}).get("id")) != str(TG_CHAT):
            continue

        cmd = text.split()[0].lower().split("@")[0]

        if cmd == "/status":
            notes.append(("text", config_text(cfg)))
        elif cmd == "/pause":
            cfg["paused"] = True
            notes.append(("text", "⏸ paused — no scanning until /resume"))
        elif cmd == "/resume":
            cfg["paused"] = False
            notes.append(("text", "▶️ resumed"))
        elif cmd in ("/report", "/report4h", "/report24h"):
            notes.append(("report", ""))
        elif cmd == "/journal":
            notes.append(("journal", ""))
        elif cmd == "/scan":
            cfg["force"] = True
            notes.append(("text", "🔄 cooldown ignored on this run"))
        elif cmd == "/set":
            changed, bad = [], []
            for token in text.split()[1:]:
                if "=" not in token:
                    bad.append(token)
                    continue
                k, v = token.split("=", 1)
                k = k.strip().lower()
                if k not in SETTINGS:
                    bad.append(k)
                    continue
                try:
                    cfg[k] = SETTINGS[k][2](v)
                    changed.append(f"{k}={cfg[k]:g}" if isinstance(cfg[k], float) else f"{k}={cfg[k]}")
                except ValueError:
                    bad.append(token)
            parts = []
            if changed:
                parts.append("✅ " + "  ".join(changed))
            if bad:
                parts.append("⚠️ ignored: " + ", ".join(bad))
            if not changed and not bad:
                parts.append("usage: <code>/set mi=8 cap=45 vol=1</code>")
            notes.append(("text", "\n".join(parts)))
        elif cmd in ("/start", "/help"):
            notes.append(("text",
                "<b>Wissam Cascade</b>\n"
                "<code>/status</code> current settings\n"
                "<code>/set mi=8 cap=45 vol=1</code> change them\n"
                "<code>/report</code> what the signals actually did\n"
                "<code>/journal</code> the last ten rows\n"
                "<code>/scan</code> ignore the cooldown once\n"
                "<code>/pause</code> · <code>/resume</code>"))
    return cfg, notes


# ════════════════════════════════════════════════════════════
#  Pine-equivalent series math
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
    m = macd_line(c, 15, 30)
    d = [(m[i] - m[i - 1]) if (i > 0 and m[i] is not None and m[i - 1] is not None) else None
         for i in range(len(m))]
    return rsi_from(d, 14)


def macd_rsi_x(c):
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
    level = None
    for i in range(n, 0, -1):
        if None in (ef[i], es[i], ef[i - 1], es[i - 1]):
            continue
        if ef[i] > es[i] and ef[i - 1] <= es[i - 1]:
            bars, level = n - i, es[i]       # the price at the crossing point
            break
    return {"bars": bars, "gap": (ef[n] - es[n]) / es[n] * 100,
            "above": ef[n] > es[n], "level": level}


def bg_parts(mr, mrx, i):
    a, b = i - 1, i - 2
    if b < 0:
        return None
    if None in (mr[i], mrx[i], mr[a], mr[b], mrx[a], mrx[b]):
        return None
    return {"mama": mrx[i] - mr[i], "mr": mr[i], "mrx": mrx[i],
            "c_mr": mr[a] > mr[b], "c_mrx": mrx[a] > mrx[b]}


def bg_all(p, cfg):
    return (abs(p["mama"]) < cfg["mi"] and p["c_mr"] and p["c_mrx"]
            and p["mrx"] < cfg["cap"])


# ════════════════════════════════════════════════════════════
#  Binance
# ════════════════════════════════════════════════════════════

def pick_host():
    global HOST
    if HOST:
        return HOST
    for h in HOSTS:
        try:
            if session.get(h + "/api/v3/ping", timeout=12).ok:
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


def universe(cfg):
    info = jget("/api/v3/exchangeInfo")
    tick = jget("/api/v3/ticker/24hr")
    tmap = {t["symbol"]: t for t in tick}
    floor = cfg["vol"] * 1e6

    pairs = []
    for s in info["symbols"]:
        if (s["quoteAsset"] != "USDT" or s["status"] != "TRADING"
                or not s.get("isSpotTradingAllowed")):
            continue
        base = s["baseAsset"]
        if LEV_RE.search(base) or base in STABLE:
            continue
        t = tmap.get(s["symbol"])
        if not t or float(t["quoteVolume"]) < floor:
            continue
        pairs.append(s["symbol"])

    pairs.sort(key=lambda p: -float(tmap[p]["quoteVolume"]))
    return pairs, tmap


def closes_of(symbol, tf, limit):
    kl = jget(f"/api/v3/klines?symbol={symbol}&interval={tf}&limit={limit}")
    return [float(k[4]) for k in kl[:-1]]        # drop the candle still forming


def run_stage(tf, want, pairs, cfg):
    need_conv, need_cross = "conv" in want, "cross" in want
    limit = max(300 if need_conv else 0, max(EMA_SLOW * 4, 60) if need_cross else 0)
    out = {"conv": {}, "cross": {}, "up": []}

    def work(sym):
        try:
            c = closes_of(sym, tf, limit)
            if need_cross:
                ci = cross_info(c, EMA_FAST, EMA_SLOW)
                if ci and ci["above"]:
                    out["up"].append(sym)      # market breadth, free from this pass
                cap = cfg["bars1d"] if tf == "1d" else cfg["bars"]
                if ci and ci["bars"] is not None and ci["bars"] <= cap and ci["above"]:
                    out["cross"][sym] = {"bars": ci["bars"], "gap": ci["gap"],
                                         "level": ci["level"]}
            if need_conv and len(c) >= 80:
                mr, mrx = macd_rsi(c), macd_rsi_x(c)
                n = len(c) - 1
                for i in range(n, max(1, n - cfg["look"]), -1):
                    p = bg_parts(mr, mrx, i)
                    if p and bg_all(p, cfg):
                        cur = bg_parts(mr, mrx, n)
                        out["conv"][sym] = {"mama": abs(cur["mama"]), "mrx": cur["mrx"]}
                        break
        except Exception:
            pass

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, pairs))
    out["above"] = len(out["up"])
    return out


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
#  Journal — every signal is written down, then revisited
# ════════════════════════════════════════════════════════════

# the convergence timeframe of a cascade is the one the trade is watched on,
# and it sets how long the signal is given to resolve
STF_OF   = {"15m→1h": "15m", "1h→4h": "1h", "4h→1d": "4h",
            "15m→4h": "15m", "1h→1d": "1h"}
STF_MIN  = {"15m": 15, "1h": 60, "4h": 240}
HORIZON  = {"15m": 24, "1h": 72, "4h": 168}      # hours before a signal is closed as "none"
TAPE     = "15m"                                  # every path is measured on 15m candles
TAPE_MS  = 15 * 60 * 1000

FIELDS = ["ts", "when", "symbol", "cascades", "stf",
          "entry", "stop", "risk",
          "vol_m", "chg24", "breadth", "mama", "bars",
          "checked", "mfe", "mae", "t05", "t10", "tstop", "outcome"]


def load_journal():
    try:
        with open(JOURNAL_FILE, newline="") as f:
            return [dict(r) for r in csv.DictReader(f)]
    except Exception:
        return []


def save_journal(rows):
    try:
        with open(JOURNAL_FILE, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            for r in rows:
                w.writerow({k: r.get(k, "") for k in FIELDS})
    except Exception as e:
        print("journal not saved:", e)


def outcome_of(t05, t10, tstop, closed):
    """Which came first — the target or the stop. A tie inside one candle
    counts as the stop, so the figures are never flattering by accident."""
    s = float(tstop) if tstop else None
    a = float(t05) if t05 else None
    b = float(t10) if t10 else None
    if b is not None and (s is None or b < s):
        return "1%"
    if a is not None and (s is None or a < s):
        return "0.5%"
    if s is not None:
        return "stop"
    return "none" if closed else ""


def track(r):
    """Walk the 15m candles since the last check, looking for +0.5%, +1%
    and the stop. Each run only fetches the slice it has not seen."""
    try:
        ts, entry, stop = float(r["ts"]), float(r["entry"]), float(r["stop"])
    except (KeyError, ValueError):
        return False
    if entry <= 0 or r.get("outcome"):
        return False

    stf = r.get("stf") or "15m"
    cap = ts + HORIZON.get(stf, 24) * 3600 * 1000
    frm = float(r.get("checked") or ts)
    to  = min(time.time() * 1000, cap)
    if to - frm < TAPE_MS:                 # not even one new candle
        return False

    kl = jget(f"/api/v3/klines?symbol={r['symbol']}&interval={TAPE}"
              f"&startTime={int(frm)}&endTime={int(to)}&limit=1000")
    if kl is None:
        return False

    t05, t10, tstop = r.get("t05", ""), r.get("t10", ""), r.get("tstop", "")
    mfe = float(r.get("mfe") or 0)
    mae = float(r.get("mae") or 0)
    for k in kl:
        hi, lo = float(k[2]), float(k[3])
        up, dn = (hi - entry) / entry * 100, (lo - entry) / entry * 100
        mfe, mae = max(mfe, up), min(mae, dn)
        when = f"{(int(k[0]) + TAPE_MS - ts) / 60000:.0f}"   # minutes, at candle close
        if not tstop and lo <= stop:
            tstop = when
        if not t05 and up >= 0.5:
            t05 = when
        if not t10 and up >= 1.0:
            t10 = when
        if tstop and t10:
            break

    r["t05"], r["t10"], r["tstop"] = t05, t10, tstop
    r["mfe"], r["mae"] = f"{mfe:.2f}", f"{mae:.2f}"
    r["checked"] = f"{int(to)}"
    closed = to >= cap
    r["outcome"] = outcome_of(t05, t10, tstop, closed) if (tstop or t10 or closed) else ""
    return True


def settle_journal(rows):
    open_rows = [r for r in rows if not r.get("outcome")]
    if not open_rows:
        return 0, 0
    moved = 0
    with ThreadPoolExecutor(max_workers=min(WORKERS, 8)) as ex:
        for ok in ex.map(track, open_rows):
            moved += 1 if ok else 0
    return moved, sum(1 for r in open_rows if r.get("outcome"))


def median(xs):
    xs = sorted(xs)
    n = len(xs)
    if not n:
        return None
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def report_text(rows, _arg=""):
    """What the signals actually did: did 0.5% and 1% arrive before the stop."""
    tags = {}
    for r in rows:
        for tag in (r.get("cascades") or "").split("|"):
            tag = tag.strip()
            if tag:
                tags.setdefault(tag, []).append(r)

    if not tags:
        return "<b>Report</b>\n<code>the journal is still empty</code>"

    out = ["<b>Report</b> · <code>0.5% and 1% against the stop at the EMA cross</code>"]
    for tag in sorted(tags, key=lambda t: -len(tags[t])):
        rs = tags[tag]
        done = [r for r in rs if r.get("outcome")]
        n = len(done)
        if not n:
            out.append(f"\n<b>{tag}</b> · <code>{len(rs)} open, nothing resolved yet</code>")
            continue
        c = {k: sum(1 for r in done if r["outcome"] == k) for k in ("1%", "0.5%", "stop", "none")}
        hit05 = c["1%"] + c["0.5%"]                     # 0.5% reached before the stop
        risks = [abs(float(r["risk"])) for r in done if r.get("risk")]
        risk = sum(risks) / len(risks) if risks else 0
        mins = [float(r["t05"]) for r in done
                if r.get("t05") and r["outcome"] in ("1%", "0.5%")]
        med = median(mins)
        tfm = STF_MIN.get(rs[0].get("stf") or "15m", 15)
        # what one trade is worth on average if he always takes 0.5%
        edge = (hit05 / n) * 0.5 - (c["stop"] / n) * risk
        mfes = [float(r["mfe"]) for r in done if r.get("mfe")]
        out.append(
            f"\n<b>{tag}</b> · {n} resolved" + (f" · {len(rs) - n} open" if len(rs) > n else "") +
            f"\n<code>0.5%  {hit05 / n * 100:>3.0f}%"
            f"   1%  {c['1%'] / n * 100:>3.0f}%"
            f"   stop {c['stop'] / n * 100:>3.0f}%"
            f"   flat {c['none'] / n * 100:>3.0f}%</code>"
            f"\n<code>stop {risk:.2f}% away · "
            + (f"0.5% after {med / tfm:.0f} candle(s) · " if med else "")
            + f"best +{max(mfes):.1f}% · edge {edge:+.2f}%/trade</code>")

    out.append("\n<i>edge = what one trade returns on average if you always take 0.5%: "
               "wins × 0.5% minus stops × the stop distance. Above zero the cascade pays.</i>")
    return "\n".join(out)


def journal_tail(rows, k=10):
    if not rows:
        return "<code>the journal is still empty</code>"
    lines = []
    for r in rows[-k:][::-1]:
        res = r.get("outcome") or "open"
        lines.append(f"{r.get('when','')} {r['symbol'][:-4]:<7} {r.get('cascades',''):<12} "
                     f"{res:<5} mfe {r.get('mfe','')}  mae {r.get('mae','')}")
    return f"<b>Last {len(lines)}</b>\n<code>" + "\n".join(lines) + "</code>"


# ════════════════════════════════════════════════════════════
#  state
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
    cfg = load_config()
    cfg, notes = parse_commands(cfg)
    for kind, text in notes:
        if kind == "report":
            tg_send(report_text(load_journal()))
        elif kind == "journal":
            tg_send(journal_tail(load_journal()))
        else:
            tg_send(text)
    save_config(cfg)                       # keep the offset even if we stop here

    if cfg["paused"]:
        print("paused — skipping the scan")
        return

    started = time.time()
    pairs, tmap = universe(cfg)
    print(f"universe: {len(pairs)} pairs  (vol ≥ {cfg['vol']}M)")

    s15 = run_stage("15m", ["conv"], pairs, cfg)
    s1h = run_stage("1h", ["conv", "cross"], pairs, cfg)
    s4h = run_stage("4h", ["conv", "cross"], pairs, cfg)
    s1d = run_stage("1d", ["cross"], pairs, cfg)

    sets = {"conv15": s15["conv"], "conv1h": s1h["conv"], "cross1h": s1h["cross"],
            "conv4h": s4h["conv"], "cross4h": s4h["cross"], "cross1d": s1d["cross"]}
    for k, v in sets.items():
        print(f"  {k}: {len(v)}")

    hits = find_cascades(sets)
    breadth = s1h["above"] / len(pairs) * 100 if pairs else 0.0
    print(f"cascades: {len(hits)}  breadth {breadth:.0f}%  ({time.time() - started:.0f}s)")

    st = load_state()
    now = time.time()
    force = cfg.get("force", False)
    fresh = []
    for sym, tags in hits.items():
        key = sym + "|" + ",".join(sorted(tags))
        last = st.get(key)
        if force or last is None or (now - last) >= cfg["cooldown"] * 3600:
            fresh.append((sym, tags))
        st[key] = now

    st = {k: v for k, v in st.items() if now - v < 86400}
    save_state(st)

    if force:
        cfg["force"] = False
        save_config(cfg)

    # ── the journal: advance the open rows, then write today's signals ──
    rows = load_journal()
    moved, closed = settle_journal(rows)

    stops = {}
    for sym, tags in fresh:
        t = tmap.get(sym, {})
        try:
            entry = float(t.get("lastPrice", 0))
        except ValueError:
            entry = 0.0
        tag0 = min(tags, key=lambda g: list(STF_OF).index(g) if g in STF_OF else 9)
        stf = STF_OF.get(tag0, "15m")
        conv = next((sets[c][sym] for g, c, _x in CASCADES if g == tag0 and sym in sets[c]), {})
        cross = next((sets[x][sym] for g, _c, x in CASCADES if g == tag0 and sym in sets[x]), {})
        stop = cross.get("level") or 0.0
        risk = (stop - entry) / entry * 100 if entry and stop else 0.0
        stops[sym] = (entry, stop, risk, stf)
        rows.append({
            "ts":      f"{int(now * 1000)}",
            "when":    datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%d %H:%M"),
            "symbol":  sym,
            "cascades": "|".join(tags),
            "stf":     stf,
            "entry":   f"{entry:.10g}",
            "stop":    f"{stop:.10g}",
            "risk":    f"{risk:.2f}",
            "vol_m":   f"{float(t.get('quoteVolume', 0)) / 1e6:.1f}",
            "chg24":   f"{float(t.get('priceChangePercent', 0)):.1f}",
            "breadth": f"{breadth:.0f}",
            "mama":    f"{conv.get('mama', 0):.1f}" if conv else "",
            "bars":    f"{cross.get('bars', '')}",
            "checked": f"{int(now * 1000)}",
        })
    if fresh or moved:
        save_journal(rows)
    print(f"journal: {len(rows)} rows, +{len(fresh)} new, {moved} advanced, {closed} resolved")

    if fresh:
        fresh.sort(key=lambda x: -float(tmap[x[0]]["quoteVolume"]))
        lines = []
        for sym, tags in fresh[:20]:
            t = tmap[sym]
            vol = float(t["quoteVolume"]) / 1e6
            chg = float(t["priceChangePercent"])
            link = f'<a href="https://www.tradingview.com/chart/?symbol=BINANCE:{sym}">{sym[:-4]}</a>'
            entry, stop, risk, _stf = stops.get(sym, (0, 0, 0, ""))
            sl = f" · stop {stop:.10g} ({risk:+.1f}%)" if stop else ""
            lines.append(f"• {link}  {' · '.join(tags)}\n"
                         f"   ${vol:,.1f}M · {chg:+.1f}% · entry {t['lastPrice']}{sl}")
        more = f"\n…and {len(fresh) - 20} more" if len(fresh) > 20 else ""
        tg_send(f"🟢 <b>Wissam Cascade</b>  <code>{len(pairs)} pairs · "
                f"{breadth:.0f}% up</code>\n\n"
                + "\n".join(lines) + more)
    else:
        print("nothing new to send")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        tg_send("⚠️ <b>Wissam Cascade error</b>\n<code>"
                + traceback.format_exc()[-400:] + "</code>")
        sys.exit(1)
