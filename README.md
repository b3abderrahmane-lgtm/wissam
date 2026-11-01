# Wissam Cascade — phone app

Installable web app. Scans Binance USDT spot pairs on 15m, 1h, 4h and 1d,
and shows the cascades (a convergence on one timeframe plus an EMA cross on the one above).

## Put it online (needed once)

A phone can only install an app served over HTTPS, so these files must sit on a web address.
GitHub Pages is free and takes a few minutes:

1. github.com → New repository → name it `wissam` → Public → Create
2. Add file → Upload files → drag in every file from this folder → Commit
3. Settings → Pages → Source: `Deploy from a branch` → Branch: `main` / `(root)` → Save
4. Wait a minute, then open `https://<your-username>.github.io/wissam/`

## Install on the phone

Open that address in Chrome → menu (⋮) → **Install app** (or *Add to Home screen*).
It then opens full screen with its own icon, no browser bars.

## Auto scanning

Tick **Auto every 15m**. The app rescans on each 15-minute candle close and pushes
new cascades to Telegram.

Android freezes web apps that are not on screen, so this runs while the app is open.
For alerts that arrive with the app closed, run the Python scanner on a server
(GitHub Actions works and is free) and let Telegram do the notifying.

## Files

- `index.html` — the whole app
- `manifest.json` — name, icon, full-screen behaviour
- `sw.js` — caches the app shell so it opens offline; never caches prices
- `icon-*.png` — launcher icons
