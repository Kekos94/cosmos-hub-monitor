import requests
import json
import os
import time
import pandas as pd
from datetime import datetime, timezone
import re
import hashlib
import base64

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

RPCS = [
    "https://cosmos-rpc.polkachu.com:443",
    "https://rpc.cosmoshub.strange.love:443",
    "https://rpc-cosmoshub.blockapsis.com:443",
    "https://rpc.cosmos.network",
]

LCDS = [
    "https://cosmos-api.polkachu.com",
    "https://cosmoshub-lcd.publicnode.com",
    "https://api.cosmos.network",
]

STATE_FILE = "state_fee_v41.json"
OUTFILE = "hub_revenue_daily.csv"

BLOCK_BATCH = 60  # moins, car tx-by-hash = beaucoup d'appels
UAATOM_PER_ATOM = 1_000_000

REQ_TIMEOUT = 25
MAX_RETRIES = 3
BACKOFF = 0.8

SLEEP_BETWEEN_BLOCKS = 0.05
SLEEP_BETWEEN_TXS = 0.02


# ------------------------------------------------------------
# HTTP helpers
# ------------------------------------------------------------

def request_with_retries(url, params=None, timeout=REQ_TIMEOUT):
    last_err = None
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(BACKOFF * i)
    raise last_err

def http_get_any(base_urls, path, params=None, timeout=REQ_TIMEOUT):
    errors = []
    for base in base_urls:
        url = base.rstrip("/") + path
        try:
            r = request_with_retries(url, params=params, timeout=timeout)
            return r.json(), base
        except Exception as e:
            errors.append((base, str(e)))
            continue
    raise RuntimeError("Tous les endpoints ont échoué:\n" + "\n".join([f"- {b}: {err}" for b, err in errors]))

def get_atom_price_usd():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "cosmos", "vs_currencies": "usd"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return float(r.json()["cosmos"]["usd"])


# ------------------------------------------------------------
# State
# ------------------------------------------------------------

def load_state():
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def save_state(height: int):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_height": height}, f)


# ------------------------------------------------------------
# Timestamp normalization
# ------------------------------------------------------------

def normalize_iso(ts: str) -> str:
    if not isinstance(ts, str) or ts == "":
        return ts
    ts = ts.replace("Z", "+00:00")
    m = re.match(r"^(.*\.(\d+))([+-]\d\d:\d\d)$", ts)
    if not m:
        return ts
    frac = m.group(2)
    tz = m.group(3)
    frac6 = (frac + "000000")[:6]
    base_no_frac = ts.split(".")[0]
    return f"{base_no_frac}.{frac6}{tz}"

def parse_date(block_time_iso: str) -> str:
    ts = normalize_iso(block_time_iso)
    dt = datetime.fromisoformat(ts).astimezone(timezone.utc)
    return str(dt.date())


# ------------------------------------------------------------
# RPC calls
# ------------------------------------------------------------

def rpc_status():
    j, base = http_get_any(RPCS, "/status")
    return j, base

def rpc_block(height: int):
    j, base = http_get_any(RPCS, f"/block?height={height}")
    return j, base


# ------------------------------------------------------------
# Hash computation
# ------------------------------------------------------------

def tm_tx_hash_from_b64(tx_b64: str) -> str:
    """
    Tendermint tx hash = SHA256(raw_tx_bytes), hex uppercase.
    The /block endpoint provides base64 tx bytes in data.txs.
    """
    raw = base64.b64decode(tx_b64)
    h = hashlib.sha256(raw).hexdigest().upper()
    return h


# ------------------------------------------------------------
# LCD tx by hash
# ------------------------------------------------------------

def lcd_get_tx_by_hash(tx_hash: str):
    data, lcd_used = http_get_any(LCDS, f"/cosmos/tx/v1beta1/txs/{tx_hash}", timeout=30)
    return data, lcd_used


# ------------------------------------------------------------
# Extractors
# ------------------------------------------------------------

def is_ibc_tx(tx_body: dict) -> bool:
    msgs = tx_body.get("messages", []) or []
    for m in msgs:
        t = m.get("@type", "")
        if isinstance(t, str) and t.startswith("/ibc."):
            return True
    return False

def fee_uatom_from_lcd_tx(lcd_tx: dict) -> int:
    tx = lcd_tx.get("tx", {}) or {}
    auth = tx.get("auth_info", {}) or {}
    fee = auth.get("fee", {}) or {}
    amounts = fee.get("amount", []) or []
    total = 0
    for a in amounts:
        if a.get("denom") == "uatom":
            try:
                total += int(a.get("amount", "0"))
            except Exception:
                pass
    return total

def body_from_lcd_tx(lcd_tx: dict) -> dict:
    tx = lcd_tx.get("tx", {}) or {}
    return tx.get("body", {}) or {}


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    st, rpc_used = rpc_status()
    latest = int(st["result"]["sync_info"]["latest_block_height"])

    state = load_state()
    if state and state.get("last_height") is not None:
        start = int(state["last_height"]) + 1
    else:
        start = max(1, latest - BLOCK_BATCH)

    end = min(latest, start + BLOCK_BATCH)

    print(f"RPC utilisé (status): {rpc_used}")
    print(f"Scan blocs: {start} -> {end} (latest={latest})")

    atom_price = get_atom_price_usd()
    print(f"ATOM price used (USD): {atom_price}")

    by_date = {}
    last_ok = None

    for height in range(start, end + 1):
        # get block (includes tx bytes)
        try:
            b, _ = rpc_block(height)
            header = b["result"]["block"]["header"]
            date = parse_date(header["time"])
            txs_b64 = b["result"]["block"]["data"].get("txs", []) or []
        except Exception as e:
            print(f"[{height}] RPC /block error -> STOP: {e}")
            break

        if date not in by_date:
            by_date[date] = {"tx_total": 0, "tx_ibc": 0, "total_fee_uatom": 0, "ibc_fee_uatom": 0, "lcd_errors": 0}

        hashes = [tm_tx_hash_from_b64(x) for x in txs_b64]
        by_date[date]["tx_total"] += len(hashes)

        # per tx -> LCD by hash
        for h in hashes:
            try:
                lcd_tx, _lcd = lcd_get_tx_by_hash(h)
            except Exception as e:
                by_date[date]["lcd_errors"] += 1
                print(f"[{height}] LCD /txs/{{hash}} error -> STOP (sans trou): {e}")
                # ne pas avancer state si bloc incomplet
                hashes = None
                break

            fu = fee_uatom_from_lcd_tx(lcd_tx)
            by_date[date]["total_fee_uatom"] += fu

            body = body_from_lcd_tx(lcd_tx)
            if is_ibc_tx(body):
                by_date[date]["tx_ibc"] += 1
                by_date[date]["ibc_fee_uatom"] += fu

            time.sleep(SLEEP_BETWEEN_TXS)

        if hashes is None:
            break

        save_state(height)
        last_ok = height
        time.sleep(SLEEP_BETWEEN_BLOCKS)

    # Build df for this run
    rows = []
    for date, v in sorted(by_date.items()):
        tx_total = v["tx_total"]
        total_fee = v["total_fee_uatom"]
        rows.append({
            "date": date,
            "tx_total": tx_total,
            "tx_ibc": v["tx_ibc"],
            "tx_ibc_ratio_pct": (v["tx_ibc"] / tx_total * 100.0) if tx_total else 0.0,
            "total_fee_uatom": total_fee,
            "ibc_fee_uatom": v["ibc_fee_uatom"],
            "ibc_fee_share_pct": (v["ibc_fee_uatom"] / total_fee * 100.0) if total_fee else 0.0,
            "lcd_errors": v["lcd_errors"],
        })

    df_new = pd.DataFrame(rows)

    if os.path.exists(OUTFILE):
        df_old = pd.read_csv(OUTFILE)
        df = pd.concat([df_old, df_new], ignore_index=True)
        num_cols = [c for c in df.columns if c != "date"]
        df = df.groupby("date", as_index=False)[num_cols].sum()
        df["tx_ibc_ratio_pct"] = df.apply(lambda r: (r["tx_ibc"] / r["tx_total"] * 100.0) if r["tx_total"] else 0.0, axis=1)
        df["ibc_fee_share_pct"] = df.apply(lambda r: (r["ibc_fee_uatom"] / r["total_fee_uatom"] * 100.0) if r["total_fee_uatom"] else 0.0, axis=1)
    else:
        df = df_new

    df["atom_price_usd_used"] = atom_price
    df["total_fee_atom"] = df["total_fee_uatom"] / UAATOM_PER_ATOM
    df["ibc_fee_atom"] = df["ibc_fee_uatom"] / UAATOM_PER_ATOM
    df["total_fee_usd"] = df["total_fee_atom"] * atom_price
    df["ibc_fee_usd"] = df["ibc_fee_atom"] * atom_price

    df = df.sort_values("date")
    df.to_csv(OUTFILE, index=False)

    print("\nOK ->", OUTFILE)
    if last_ok:
        print("Dernier bloc traité :", last_ok)
    print("\nDernières lignes :")
    print(df.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()
