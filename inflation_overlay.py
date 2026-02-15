import os
import requests
import pandas as pd
import numpy as np

LCDS = [
    "https://cosmos-api.polkachu.com",
    "https://lcd.cosmoshub.strange.love",
    "https://api.cosmos.network",
]

INFILE = "hub_revenue_daily.csv"
OUTFILE = "hub_revenue_with_inflation.csv"

UAATOM_PER_ATOM = 1_000_000

def http_get_any(path, timeout=15):
    last_err = None
    for base in LCDS:
        try:
            r = requests.get(base.rstrip("/") + path, timeout=timeout)
            r.raise_for_status()
            return r.json(), base
        except Exception as e:
            last_err = e
    raise last_err

def get_inflation():
    j, used = http_get_any("/cosmos/mint/v1beta1/inflation")
    return float(j["inflation"]), used

def get_supply_atom():
    j, used = http_get_any("/cosmos/bank/v1beta1/supply/by_denom?denom=uatom")
    supply_uatom = int(j["amount"]["amount"])
    return supply_uatom / UAATOM_PER_ATOM, used

def get_atom_price_usd():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {"ids": "cosmos", "vs_currencies": "usd"}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return float(r.json()["cosmos"]["usd"])

def main():
    if not os.path.exists(INFILE):
        print(f"Fichier introuvable: {INFILE}")
        return

    # --- Fetch macro inputs (today snapshot) ---
    print("Récupération inflation + supply + prix...")
    inflation, lcd_inf = get_inflation()
    supply_atom_today, lcd_sup = get_supply_atom()
    atom_price = get_atom_price_usd()

    daily_emission_atom_est = (supply_atom_today * inflation) / 365.0

    print(f"Inflation annuelle: {inflation*100:.2f}% (source: {lcd_inf})")
    print(f"Supply totale (snapshot): {supply_atom_today:,.0f} ATOM (source: {lcd_sup})")
    print(f"Emission journalière estimée: {daily_emission_atom_est:,.2f} ATOM")
    print(f"Prix ATOM (USD): {atom_price:.2f}")

    # --- Read hub daily revenue (aggregated) ---
    df = pd.read_csv(INFILE)
    if df.empty:
        print("CSV vide.")
        return

    # Normalize date
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values("date")

    # Ensure fee columns in ATOM exist
    if "total_fee_atom" not in df.columns:
        if "total_fee_uatom" in df.columns:
            df["total_fee_atom"] = df["total_fee_uatom"] / UAATOM_PER_ATOM
        else:
            df["total_fee_atom"] = 0.0

    if "ibc_fee_atom" not in df.columns:
        if "ibc_fee_uatom" in df.columns:
            df["ibc_fee_atom"] = df["ibc_fee_uatom"] / UAATOM_PER_ATOM
        else:
            df["ibc_fee_atom"] = 0.0

    # --- Load previous enriched file if exists (to preserve daily supply snapshots) ---
    prev = None
    if os.path.exists(OUTFILE):
        try:
            prev = pd.read_csv(OUTFILE)
            if "date" in prev.columns:
                prev["date"] = pd.to_datetime(prev["date"]).dt.date
        except Exception:
            prev = None

    # We'll produce an enriched table with 1 row per date (as in df),
    # and store supply snapshot for each date:
    # - for past dates: keep whatever was recorded previously
    # - for the most recent date: overwrite with today's snapshot (so it updates daily)

    enriched = df.copy()

    # Default: if no prev, set all to NaN then fill only last day with today's snapshot
    enriched["annual_inflation"] = inflation
    enriched["atom_price_usd_used"] = atom_price

    # Create/merge supply history if available
    if prev is not None and "total_supply_atom" in prev.columns:
        supply_hist = prev[["date", "total_supply_atom"]].drop_duplicates("date")
        enriched = enriched.merge(supply_hist, on="date", how="left")
    else:
        enriched["total_supply_atom"] = np.nan

    # Overwrite supply snapshot for the latest date in revenue file
    latest_date = enriched["date"].max()
    enriched.loc[enriched["date"] == latest_date, "total_supply_atom"] = supply_atom_today

    # For any missing historical supply snapshot (first runs), we can’t reconstruct past supply.
    # We leave them NaN; net issuance will be NaN until you have at least 2 daily snapshots.
    # But daily emission estimate can still be computed per row using today's snapshot or the stored one.
    enriched["estimated_daily_emission_atom"] = (enriched["total_supply_atom"] * inflation) / 365.0

    # Coverage vs estimated emission (only where emission is known)
    enriched["fee_coverage_pct"] = np.where(
        enriched["estimated_daily_emission_atom"] > 0,
        (enriched["total_fee_atom"] / enriched["estimated_daily_emission_atom"]) * 100.0,
        np.nan
    )
    enriched["ibc_fee_coverage_pct"] = np.where(
        enriched["estimated_daily_emission_atom"] > 0,
        (enriched["ibc_fee_atom"] / enriched["estimated_daily_emission_atom"]) * 100.0,
        np.nan
    )

    # --- NEW: net issuance via daily supply snapshots ---
    enriched = enriched.sort_values("date")
    enriched["net_issuance_atom"] = enriched["total_supply_atom"].diff()
    enriched["net_issuance_usd"] = enriched["net_issuance_atom"] * atom_price

    # Coverage vs net issuance (only where net issuance > 0)
    enriched["fee_coverage_net_pct"] = np.where(
        enriched["net_issuance_atom"] > 0,
        (enriched["total_fee_atom"] / enriched["net_issuance_atom"]) * 100.0,
        np.nan
    )
    enriched["ibc_fee_coverage_net_pct"] = np.where(
        enriched["net_issuance_atom"] > 0,
        (enriched["ibc_fee_atom"] / enriched["net_issuance_atom"]) * 100.0,
        np.nan
    )

    # Net dilution metrics
    enriched["net_daily_dilution_atom_estimated"] = enriched["estimated_daily_emission_atom"] - enriched["total_fee_atom"]
    enriched["net_daily_dilution_atom_real"] = enriched["net_issuance_atom"] - enriched["total_fee_atom"]

    # Optional USD columns for fees
    enriched["total_fee_usd"] = enriched["total_fee_atom"] * atom_price
    enriched["ibc_fee_usd"] = enriched["ibc_fee_atom"] * atom_price

    enriched.to_csv(OUTFILE, index=False)

    print(f"\nOK -> {OUTFILE}")
    print("\nDernières lignes :")
    print(enriched.tail(5).to_string(index=False))
    print("\nNote: net_issuance_atom sera NaN jusqu'à disposer d'au moins 2 jours avec un snapshot total_supply_atom enregistré.")

if __name__ == "__main__":
    main()
