import requests
import pandas as pd
from datetime import datetime

LCDS = [
    "https://cosmos-api.polkachu.com",
    "https://lcd.cosmoshub.strange.love",
]

CSV_FILE = "hub_revenue_daily.csv"

def http_get_any(path):
    last_err = None
    for base in LCDS:
        try:
            r = requests.get(base + path, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
    raise last_err

def get_inflation():
    j = http_get_any("/cosmos/mint/v1beta1/inflation")
    return float(j["inflation"])

def get_supply_atom():
    j = http_get_any("/cosmos/bank/v1beta1/supply/by_denom?denom=uatom")
    supply_uatom = int(j["amount"]["amount"])
    return supply_uatom / 1_000_000

def main():
    print("Récupération inflation et supply...")
    
    inflation = get_inflation()
    supply_atom = get_supply_atom()
    
    daily_emission_atom = (supply_atom * inflation) / 365
    
    print(f"Inflation annuelle: {inflation*100:.2f}%")
    print(f"Supply totale: {supply_atom:,.0f} ATOM")
    print(f"Emission journalière estimée: {daily_emission_atom:,.2f} ATOM")
    
    df = pd.read_csv(CSV_FILE)
    
    df["annual_inflation"] = inflation
    df["total_supply_atom"] = supply_atom
    df["estimated_daily_emission_atom"] = daily_emission_atom
    
    df["fee_coverage_pct"] = (
        df["total_fee_atom"] / daily_emission_atom * 100
    )
    
    df["ibc_fee_coverage_pct"] = (
        df["ibc_fee_atom"] / daily_emission_atom * 100
    )
    
    df["net_daily_dilution_atom"] = (
        daily_emission_atom - df["total_fee_atom"]
    )
    
    df.to_csv("hub_revenue_with_inflation.csv", index=False)
    
    print("\nOK -> hub_revenue_with_inflation.csv")
    print("\nDernières lignes :")
    print(df.tail())

if __name__ == "__main__":
    main()
