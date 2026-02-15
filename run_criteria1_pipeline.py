import subprocess
import sys
import datetime
from pathlib import Path
import pandas as pd
import json

# Fichiers attendus
RAW_CSV = "hub_revenue_daily.csv"
ENRICHED_CSV = "hub_revenue_with_inflation.csv"
STATE_FEE = "state_fee_v41.json"   # adapte si ton v41 utilise un autre state filename

def run_py(script_name: str):
    print(f"\n=== RUN: {script_name} ===")
    r = subprocess.run([sys.executable, script_name], capture_output=True, text=True)
    print(r.stdout)
    if r.returncode != 0:
        print(r.stderr)
        raise RuntimeError(f"Script échoué: {script_name}")
    return r.stdout

def file_exists(path: Path) -> bool:
    return path.exists() and path.is_file()

def read_last_row(csv_path: Path):
    df = pd.read_csv(csv_path)
    if df.empty:
        return None, df
    # si colonne date existe, on prend la plus récente
    if "date" in df.columns:
        df_sorted = df.sort_values("date")
        return df_sorted.iloc[-1].to_dict(), df_sorted
    return df.iloc[-1].to_dict(), df

def checklist(here: Path):
    print("\n=== CHECKLIST ===")
    status = "OK"
    notes = []

    raw = here / RAW_CSV
    enr = here / ENRICHED_CSV
    stf = here / STATE_FEE

    # 1) Fichiers
    if not file_exists(raw):
        status = "FAIL"
        notes.append(f"Fichier manquant: {RAW_CSV}")
    if not file_exists(enr):
        status = "FAIL"
        notes.append(f"Fichier manquant: {ENRICHED_CSV}")

    # 2) State (avancement)
    last_height = None
    if file_exists(stf):
        try:
            with open(stf, "r", encoding="utf-8") as f:
                j = json.load(f)
                last_height = j.get("last_height")
        except Exception:
            status = "WARN" if status != "FAIL" else status
            notes.append(f"State illisible: {STATE_FEE}")
    else:
        status = "WARN" if status != "FAIL" else status
        notes.append(f"State manquant: {STATE_FEE} (peut être normal si tu as renommé)")

    # 3) Cohérence des données dans le raw CSV
    if status != "FAIL" and file_exists(raw):
        last, df = read_last_row(raw)
        if last is None:
            status = "FAIL"
            notes.append("RAW CSV vide.")
        else:
            # Tx total
            tx_total = float(last.get("tx_total", 0) or 0)
            if tx_total <= 0:
                status = "WARN" if status != "FAIL" else status
                notes.append("tx_total=0 sur la dernière ligne (possible si batch trop petit ou journée vide).")

            # Fees
            total_fee_uatom = float(last.get("total_fee_uatom", 0) or 0)
            if total_fee_uatom <= 0:
                status = "WARN" if status != "FAIL" else status
                notes.append("total_fee_uatom=0 sur la dernière ligne (possible si collecte trop courte, ou bloc(s) vide).")

            # Erreurs LCD (si colonne présente)
            if "lcd_errors" in last:
                lcd_errors = float(last.get("lcd_errors", 0) or 0)
                if lcd_errors > 0:
                    status = "WARN" if status != "FAIL" else status
                    notes.append(f"lcd_errors={lcd_errors} sur la dernière ligne.")

    # 4) Cohérence enriched CSV : présence des colonnes inflation
    if status != "FAIL" and file_exists(enr):
        last_e, df_e = read_last_row(enr)
        required_cols = ["estimated_daily_emission_atom", "fee_coverage_pct", "ibc_fee_coverage_pct"]
        missing = [c for c in required_cols if c not in df_e.columns]
        if missing:
            status = "FAIL"
            notes.append(f"Colonnes manquantes dans enriched CSV: {missing}")

    print(f"Status: {status}")
    if last_height is not None:
        print(f"State last_height: {last_height}")
    for n in notes:
        print(" -", n)

    return status

def main():
    here = Path(__file__).resolve().parent
    print("Dossier:", here)
    print("Date:", datetime.datetime.now().isoformat(timespec="seconds"))

    # 1) Collecte fees + tx + IBC
    run_py("hub_fee_monitor_v41.py")

    # 2) Overlay inflation
    run_py("inflation_overlay.py")

    # 3) Checklist
    status = checklist(here)

    print("\n=== FIN PIPELINE ===")
    if status == "OK":
        print("✅ Tout est bon.")
    elif status == "WARN":
        print("⚠️ Pipeline terminé mais avec avertissements (données utilisables, à surveiller).")
    else:
        print("❌ Pipeline en échec (à corriger avant de continuer).")

if __name__ == "__main__":
    main()
