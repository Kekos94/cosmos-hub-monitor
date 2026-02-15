import os
import pandas as pd
from datetime import datetime

OUT_DIR = "site"
HUB_FILE = "hub_revenue_with_inflation.csv"
VOL_FILE = "ecosystem_out_usd_daily.csv"  # optionnel si tu ne l’as pas encore
OUT_HTML = os.path.join(OUT_DIR, "index.html")

def safe_read_csv(path):
    if not os.path.exists(path):
        return None
    df = pd.read_csv(path)
    if df.empty:
        return None
    return df

def to_html_table(df, max_rows=7):
    df2 = df.copy()
    if "date" in df2.columns:
        df2 = df2.sort_values("date")
    df2 = df2.tail(max_rows)
    return df2.to_html(index=False)

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    hub = safe_read_csv(HUB_FILE)
    vol = safe_read_csv(VOL_FILE)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    if hub is None:
        html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Cosmos Hub Monitor</title></head>
<body>
<h1>Cosmos Hub Monitor</h1>
<p>Dernière génération: {now}</p>
<p><b>Erreur :</b> {HUB_FILE} absent ou vide.</p>
</body></html>"""
        with open(OUT_HTML, "w", encoding="utf-8") as f:
            f.write(html)
        print("Dashboard généré (mais sans données Hub).")
        return

    hub["date"] = pd.to_datetime(hub["date"])
    hub = hub.sort_values("date")
    last = hub.iloc[-1]

    # Valeurs clés (Critère 1)
    k = {
        "Date": str(last["date"].date()),
        "Tx total": int(last.get("tx_total", 0)),
        "Tx IBC": int(last.get("tx_ibc", 0)),
        "Ratio IBC (%)": float(last.get("tx_ibc_ratio_pct", 0.0)),
        "Fees totales (ATOM)": float(last.get("total_fee_atom", 0.0)),
        "Fees IBC (ATOM)": float(last.get("ibc_fee_atom", 0.0)),
        "Part fees IBC (%)": float(last.get("ibc_fee_share_pct", 0.0)),
        "Inflation annuelle (%)": float(last.get("annual_inflation", 0.0)) * 100.0,
        "Emission/jour (ATOM)": float(last.get("estimated_daily_emission_atom", 0.0)),
        "Coverage fees (%)": float(last.get("fee_coverage_pct", 0.0)),
        "Coverage fees IBC (%)": float(last.get("ibc_fee_coverage_pct", 0.0)),
    }

    # Tables
    hub_last7 = hub.copy()
    hub_last7["date"] = hub_last7["date"].dt.date

    html_kv = "<table border='1' cellpadding='6' cellspacing='0'>"
    for kk, vv in k.items():
        html_kv += f"<tr><th align='left'>{kk}</th><td>{vv}</td></tr>"
    html_kv += "</table>"

    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Cosmos Hub Monitor</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 30px; }}
    h1 {{ margin-bottom: 0; }}
    .muted {{ color: #666; }}
    table {{ border-collapse: collapse; }}
    th, td {{ padding: 6px 10px; }}
  </style>
</head>
<body>
  <h1>Cosmos Hub Monitor</h1>
  <p class="muted">Dernière génération: {now}</p>

  <h2>Critère 1 — Snapshot (dernier jour)</h2>
  {html_kv}

  <h2>Critère 1 — 7 derniers jours (Hub)</h2>
  {to_html_table(hub_last7[[
      "date","tx_total","tx_ibc","tx_ibc_ratio_pct",
      "total_fee_atom","ibc_fee_atom","ibc_fee_share_pct",
      "fee_coverage_pct","ibc_fee_coverage_pct"
  ]], max_rows=7)}

  <h2>Fichiers</h2>
  <ul>
    <li><a href="hub_revenue_with_inflation.csv">hub_revenue_with_inflation.csv</a></li>
    <li><a href="hub_revenue_daily.csv">hub_revenue_daily.csv</a></li>
"""

    if vol is not None:
        vol["date"] = pd.to_datetime(vol["date"])
        vol = vol.sort_values("date")
        vol_last7 = vol.copy()
        vol_last7["date"] = vol_last7["date"].dt.date
        html += f"""
    <li><a href="ecosystem_out_usd_daily.csv">ecosystem_out_usd_daily.csv</a></li>
  </ul>

  <h2>Critère 2 — 7 derniers jours (ecosystem OUT USD)</h2>
  {to_html_table(vol_last7.tail(7), max_rows=7)}
"""
    else:
        html += """
  </ul>
  <p class="muted">ecosystem_out_usd_daily.csv absent (OK si Critère 2 pas encore en prod).</p>
"""

    html += """
</body></html>
"""

    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    # Copier les CSV dans /site pour téléchargement
    for fn in ["hub_revenue_with_inflation.csv", "hub_revenue_daily.csv", "ecosystem_out_usd_daily.csv"]:
        if os.path.exists(fn):
            os.replace(fn, os.path.join(OUT_DIR, fn)) if not os.path.exists(os.path.join(OUT_DIR, fn)) else None

    print("Dashboard généré:", OUT_HTML)

if __name__ == "__main__":
    main()
