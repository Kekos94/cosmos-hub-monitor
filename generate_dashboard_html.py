"""Publish measured windows; keep legacy records visibly unverified."""
import csv
import html
import shutil
from criterion1_data import ROOT, WINDOWS, read_rows, utcnow, validate
from criterion1_fee_audit import load_fee_audit
from criterion1_daily import DAILY_CSV, load_days, progress, daily_summary, export_daily


def table(rows, fields):
    if not rows:
        return '<p>Aucune fenêtre mesurée disponible.</p>'
    head = ''.join(f'<th>{html.escape(label)}</th>' for _, label in fields)
    body = ''.join('<tr>' + ''.join(f'<td>{html.escape(str(r.get(k, ""))) or "indisponible"}</td>' for k, _ in fields) + '</tr>' for r in rows[-7:])
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def main():
    out = ROOT / 'site'
    out.mkdir(exist_ok=True)
    windows = read_rows(WINDOWS)
    validate(windows)
    window_fields = [('start_height', 'Premier bloc'), ('end_height', 'Dernier bloc'),
                     ('block_count', 'Blocs analysés'), ('start_boundary_time', 'Début UTC (exclu)'),
                     ('end_time', 'Fin UTC (incluse)'), ('duration_seconds', 'Durée (s)'),
                     ('utc_day_coverage_pct', '% du temps par journée UTC'),
                     ('tx_total', 'Transactions observées'), ('tx_ibc', 'Transactions IBC'),
                     ('collected_fee_atom', 'Fees collectées (ATOM)'),
                     ('fee_collection_status', 'Complétude des fees'),
                     ('collected_tip_atom', 'Tips inclus dans les fees (ATOM)'),
                     ('unprocessed_blocks_seen', 'Blocs restant à analyser'),
                     ('estimated_window_emission_atom', 'Émission estimée sur la fenêtre (ATOM)'),
                     ('fee_to_estimated_emission_pct', 'Fees / émission estimée (%)'),
                     ('net_supply_change_atom', 'Variation nette supply sur la fenêtre (ATOM)'),
                     ('fee_to_net_supply_increase_pct', 'Fees / hausse nette supply (%)'),
                     ('burn_status', 'Burn effectif'), ('atom_price_usd', 'Prix USD à la collecte'),
                     ('price_source_updated_at', 'Prix : timestamp source UTC')]
    states = load_days(ROOT / 'data' / 'criterion1_days')
    export_daily(ROOT / 'data' / 'criterion1_days', ROOT / DAILY_CSV.name)
    complete_days = [daily_summary(state, audit=load_fee_audit(state, ROOT / 'data' / 'criterion1_fee_audits')) for state in states if progress(state)['status'] == 'complete']
    day_progress = [progress(state) for state in states]
    daily_fields = [('date', 'Journée UTC'), ('block_count', 'Blocs analysés'),
                    ('block_coverage_pct', 'Couverture des blocs (%)'), ('tx_total', 'Transactions'),
                    ('collected_fee_atom', 'Fees collectées (ATOM, tips inclus)'),
                    ('collected_tip_atom', 'Dont tips (ATOM)'), ('fee_collection_status', 'Complétude des fees'),
                    ('fee_evidence_recovered', 'Frais attestés par transfert ante'),
                    ('estimated_emission_atom', 'Émission estimée sur les mêmes blocs (ATOM)'),
                    ('fee_to_estimated_emission_pct', 'Fees / émission estimée (%)'),
                    ('net_supply_change_atom', 'Variation nette supply sur les mêmes blocs (ATOM)'),
                    ('fee_to_net_supply_increase_pct', 'Fees / hausse nette supply (%)'),
                    ('measured_block_interval_seconds', 'Durée entre états de blocs (s)'), ('burn_status', 'Burn effectif')]
    legacy = read_rows(ROOT / 'hub_revenue_daily.csv')
    published = []
    for name in ('hub_revenue_daily.csv', 'hub_revenue_with_inflation.csv', WINDOWS.name, DAILY_CSV.name, 'ecosystem_out_usd_daily.csv'):
        source = ROOT / name
        if source.exists():
            shutil.copy2(source, out / name)
            published.append(f'<li><a href="{name}">{name}</a></li>')
    document = f'''<!doctype html>
<html lang="fr"><head><meta charset="utf-8"><title>Cosmos Hub Monitor</title>
<style>body{{font-family:Arial,sans-serif;margin:30px;line-height:1.5}}.scroll{{overflow-x:auto}}table{{border-collapse:collapse}}th,td{{border:1px solid #ddd;padding:8px;text-align:left}}.notice{{background:#fff5d6;padding:16px}}</style></head><body>
<p><a href="index.html">Retour au dashboard</a></p><h1>Cosmos Hub Monitor</h1><p>Généré : {html.escape(utcnow())}</p>
<h2>Critère 1 — journées UTC complètes</h2>
<p>Une journée comprend tous les blocs horodatés entre 00:00 UTC inclus et le minuit suivant exclu.
Les blocs précédant et suivant cet intervalle sont vérifiés. Une collecte partielle ne produit aucun total journalier.</p>
<p>Les ratios comparent les fees aux états de supply et aux estimations d'émission sur les mêmes blocs.
L'intervalle entre ces états diffère de quelques secondes des 86 400 secondes civiles ; sa durée réelle est affichée.
L'émission estimée est la somme des estimations de chaque lot, sans extrapoler les frais.
Aucun prix spot n'est utilisé comme prix historique journalier ; les valorisations USD journalières restent indisponibles.</p>
{table(complete_days, daily_fields) if complete_days else '<p>Aucune journée complète collectée pour le moment.</p>'}
<h3>Avancement de la collecte exhaustive</h3>
{table(day_progress, [('date','Journée UTC'), ('status','État'), ('scanned_blocks','Blocs traités'), ('expected_blocks','Blocs attendus'), ('block_coverage_pct','Couverture des blocs (%)')]) if day_progress else '<p>La première collecte visera la dernière journée UTC terminée.</p>'}
<h2>Critère 1 — anciennes fenêtres mesurées</h2>
<p>Chaque ligne représente les blocs indiqués, jamais un total journalier extrapolé.
La durée va du timestamp du bloc précédent au timestamp du dernier bloc.
Le pourcentage par jour mesure une couverture temporelle, pas une part de transactions ou de revenus journaliers.</p>
<p>Les fees collectées sont attestées par les événements tx/fee ou fee_pay/fee et tip_pay/tip. Pour certaines transactions échouées, un transfert ante au module historique de frais, corroboré par les événements de débit et de crédit, atteste le paiement. Les tips sont inclus une seule fois dans le total et présentés séparément. Si un événement manque, les fees collectées et ratios sont indisponibles.
L'émission estimée applique le taux et la supply observés au début de la fenêtre à sa durée, sur une base de 365 jours ; elle n'est pas une émission brute mesurée.
La variation de supply est nette et ne permet pas d'isoler le burn. Les frais ne sont jamais soustraits de la dilution.</p>
<p>Le prix affiché est le spot horodaté à la collecte. Sa valorisation éventuelle est une valorisation à la collecte, pas un prix historique de la fenêtre.
Les snapshots de supply et d'inflation sont acceptés uniquement si la hauteur historique est confirmée par le fournisseur.</p>
{table(windows, window_fields)}
<h2>Archives historiques — {len(legacy)} observations</h2>
<p class="notice">Les deux CSV historiques sont conservés à l'identique. La couverture des blocs et les timestamps des snapshots y sont inconnus.
Les anciens prix USD, taux d'inflation, ratios de couverture et calculs de dilution sont non validés ; ils ne sont pas affichés comme indicateurs fiables.
Les fees archivées sont des montants déclarés dans les transactions, sans preuve de burn.</p>
{table(legacy, [('date','Date archive'), ('tx_total','Transactions observées'), ('tx_ibc','Transactions IBC'), ('total_fee_atom','Fees déclarées observées (ATOM)')])}
<h2>Fichiers</h2><ul>{''.join(published)}</ul>
'''
    volume = ROOT / 'ecosystem_out_usd_daily.csv'
    if (out / 'criterion2.html').exists():
        html_link = '<h2>Critère 2 — flux IBC multichaînes</h2><p><a href="criterion2.html">Consulter les transferts, la couverture et les comparaisons</a></p>'
        document += html_link
    elif volume.exists():
        rows = read_rows(volume)
        document += '<h2>Critère 2 — données existantes</h2>' + table(rows, [(k, k) for k in rows[0]] if rows else [])
    document += '</body></html>'
    (out / 'criterion1.html').write_text(document, encoding='utf-8')
    from build_web_dashboard import main as build_dashboard
    build_dashboard(ROOT)
    print('Dashboard generated; source CSVs retained at repository root.')


if __name__ == '__main__':
    main()
