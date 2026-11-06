"""Publish Criterion 2 independently, without rewriting Criterion 1 artifacts."""
import html
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from criterion2_pipeline import ROOT, load_config
from criterion2_export import exports


def table(rows,fields):
    if not rows:return '<p>Aucune donnée complète disponible.</p>'
    headers=''.join('<th>'+html.escape(label)+'</th>' for _,label in fields)
    def display(row,key):
        value=row.get(key,'')
        if value=='':return 'indisponible'
        if key.endswith(('_usd','_pct')):
            return format(Decimal(str(value)),',.2f').replace(',',' ').replace('.',',')
        return str(value)
    body=''.join('<tr>'+''.join('<td>'+html.escape(display(r,key))+'</td>' for key,_ in fields)+'</tr>' for r in rows[-30:])
    return '<div class="scroll"><table><tr>'+headers+'</tr>'+body+'</table></div>'


def main():
    progress,chains,assets,comparisons=exports()
    cfg,uid=load_config();site=ROOT/'site';site.mkdir(exist_ok=True)
    files=['criterion2_progress.csv','ibc_out_chains_daily.csv','ibc_out_assets_daily.csv','ecosystem_out_usd_daily.csv']
    for name in files:shutil.copy2(ROOT/name,site/name)
    links=''.join(f'<li><a href="{name}">{name}</a></li>' for name in files)
    document=f'''<!doctype html><html lang="fr"><head><meta charset="utf-8"><title>Cosmos Monitor — Critère 2</title>
<style>body{{font:16px/1.5 Arial,sans-serif;margin:30px}}table{{border-collapse:collapse}}td,th{{border:1px solid #ddd;padding:8px}}.scroll{{overflow:auto}}.notice{{background:#fff3d1;padding:16px}}</style></head><body>
<p><a href="index.html">Retour au dashboard</a> · <a href="criterion1.html">Critère 1 détaillé</a></p><h1>Critère 2 — flux IBC sortants</h1>
<p>Généré : {datetime.now(timezone.utc).isoformat()}</p>
<p>Périmètre configuré : {html.escape(', '.join(c['name'] for c in cfg['chains']))}.</p>
<p class="notice">Ces mesures portent sur les paquets ICS20 v1 émis via le port transfer, selon l'index de transactions du fournisseur.
Elles dépendent de la complétude de cet index ; elles ne constituent pas un audit de chaque bloc.
Ce sont des flux bruts émis : les accusés de réception, erreurs et remboursements ne sont pas réconciliés.
Les protocoles IBC v2, NFT, CCTP et les autres ports ne sont pas inclus.</p>
<h2>Avancement — aucune extrapolation des données partielles</h2>
{table(progress,[('date','Jour UTC'),('chain','Chaîne'),('coverage_status','Couverture indexée'),('indexed_transactions_saved','Transactions contrôlées'),('indexed_transactions_total','Résultats annoncés par l’index'),('sent_packets_saved','Paquets observés'),('source','Fournisseur')])}
<h2>Comparaison sur les journées communes aux quatre chaînes</h2>
<p>Une comparaison exige la même journée UTC complètement parcourue dans chaque index du périmètre.
La part USD valorisée ne concerne que les actifs dont les unités et le prix historique sont connus.
La part USD totale reste indisponible dès qu'un actif est non valorisé. USDC n'est pas fixé arbitrairement à 1 USD.</p>
{table(comparisons,[('date','Jour UTC'),('chains','Périmètre'),('sent_packets','Paquets émis'),('unpriced_packets','Paquets non valorisés'),('valued_out_usd','Flux valorisés (USD de référence)'),('hub_share_valued_usd_pct','Part Hub du sous-ensemble valorisé (%)'),('hub_share_total_usd_pct','Part Hub totale (%)'),('hub_share_packet_count_pct','Part Hub des paquets (%)'),('valuation_status','Valorisation')])}
<h2>Flux par chaîne — journées indexées complètes uniquement</h2>
{table(chains,[('date','Jour UTC'),('chain','Chaîne'),('sent_packets','Paquets'),('priced_packets','Valorisés'),('unpriced_packets','Non valorisés'),('valued_out_usd','Sous-total valorisé USD'),('total_out_usd','Total USD si complet')])}
<p>Les prix sont des références historiques journalières CoinGecko, pas des prix d'exécution. Chaque prix valide est figé avec sa date et son horodatage de collecte.
Les tokens sont convertis selon leurs propres unités du Chain Registry ; aucun montant brut de tokens différents n'est additionné.</p>
<p>Cette part des sorties mesure l'activité relative dans ce périmètre. Elle ne prouve ni une centralité de routage universelle du Hub,
ni une part des RWA. Les multi-sauts et transferts de retour peuvent représenter plusieurs émissions pour un même mouvement économique.</p>
<h2>Fichiers</h2><ul>{links}</ul></body></html>'''
    (site/'criterion2.html').write_text(document,encoding='utf-8')
    from build_web_dashboard import main as build_dashboard
    build_dashboard(ROOT)
    print('Criterion 2 page generated; Criterion 1 outputs unchanged.')


if __name__=='__main__':main()
