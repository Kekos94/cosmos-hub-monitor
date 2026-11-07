"""Publish Criterion 2 independently, without rewriting Criterion 1 artifacts."""
import html
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from criterion2_pipeline import ROOT, load_config
from criterion2_export import exports
from criterion2_incoming_export import exports as incoming_exports
from criterion2_lifecycle_export import exports as lifecycle_exports
from criterion2_net import exports as net_exports,config as net_config
from criterion2_windows import exports as window_exports
from criterion2_transit_export import exports as transit_exports
from criterion2_rwa_view import export as rwa_export


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
    in_progress,in_chains,in_assets,in_comparisons=incoming_exports()
    lifecycle,life_progress=lifecycle_exports()
    net_chains,net_assets=net_exports()
    net_chains=[r for r in net_chains if r['universe_id']==net_config()[-1]]
    window_chains,window_assets=window_exports()
    transit,transit_assets,transit_routes=transit_exports()
    rwa_export()
    cfg,uid=load_config();site=ROOT/'site';site.mkdir(exist_ok=True)
    files=['criterion2_progress.csv','ibc_out_chains_daily.csv','ibc_out_assets_daily.csv','ecosystem_out_usd_daily.csv']
    files+=['criterion2_incoming_progress.csv','ibc_in_chains_daily.csv','ibc_in_assets_daily.csv','ecosystem_in_usd_daily.csv']
    files+=['ibc_lifecycle_daily.csv','ibc_lifecycle_progress.csv','ibc_packet_lifecycle.csv.gz']
    files+=['ibc_net_chains_daily.csv','ibc_net_assets_daily.csv']
    files+=['ibc_net_windows_chains_latest.csv','ibc_net_windows_assets_latest.csv']
    files+=['ibc_transit_daily.csv','ibc_transit_assets_daily.csv','ibc_transit_routes_daily.csv','ibc_transit_packets.csv']
    files.append('ibc_rwa_daily.csv')
    for name in files:shutil.copy2(ROOT/name,site/name)
    links=''.join(f'<li><a href="{name}">{name}</a></li>' for name in files)
    document=f'''<!doctype html><html lang="fr"><head><meta charset="utf-8"><title>Cosmos Monitor — Critère 2</title>
<style>body{{font:16px/1.5 Arial,sans-serif;margin:30px}}table{{border-collapse:collapse}}td,th{{border:1px solid #ddd;padding:8px}}.scroll{{overflow:auto}}.notice{{background:#fff3d1;padding:16px}}</style></head><body>
<p><a href="index.html">Retour au dashboard</a> · <a href="criterion1.html">Critère 1 détaillé</a></p><h1>Critère 2 — flux IBC sortants</h1>
<p>Généré : {datetime.now(timezone.utc).isoformat()}</p>
<p>Périmètre configuré : {html.escape(', '.join(c['name'] for c in cfg['chains']))}.</p>
<p class="notice">Ces mesures portent sur les paquets ICS20 v1 émis via le port transfer, selon l'index de transactions du fournisseur.
Elles dépendent de la complétude de cet index ; elles ne constituent pas un audit de chaque bloc.
Ces séries conservent les flux bruts émis. Les confirmations et remboursements sont rapprochés séparément dans le suivi ci-dessous.
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
<h2 id="incoming">Entrées IBC — réceptions et accusés de succès</h2>
<p>Les réceptions sont datées sur la chaîne destinataire. Seuls les paquets ICS20 v1 avec un accusé de succès standard interprété dans la transaction de réception entrent dans les montants confirmés. Les accusés d’erreur sont exclus ; les accusés absents, asynchrones ou non interprétés restent non confirmés. Aucun zéro ne remplace une confirmation manquante. Les dénominations sont reconstruites selon le canal de réception, avec retrait du préfixe lors d’un retour à la chaîne source.</p>
<p>Ces entrées ne constituent ni un solde net, ni une mesure du transit. Un accusé de succès ne prouve pas un crédit définitif au bénéficiaire final à travers tous les sauts. Les jours d’émission et de réception peuvent différer. Les séries brutes restent séparées ; le solde des transferts réussis est présenté plus bas.</p>
{table(in_progress,[('date','Jour UTC'),('chain','Chaîne'),('coverage_status','Couverture indexée'),('indexed_transactions_saved','Transactions contrôlées'),('observed_packets_saved','Réceptions observées')])}
{table(in_comparisons,[('date','Jour UTC'),('chains','Périmètre'),('observed_packets','Réceptions observées'),('received_packets','Entrées confirmées'),('error_ack_packets','Accusés d’erreur'),('unconfirmed_packets','Réceptions non confirmées'),('valued_in_usd','Sous-total entrant valorisé USD'),('hub_share_confirmed_packet_count_pct','Part Hub des entrées confirmées (%)')])}
{table(in_chains,[('date','Jour UTC'),('chain','Chaîne'),('observed_packets','Réceptions observées'),('received_packets','Entrées confirmées'),('error_ack_packets','Accusés d’erreur'),('unconfirmed_packets','Réceptions non confirmées'),('unpriced_packets','Entrées confirmées non valorisées'),('valued_in_usd','Sous-total entrant valorisé USD')])}
<h2 id="lifecycle">Confirmations et remboursements — cohortes de paquets</h2>
<p>Les paquets restent rattachés à leur jour UTC d’émission ou de réception. Des preuves de transactions ultérieures peuvent compléter leur état. La couverture indique jusqu’à quelle borne UTC tous les index nécessaires ont été parcourus sans trou depuis la cohorte ; une recherche complète ne garantit pas que tous les paquets soient résolus.</p>
<p>Les répétitions sans effet (NOOP) sont exclues : une confirmation source exige une réponse de message exécutée. Les accusés standard, IBC hooks et ICS29 reconnus sont interprétés ; les autres restent inconnus. Un timeout confirmé ou un accusé d’erreur ne suffit pas à déclarer un remboursement : il faut un crédit bancaire correspondant, avec débit, montant, actif, bénéficiaire et contexte applicatif vérifiés. Les remboursements sur une chaîne source distante restent non vérifiés.</p>
<p>Les séries brutes et les entrées confirmées dans la transaction de réception gardent leur définition. Ce rapprochement ne calcule aucun flux net et ne prouve pas le règlement au bénéficiaire final à travers plusieurs sauts.</p>
{table(lifecycle,[('date','Cohorte UTC'),('chain','Chaîne'),('direction','Sens'),('tracked_packets','Paquets suivis'),('success_confirmed','Succès confirmés'),('error_acknowledged','Accusés d’erreur'),('timeout_confirmed','Timeouts confirmés'),('awaiting_evidence','Sans preuve concluante'),('unsupported_ack','Accusés non interprétés'),('conflicts','Preuves contradictoires'),('refunds_verified','Remboursements vérifiés'),('refunds_unverified','Remboursements non vérifiés'),('complete_through_utc','Index parcourus jusqu’au (UTC)'),('latest_evidence_day','Dernier jour de preuve')])}
{table(life_progress,[('date','Jour UTC'),('chain','Chaîne'),('scope','Index de preuve'),('coverage_status','Couverture indexée'),('scanned_heights','Hauteurs parcourues'),('expected_heights','Hauteurs attendues'),('events','Événements observés')])}
<h2 id="net-flows">Flux nets des transferts réussis</h2>
<p>Le solde est calculé par dénomination locale : entrées réussies moins sorties réussies. Les jours sont ceux de réception et d’émission, avec une même journée UTC indexée dans les deux sens sur les quatre chaînes. Les preuves sont arrêtées à une borne UTC commune ; les preuves postérieures ou issues d’un parcours incomplet sont exclues de ce calcul.</p>
<p>Les erreurs et timeouts confirmés sont exclus des transferts réussis. Les remboursements ne sont pas ajoutés une deuxième fois : ce solde n’est pas une variation de solde bancaire. Les paquets incertains bloquent le solde total de leur actif ; un prix ou des unités manquants bloquent sa valorisation. Aucun token n’est regroupé sur la seule base de son symbole.</p>
<p>Les sous-totaux USD utilisent exactement les mêmes actifs résolus et valorisés dans les deux sens pour chaque chaîne. Leur couverture peut différer entre chaînes : ce ne sont ni des totaux complets ni un classement. Le total USD reste indisponible dès qu’un actif ou un paquet manque. Un solde positif indique davantage d’entrées réussies que de sorties réussies sur la période ; il ne prouve ni une accumulation de capital, ni une centralité de transit.</p>
{table(net_chains,[('date','Cohorte UTC'),('chain','Chaîne'),('coverage_status','Couverture indexée'),('evidence_through_utc','Borne commune des preuves (UTC)'),('valued_assets','Actifs résolus valorisés'),('unpriced_assets','Actifs sans valorisation'),('unresolved_packets','Paquets incertains'),('valued_in_usd','Entrées du sous-ensemble USD'),('valued_out_usd','Sorties du sous-ensemble USD'),('valued_net_usd','Solde du sous-ensemble USD'),('total_net_usd','Solde total USD'),('net_status','État du solde')])}
<p>Le fichier par actif contient les montants bruts exacts, les unités, le prix historique de référence et le solde de chaque dénomination locale. Il conserve les calculs connus séparément des soldes complets. Les séries d’entrées et de sorties brutes gardent leur définition antérieure.</p>
<h2 id="windows">Cumuls sur 7, 30 et 90 jours</h2>
<p>Chaque fenêtre se termine à la dernière journée UTC close lors de la génération. Les jours couverts sont les journées communes indexées complètes des quatre chaînes dans les deux sens. Les jours manquants ne sont jamais remplacés par zéro. Le cumul observé porte uniquement sur ces jours ; le bilan complet exige tous les jours de la fenêtre et des états de confirmation compatibles.</p>
<p>Les montants sont additionnés par dénomination locale et avec des unités cohérentes. Pour un cumul USD, un actif doit être résolu et valorisé sur chacune de ses journées actives ; le même ensemble est utilisé pour les entrées et les sorties sur toute la fenêtre. Les valorisations journalières historiques sont additionnées, sans revaloriser le passé au dernier prix. Si les bornes de preuves diffèrent entre journées, les montants cumulés restent indisponibles.</p>
{table(window_chains,[('window_days','Fenêtre (jours)'),('start_date','Début UTC'),('end_date','Fin UTC incluse'),('chain','Chaîne'),('covered_days','Jours couverts'),('missing_days','Jours manquants'),('valued_in_usd','Entrées du sous-ensemble USD'),('valued_out_usd','Sorties du sous-ensemble USD'),('valued_net_usd','Solde du sous-ensemble USD'),('total_net_usd','Solde total USD'),('evidence_status','État des preuves')])}
<h2 id="transit">Transit PFM vérifié via le Cosmos Hub</h2>
<p>Seules les premières émissions PFM liées à une réception dans le même message exécuté sont attribuées. Les instructions de routage, le compte intermédiaire dérivé et les crédits et débits bancaires doivent correspondre. Les canaux et clients historiques identifient la chaîne source et le prochain saut. Un retour à la même chaîne est séparé du transit vers une autre chaîne.</p>
<p>Un transit confirmé exige un succès sur le paquet entrant et sur le paquet sortant à la même borne de preuves. Une nouvelle tentative après timeout reste indéterminée sans lien prouvé. Les transferts manuels, différés et les autres mécanismes ne sont pas mesurés. Les réceptions sans lien ne sont pas déclarées non transitaires. La couverture dépend des index du fournisseur et ne constitue pas un audit par client léger.</p>
{table(transit,[('date','Jour UTC'),('received_packets','Réceptions observées'),('inspected_transactions','Transactions candidates contrôlées'),('candidate_transactions','Transactions candidates'),('linked_attempts','Premières émissions PFM liées'),('confirmed_transit','Transits confirmés'),('return_to_source','Retours à la source'),('failed_forward','Échecs confirmés'),('unresolved','Liens sans règlement concluant'),('topology_unknown','Topologie inconnue'),('collection_status','Couverture des preuves'),('evidence_through_utc','Borne des preuves UTC')])}
{table(transit_routes,[('date','Jour UTC'),('source_chain','Chaîne source'),('via_chain','Via'),('next_chain','Prochain saut'),('confirmed_packets','Transits confirmés'),('failed_packets','Échecs confirmés'),('unresolved_packets','Non résolus')])}
<p>Les montants confirmés sont additionnés par actif local, une seule fois par réception. La valorisation utilise le prix historique de la journée de réception. Les montants sans unités ou prix restent indisponibles ; aucun total de tokens différents n’est calculé. Le prochain saut ne prouve pas le bénéficiaire final.</p>
{table(transit_assets,[('date','Jour UTC'),('symbol','Actif'),('bank_denom','Dénomination locale'),('confirmed_packets','Transits confirmés'),('amount_display','Montant confirmé'),('confirmed_usd','USD de référence'),('valuation_status','Valorisation')])}
<h2>Fichiers</h2><ul>{links}</ul></body></html>'''
    (site/'criterion2.html').write_text(document,encoding='utf-8')
    from build_web_dashboard import main as build_dashboard
    build_dashboard(ROOT)
    print('Criterion 2 page generated; Criterion 1 outputs unchanged.')


if __name__=='__main__':main()
