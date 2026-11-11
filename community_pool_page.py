"""Build the bilingual community-pool register from saved observations."""
import json
from html import escape

from community_pool_deposits import ADDRESS, CSV, PROOF, ROOT

COPY = {
    'fr': {
        'title': 'Versements au fonds communautaire',
        'back': 'Retour au tableau de bord',
        'scope': 'Transferts explicites vers le fonds communautaire, relevés dans l’index des transactions du Cosmos Hub. Les crédits automatiques du protocole sont suivis hors de ce registre.',
        'revenue': 'Une recette est comptabilisée après attribution à un accord ou à un service rémunéré. Accord Injective USDC : encaissement non vérifié lors de la vérification du 8 novembre 2026.',
        'coverage': 'Hauteurs interrogées',
        'observed': 'Collecte UTC',
        'empty': 'En attente du premier relevé.',
        'headers': ('Date UTC', 'Actif', 'Montant', 'Expéditeur', 'Attribution', 'Transaction'),
        'migration_test': 'Test de migration',
        'migration_return': 'Restitution après migration',
        'unclassified': 'À qualifier',
        'evidence': 'Justificatif',
        'raw': 'unités brutes',
    },
    'en': {
        'title': 'Community pool deposits',
        'back': 'Back to the dashboard',
        'scope': 'Explicit transfers to the community pool, as reported by the Cosmos Hub transaction index. Automatic protocol credits are outside this register.',
        'revenue': 'A receipt is counted as revenue once linked to an agreement or a paid service. Injective USDC agreement: settlement unverified as of 8 November 2026.',
        'coverage': 'Queried block heights',
        'observed': 'Collected at (UTC)',
        'empty': 'Waiting for the first observation.',
        'headers': ('UTC date', 'Asset', 'Amount', 'Sender', 'Attribution', 'Transaction'),
        'migration_test': 'Migration test',
        'migration_return': 'Returned after migration',
        'unclassified': 'Unclassified',
        'evidence': 'Evidence',
        'raw': 'base units',
    },
}


def build(root):
    proof = root / PROOF.relative_to(ROOT)
    snapshot = json.loads(proof.read_text()) if proof.exists() else None
    for lang, labels in COPY.items():
        suffix = '-en' if lang == 'en' else ''
        title = labels['title']
        headers = ''.join(f'<th scope="col">{heading}</th>' for heading in labels['headers'])
        coverage = labels['empty']
        rows = []
        if snapshot:
            coverage = (f'{labels["coverage"]} {snapshot["from_height"]}–{snapshot["through_height"]} · '
                        f'{labels["observed"]} {escape(snapshot["observed_at"])} · {escape(snapshot["source"])}')
            for row in reversed(snapshot['records']):
                tx = escape(row['tx_hash'], quote=True)
                category = labels[row['category']]
                evidence = row['evidence_url']
                if evidence:
                    category += f' · <a href="{escape(evidence, quote=True)}">{labels["evidence"]}</a>'
                asset = escape(row['asset'])
                amount = escape(row['amount_display'])
                if row['asset'] == row['denom']:
                    amount += ' ' + labels['raw']
                rows.append(f'<tr><td>{escape(row["time_utc"])}</td><td>{asset}</td><td>{amount}</td>'
                            f'<td>{escape(row["sender"])}</td><td>{category}</td>'
                            f'<td><a href="https://www.mintscan.io/cosmos/tx/{tx}">{tx[:12]}…</a></td></tr>')
        table = f'<div class="lifecycle-scroll"><table><thead><tr>{headers}</tr></thead><tbody>{"".join(rows)}</tbody></table></div>' if rows else ''
        document = f'''<!doctype html>
<html lang="{lang}"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>1C — {title}</title><link rel="stylesheet" href="dashboard.css"></head>
<body><main class="shell">
<p><a href="criterion1-services{suffix}.html#services">{labels['back']}</a> · <a href="community-pool.html" lang="fr">Français</a> · <a href="community-pool-en.html" lang="en">English</a></p>
<section class="criterion"><div class="section-heading"><div><p class="eyebrow">COSMOS HUB · 1C</p><h1>{title}</h1></div><a href="{CSV}" download>CSV ↓</a></div>
<p>{labels['scope']}</p><p>{labels['revenue']}</p>
<p><a href="https://www.mintscan.io/cosmos/address/{ADDRESS}">{ADDRESS}</a></p>
<p class="chart-note">{coverage}</p>{table}
</section></main></body></html>
'''
        (root / 'site' / f'community-pool{suffix}.html').write_text(document, encoding='utf-8')
