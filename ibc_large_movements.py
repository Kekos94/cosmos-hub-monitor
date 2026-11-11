"""Build the initial register of confirmed large transfers from saved IBC evidence."""
import csv
import gzip
import json
from collections import defaultdict
from decimal import Decimal
from html import escape

from criterion2_pipeline import inspect_day

MINIMUM_UATOM = 100_000_000_000
EXCEPTIONAL_UATOM = 1_000_000_000_000
HUB = 'cosmoshub-4'
MINTSCAN = 'https://www.mintscan.io'


def confirmed_movements(root, out_uid, in_uid):
    evidence = root / 'ibc_packet_lifecycle.csv.gz'
    if not evidence.exists():
        return {}
    with gzip.open(evidence, 'rt', encoding='utf-8', newline='') as stream:
        confirmed = {
            (row['universe_id'], row['date'], row['direction'], row['packet_id'])
            for row in csv.DictReader(stream)
            if row['chain_id'] == HUB and row['outcome'] == 'success_confirmed'
        }

    movements = defaultdict(list)
    for direction, directory, universe in (
        ('out', 'criterion2', out_uid), ('in', 'criterion2_incoming', in_uid)
    ):
        hub_days = root / 'data' / directory / universe / 'cosmoshub'
        for plan_file in sorted(hub_days.glob('*/plan.json')):
            day = inspect_day(plan_file.parent)
            plan = day['plan']
            if not day['complete'] or plan['chain_id'] != HUB or plan['universe_id'] != universe:
                continue
            for packet in day['packets']:
                if packet['denom'] != 'uatom':
                    continue
                key = (universe, plan['date'], direction, packet['packet_id'])
                amount = int(packet['amount_raw'])
                if amount >= MINIMUM_UATOM and key in confirmed:
                    movements[plan['date']].append((amount, direction, packet))
    return movements


def build(root, site, data, catalog, english):
    movements = confirmed_movements(root, data['universe_id'], data['incoming_universe_id'])
    accounts_file = root / 'config/ibc_large_movement_accounts.json'
    accounts = json.loads(accounts_file.read_text(encoding='utf-8')) if accounts_file.exists() else {}
    sections = []
    for day, records in sorted(movements.items(), reverse=True):
        cards = []
        for amount, direction, packet in sorted(records, key=lambda row: row[0], reverse=True):
            account = accounts.get(packet['packet_id'], {})
            if direction != 'out' or account.get('send_hash') != packet['tx_hash']:
                account = {}
            side = 'Sortie du Hub' if direction == 'out' else 'Entrée sur le Hub'
            value = format(Decimal(amount).scaleb(-6), ',.6f').translate(str.maketrans({',': '\u202f', '.': ','}))
            level = '<p class="badge">Mouvement exceptionnel</p>' if amount >= EXCEPTIONAL_UATOM else ''
            route = escape(packet['source_channel']) + ' → ' + escape(packet['destination_channel'])
            hub_tx = f'<a href="{MINTSCAN}/cosmos/tx/{escape(packet["tx_hash"])}">Transaction côté Hub ↗</a>'
            attribution = '<p>Comptes non identifiés</p>'
            if account:
                route = escape(account['source_chain']) + ' → ' + escape(account['destination_chain']) + ' · ' + route
                attribution = (
                    f'<dl><dt>{escape(account["sender_label"])}</dt><dd><code>{escape(account["sender"])}</code></dd>'
                    f'<dt>{escape(account["receiver_label"])}</dt><dd><code>{escape(account["receiver"])}</code></dd></dl>'
                    f'<p><a href="{MINTSCAN}/osmosis/tx/{escape(account["receive_hash"])}">Transaction de réception ↗</a>'
                    f' · <a href="{escape(account["source"])}">Source de l’attribution ↗</a></p>'
                )
            cards.append(f'<article class="panel"><h3>{side}</h3><div class="card-value"><span>{value}</span> <small>ATOM</small></div>'
                         f'{level}<p>{route}</p>{attribution}<p>{hub_tx}</p></article>')
        sections.append(f'<section class="criterion"><h2>{escape(day)} UTC</h2>{"".join(cards)}</section>')
    content = ''.join(sections) or '<p class="notice">Aucun grand mouvement confirmé dans les données disponibles.</p>'
    template = (root / 'web/ibc-large-movements.html').read_text(encoding='utf-8')
    for language, suffix in (('fr', ''), ('en', '-en')):
        page = template.replace('__MOVEMENTS__', content)
        if language == 'en':
            page = english(page, catalog)
        links = ''.join(
            f'<a href="ibc-large-movements{ending}.html" lang="{code}"'
            + (' aria-current="page"' if language == code else '') + f'>{label}</a>'
            for code, ending, label in (('fr', '', 'Français'), ('en', '-en', 'English'))
        )
        page = page.replace('__LANGUAGE_SWITCH__', f'<div class="language-switch" aria-label="Language">{links}</div>')
        (site / f'ibc-large-movements{suffix}.html').write_text(page, encoding='utf-8')
