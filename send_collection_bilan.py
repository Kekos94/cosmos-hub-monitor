"""Send a summary of a completed collection using the last saved results."""
import csv
import json
import os
import smtplib
import ssl
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from zoneinfo import ZoneInfo

WORKFLOWS = {
    'Daily Cosmos Monitor': 'Économie',
    'Criterion 1A, 1C, 1D — Hub economy': 'Économie',
    'Criterion 2 — IBC multichain': 'IBC',
    'Criterion 1B — Multichain IBC flows': 'IBC',
}
OUTCOMES = {
    'success': 'Réussite',
    'failure': 'Échec',
    'cancelled': 'Annulation',
    'timed_out': 'Délai dépassé',
    'skipped': 'Ignorée',
}


def rows_for(filename, day):
    path = Path(filename)
    if not path.is_file():
        return []
    with path.open(encoding='utf-8', newline='') as stream:
        return [row for row in csv.DictReader(stream) if row['date'] == day]


def economy(day):
    checkpoint = Path('data/criterion1_days') / f'{day}.json'
    if not checkpoint.is_file():
        return ['Aucun bloc enregistré pour cette journée.']
    state = json.loads(checkpoint.read_text(encoding='utf-8'))
    scanned = sum(int(chunk['block_count']) for chunk in state['chunks'])
    expected = int(state['end_height']) - int(state['start_height']) + 1
    lines = [f'Blocs enregistrés : {scanned}/{expected}']
    rows = rows_for('hub_criterion1_daily_complete.csv', day)
    if rows:
        fee = rows[0]['collected_fee_atom']
        lines.append(f'Frais collectés vérifiés : {fee} ATOM' if fee else 'Frais collectés : indisponibles')
        lines.append(f"Transactions dont la preuve de frais manque : {rows[0]['tx_without_fee_event']}")
    return lines


def ibc(day):
    lines = []
    config = json.loads(Path('config/criterion2_chains.json').read_text(encoding='utf-8'))
    for label, filename in (
        ('Entrées', 'criterion2_incoming_progress.csv'),
        ('Sorties', 'criterion2_progress.csv'),
        ('Flux nets', 'ibc_net_chains_daily.csv'),
    ):
        rows = rows_for(filename, day)
        status = 'common_indexed_day' if label == 'Flux nets' else 'indexed_complete'
        complete = {row['chain_id'] for row in rows if row['coverage_status'] == status}
        expected = {chain['chain_id'] for chain in config['chains']}
        lines.append(f'{label} : {len(complete & expected)}/{len(expected)} chaînes complètes')
    transit = rows_for('ibc_transit_daily.csv', day)
    if transit:
        row = transit[-1]
        coverage = 'complète' if row['collection_status'] == 'candidate_proofs_complete' else 'partielle'
        lines.append(f"Transit PFM : {row['confirmed_transit']} confirmés, couverture {coverage}")
    else:
        lines.append('Transit PFM : collecte indisponible')
    return lines


def message_for(run, address):
    name = WORKFLOWS[run['name']]
    started = datetime.fromisoformat(run['created_at'].replace('Z', '+00:00'))
    local = started.astimezone(ZoneInfo('Europe/Paris'))
    day = (started.astimezone(timezone.utc).date() - timedelta(days=1)).isoformat()
    outcome = OUTCOMES.get(run['conclusion'], run['conclusion'])
    lines = economy(day) if name == 'Économie' else ibc(day)
    message = EmailMessage()
    message['From'] = address
    message['To'] = address
    message['Subject'] = f'Cosmos Observatory | {name} | {outcome} | {day}'
    message.set_content('\n'.join([
        f'{name} · {outcome}',
        f'Lancement le {local:%d/%m/%Y à %H h %M}, heure de Paris.',
        '',
        f'Derniers résultats enregistrés pour la journée UTC du {day}',
        *lines,
        '',
        f"Exécution GitHub : {run['html_url']}",
        'Tableau de bord : https://kekos94.github.io/cosmos-hub-monitor/',
    ]))
    return message


def main():
    address = os.environ.get('BILAN_GMAIL_ADDRESS', '')
    password = os.environ.get('BILAN_GMAIL_APP_PASSWORD', '').replace(' ', '')
    if not address or not password:
        print('Envoi en attente de configuration des deux secrets BILAN_GMAIL.')
        return
    event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text(encoding='utf-8'))
    message = message_for(event['workflow_run'], address)
    with smtplib.SMTP('smtp.gmail.com', 587, timeout=30) as smtp:
        smtp.starttls(context=ssl.create_default_context())
        print('Connexion SMTP chiffrée établie sur le port 587.', flush=True)
        smtp.login(address, password)
        print('Authentification Gmail réussie.', flush=True)
        smtp.send_message(message)
        print('Bilan accepté par le serveur SMTP Gmail.', flush=True)


if __name__ == '__main__':
    main()
