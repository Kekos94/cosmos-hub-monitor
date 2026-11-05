"""Conservative fee proof for failed Cosmos fee-market transactions.

A message failure can preserve the ante transfer but omit post-handler fee_pay.
Never infer zero fees, and never treat an arbitrary user transfer as a fee.
"""
import re


def coins(value):
    result = {}
    for part in value.split(',') if value else []:
        match = re.fullmatch(r'(\d+)([a-zA-Z][a-zA-Z0-9/:._-]*)', part)
        if not match or match[2] in result:
            raise ValueError('Invalid or duplicate coin in fee proof')
        result[match[2]] = int(match[1])
    return result


def failed_ante_fee(tx, proof):
    response = tx['tx_response']
    events = response.get('events') or []
    if not proof or int(response.get('code', 0)) == 0:
        return None, None
    account = proof.get('account', {})
    if (str(proof.get('height')) != str(response.get('height')) or
        account.get('@type') != '/cosmos.auth.v1beta1.ModuleAccount' or
        account.get('name') != 'feemarket-fee-collector'):
        return None, None
    address = account.get('base_account', {}).get('address')
    if not address or not address.startswith('cosmos1'):
        return None, None
    parsed = []
    for event in events:
        attrs = {}
        for attr in event.get('attributes', []):
            if attr['key'] in attrs:
                return None, None
            attrs[attr['key']] = attr['value']
        # Do not reinterpret contradictory fee events or message-level transfers.
        if (event['type'] in ('fee_pay', 'tip_pay') or
            (event['type'] == 'tx' and 'fee' in attrs) or
            'action' in attrs or 'msg_index' in attrs):
            return None, None
        parsed.append((event['type'], attrs))
    transfers = [a for kind, a in parsed if kind == 'transfer' and a.get('recipient') == address]
    received = [a for kind, a in parsed if kind == 'coin_received' and a.get('receiver') == address]
    if len(transfers) != 1 or len(received) != 1:
        return None, None
    transfer = transfers[0]
    fee = tx['tx']['auth_info']['fee']
    sequences = [a['acc_seq'].split('/')[0] for kind, a in parsed if kind == 'tx' and 'acc_seq' in a]
    payer = fee.get('granter') or fee.get('payer') or (sequences[0] if sequences else '')
    if not payer or transfer.get('sender') != payer:
        return None, None
    # No outgoing collector transfer means this proof makes no tip/burn claim.
    if any((kind == 'coin_spent' and a.get('spender') == address) or
           (kind == 'transfer' and a.get('sender') == address) for kind, a in parsed):
        return None, None
    amounts = coins(transfer.get('amount', ''))
    declared_coins = fee.get('amount') or []
    declared = {c['denom']: int(c['amount']) for c in declared_coins}
    if len(declared) != len(declared_coins) or any(value < 0 for value in declared.values()):
        return None, None
    if not amounts or amounts != declared or coins(received[0].get('amount', '')) != amounts:
        return None, None
    spent = [a for kind, a in parsed if kind == 'coin_spent' and a.get('spender') == payer]
    if len(spent) != 1 or coins(spent[0].get('amount', '')) != amounts:
        return None, None
    return amounts.get('uatom', 0), 0
