"""Conservative fee proof for failed Cosmos fee-market transactions.

A message failure can preserve the ante transfer but omit post-handler fee_pay.
Never infer zero fees, and never treat an arbitrary user transfer as a fee.
"""
import base64
import hashlib
import re

from cosmos_tx_decode import decode_tx, fields, repeated, single


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

def verified_no_fee(response):
    """Recognize specific Gaia ante errors that leave no committed fee debit."""
    code = response.get('code')
    if (response.get('codespace') != 'sdk' or code not in (5, 30, 32)
            or response.get('rpc_event_count') != 0 or response.get('events')):
        return False
    try:
        raw = base64.b64decode(response['signed_tx'], validate=True)
        if hashlib.sha256(raw).hexdigest().upper() != response['txhash']:
            return False
        log = response['raw_log']
        if code == 30:
            # Gaia checks TxBody.timeout_height before its fee decorator.
            body = single(raw, 1, required=True)
            timeout = [(wire, value) for number, wire, value in fields(body) if number == 3]
            if len(timeout) != 1 or timeout[0][0] != 0:
                return False
            height = int(response['height'])
            expiry = timeout[0][1]
            return (0 < expiry < height and log ==
                    f'block height: {height}, timeout height: {expiry}: tx timeout height')

        # BaseApp discards the ante cache when the fee escrow or signature check fails.
        if code == 5:
            match = re.fullmatch(
                r'error escrowing funds: spendable balance ([0-9]+)([a-zA-Z][a-zA-Z0-9/:._-]*) '
                r'is smaller than ([0-9]+)\2: insufficient funds', log)
            if not match:
                return False
            balance, denom, amount = match.groups()
            fee = decode_tx(raw)['auth_info']['fee']['amount']
            return fee == [{'denom': denom, 'amount': amount}] and int(balance) < int(amount)

        match = re.fullmatch(
            r'account sequence mismatch, expected ([0-9]+), got ([0-9]+): incorrect account sequence', log)
        if not match:
            return False
        expected, got = map(int, match.groups())
        if expected == got:
            return False
        auth = single(raw, 2, required=True)
        for signer in repeated(auth, 1):
            sequence = [(wire, value) for number, wire, value in fields(signer) if number == 3]
            if sequence == [(0, got)] or (not sequence and got == 0):
                return True
        return False
    except (KeyError, TypeError, ValueError):
        return False
