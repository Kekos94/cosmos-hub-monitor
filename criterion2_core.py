"""Criterion 2: packet parsing, unit conversion and price-safe comparisons."""
import base64
import hashlib
import json
import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext

SCOPE = 'ics20_v1_gross_sent'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def utc(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Missing UTC offset')
    return result.astimezone(timezone.utc)


def day_bounds(day):
    d = date.fromisoformat(day)
    start = datetime.combine(d, datetime.min.time(), timezone.utc)
    return start, start + timedelta(days=1)


def attributes(event):
    result = {}
    for attr in event.get('attributes', []):
        key, value = attr['key'], attr['value']
        if not key.startswith('packet_'):
            try:
                decoded = base64.b64decode(key, validate=True).decode()
                if decoded.startswith('packet_'):
                    key = decoded
                    value = base64.b64decode(value, validate=True).decode()
            except (ValueError, UnicodeError):
                continue
        if key in result and result[key] != value:
            raise ValueError('Contradictory packet attributes')
        result[key] = value
    return result


def packets(chain_id, tx):
    """Failed transactions do not contribute emitted transfers."""
    if int(tx['tx_result']['code']) != 0:
        return []
    result = []
    for event_index, event in enumerate(tx['tx_result'].get('events') or []):
        if event['type'] != 'send_packet':
            continue
        a = attributes(event)
        if a.get('packet_src_port') != 'transfer':
            continue
        for key in ('packet_src_channel', 'packet_dst_port', 'packet_dst_channel', 'packet_sequence'):
            if not a.get(key):
                raise ValueError(f'Missing {key}')
        if not a['packet_sequence'].isdigit() or int(a['packet_sequence']) <= 0:
            raise ValueError('Invalid packet sequence')
        if not re.fullmatch(r'channel-\d+', a['packet_src_channel']):
            raise ValueError('Invalid source channel')
        if 'packet_data_hex' in a:
            raw = bytes.fromhex(a['packet_data_hex'])
            if 'packet_data' in a and raw != a['packet_data'].encode():
                raise ValueError('Packet data encodings disagree')
        elif 'packet_data' in a:
            raw = a['packet_data'].encode()
        else:
            raise ValueError('Missing packet data')
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeError) as exc:
            # Never count a transfer as zero when the packet format is unknown.
            raise ValueError('Unsupported transfer packet encoding') from exc
        amount, denom = body.get('amount'), body.get('denom')
        if not isinstance(amount, str) or not re.fullmatch(r'[0-9]+', amount) or int(amount) <= 0:
            raise ValueError('Invalid ICS20 amount')
        if not isinstance(denom, str) or not denom:
            raise ValueError('Missing ICS20 denom')
        sequence = str(int(a['packet_sequence']))
        packet_id = f'{chain_id}/transfer/{a["packet_src_channel"]}/{sequence}'
        result.append(dict(packet_id=packet_id, chain_id=chain_id, height=str(tx['height']),
                           tx_hash=tx['hash'].upper(), tx_index=int(tx['index']), event_index=event_index,
                           source_channel=a['packet_src_channel'], destination_port=a['packet_dst_port'],
                           destination_channel=a['packet_dst_channel'], sequence=sequence,
                           denom=denom, amount_raw=str(int(amount)), packet_data_sha256=hashlib.sha256(raw).hexdigest(),
                           settlement_status='sent_not_reconciled'))
    return result


def verify_search_tx(tx, plan):
    height, index = int(tx['height']), int(tx['index'])
    if not plan['start_height'] <= height <= plan['end_height'] or index < 0:
        raise ValueError('Search returned transaction outside requested range')
    raw = base64.b64decode(tx['tx'], validate=True)
    if hashlib.sha256(raw).hexdigest().upper() != tx['hash'].upper():
        raise ValueError('Search transaction hash mismatch')
    if not any(e['type'] == 'send_packet' and attributes(e).get('packet_src_port') == 'transfer'
               for e in tx['tx_result'].get('events') or []):
        raise ValueError('Search endpoint ignored the transfer filter')
    return height, index, tx['hash'].upper()


def asset_index(assetlist):
    mapping = {}
    for asset in assetlist.get('assets', []):
        base, display = asset['base'], asset['display']
        exponents = {int(unit['exponent']) for unit in asset['denom_units'] if unit['denom'] == display}
        raw_units = [u for u in asset['denom_units'] if u['denom'] == base and int(u['exponent']) == 0]
        if len(exponents) != 1 or not raw_units:
            continue
        exponent = exponents.pop()
        if not 0 <= exponent <= 36 or base in mapping:
            raise ValueError('Ambiguous asset units')
        mapping[base] = dict(base=base, symbol=asset.get('symbol', ''), decimals=exponent,
                             coingecko_id=asset.get('coingecko_id', ''))
    return mapping


def resolve_asset(denom, mapping):
    local = denom
    if denom.startswith('transfer/channel-'):
        local = 'ibc/' + hashlib.sha256(denom.encode()).hexdigest().upper()
    return mapping.get(local)


def decimal_amount(raw, decimals):
    with localcontext() as ctx:
        ctx.prec = max(80, len(str(raw)) + 40)
        return Decimal(str(raw)) / (Decimal(10) ** decimals)


def validate_price(record, day, asset):
    if record['date'] != day or record['coingecko_id'] != asset or record['method'] != 'coingecko_daily_history_reference':
        raise ValueError('Price does not match historical asset/day')
    price = Decimal(record['usd'])
    if not price.is_finite() or price <= 0:
        raise ValueError('Invalid historical price')
    utc(record['observed_at'])
    start,_ = day_bounds(day)
    if utc(record['reference_time_utc']) != start or record['source_date_parameter'] != day:
        raise ValueError('Price reference is not the requested UTC day')
    return price


def valued_packet(packet, mapping, prices, day):
    asset = resolve_asset(packet['denom'], mapping)
    result = dict(packet, asset_base='', asset_symbol='', decimals='', amount_display='',
                  coingecko_id='', reference_price_usd='', out_usd='', valuation_status='unknown_asset')
    if asset is None:
        return result
    amount = decimal_amount(packet['amount_raw'], asset['decimals'])
    result.update(asset_base=asset['base'], asset_symbol=asset['symbol'], decimals=asset['decimals'],
                  amount_display=str(amount), coingecko_id=asset['coingecko_id'], valuation_status='missing_historical_price')
    price = prices.get(asset['coingecko_id'])
    if price is not None:
        rate = validate_price(price, day, asset['coingecko_id'])
        with localcontext() as ctx:
            ctx.prec = 100
            result.update(reference_price_usd=str(rate), out_usd=str(amount * rate), valuation_status='valued')
    return result


def compare_day(day, universe, summaries):
    with localcontext() as ctx:
        ctx.prec = 200
        return _compare_day(day, universe, summaries)


def _compare_day(day, universe, summaries):
    if set(summaries) != set(universe):
        raise ValueError('Cannot compare incomplete chain universe')
    if any(r['date'] != day or r['coverage_status'] != 'indexed_complete' for r in summaries.values()):
        raise ValueError('Cannot compare partial or different UTC days')
    known = sum((Decimal(r['valued_out_usd']) for r in summaries.values()), Decimal(0))
    unpriced = sum(int(r['unpriced_packets']) for r in summaries.values())
    packets_total = sum(int(r['sent_packets']) for r in summaries.values())
    hub = summaries['cosmoshub']
    return dict(date=day, scope=SCOPE, chains=';'.join(universe), indexed_chains_complete=len(universe),
                sent_packets=packets_total, priced_packets=packets_total-unpriced, unpriced_packets=unpriced,
                valued_out_usd=str(known), total_out_usd=str(known) if unpriced == 0 else '',
                hub_valued_out_usd=hub['valued_out_usd'],
                hub_share_valued_usd_pct=str(Decimal(hub['valued_out_usd'])/known*100) if known > 0 else '',
                hub_share_total_usd_pct=str(Decimal(hub['valued_out_usd'])/known*100) if known > 0 and unpriced == 0 else '',
                hub_share_packet_count_pct=str(Decimal(hub['sent_packets'])/packets_total*100) if packets_total else '',
                valuation_status='complete' if unpriced == 0 else 'partial_valued_subset',
                index_coverage_caveat='completeness_depends_on_provider_event_index', settlement_status='gross_sent_not_net_settled')
