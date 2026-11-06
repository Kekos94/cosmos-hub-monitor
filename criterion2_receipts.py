"""ICS20 v1 receptions: distinguish observed receipt from application success."""
import base64
import hashlib
import json
import re

from criterion2_core import attributes, packets,digest

SUCCESS='success_ack_in_receive_tx'
ERROR='error_ack_in_receive_tx'
UNKNOWN='unconfirmed_ack_in_receive_tx'


def identity(a):
    fields=('packet_src_port','packet_src_channel','packet_dst_port','packet_dst_channel','packet_sequence')
    if any(not a.get(k) for k in fields):raise ValueError('Incomplete reception/ack identity')
    if not a['packet_sequence'].isdigit() or int(a['packet_sequence'])<=0:raise ValueError('Invalid reception sequence')
    if any(not re.fullmatch(r'channel-\d+',a[k]) for k in ('packet_src_channel','packet_dst_channel')):
        raise ValueError('Invalid reception channel')
    return tuple(str(int(a[k])) if k=='packet_sequence' else a[k] for k in fields)


def acknowledgement(a):
    raw=bytes.fromhex(a['packet_ack_hex']) if 'packet_ack_hex' in a else a.get('packet_ack','').encode()
    if 'packet_ack_hex' in a and 'packet_ack' in a and raw!=a['packet_ack'].encode():
        raise ValueError('Acknowledgement encodings disagree')
    status=UNKNOWN
    try:
        ack=json.loads(raw)
        if isinstance(ack,dict) and set(ack)=={'result'} and base64.b64decode(ack['result'],validate=True)==b'\x01':status=SUCCESS
        elif isinstance(ack,dict) and set(ack)=={'error'} and isinstance(ack['error'],str) and ack['error']:status=ERROR
    except (ValueError,TypeError):pass
    return status,hashlib.sha256(raw).hexdigest()


def received_denom(a,denom):
    # ICS20: returning vouchers are unescrowed after stripping the sender prefix;
    # otherwise the destination channel is prefixed before minting a local voucher.
    prefix=a['packet_src_port']+'/'+a['packet_src_channel']+'/'
    result=denom[len(prefix):] if denom.startswith(prefix) else a['packet_dst_port']+'/'+a['packet_dst_channel']+'/'+denom
    if not result:raise ValueError('Empty received denomination')
    return result


def received_packets(chain_id,tx):
    if int(tx['tx_result']['code'])!=0:return []
    events=tx['tx_result'].get('events') or [];acks={};result=[];seen=set()
    for event in events:
        if event['type']!='write_acknowledgement':continue
        a=attributes(event)
        if a.get('packet_dst_port')!='transfer':continue
        key=identity(a);ack=acknowledgement(a)
        if key in acks and acks[key]!=ack:raise ValueError('Contradictory acknowledgements')
        acks[key]=ack
    for index,event in enumerate(events):
        if event['type']!='recv_packet':continue
        a=attributes(event)
        if a.get('packet_dst_port')!='transfer':continue
        key=identity(a)
        # Reuse the exact ICS20 v1 data/amount validation; only the event envelope changes.
        # The remote port may be a contract. The local receiving port is transfer;
        # normalization here is for payload validation only, never denom tracing.
        normalized=dict(a,packet_src_port='transfer')
        envelope=dict(tx,tx_result=dict(code=0,events=[dict(type='send_packet',attributes=[dict(key=k,value=v) for k,v in normalized.items()])]))
        status,ack_hash=acks.get(key,(UNKNOWN,''))
        try:
            packet=packets(chain_id,envelope)[0]
            packet['payload_status']='valid_ics20_v1'
        except (ValueError,TypeError,AttributeError) as exc:
            if status==SUCCESS:raise ValueError('Success ack with unsupported ICS20 payload') from exc
            # Rejected/malformed attempts are observations, never zero-valued inflows.
            packet=dict(chain_id=chain_id,height=str(tx['height']),tx_hash=tx['hash'].upper(),tx_index=int(tx['index']),
                        source_channel=a['packet_src_channel'],destination_port=a['packet_dst_port'],destination_channel=a['packet_dst_channel'],
                        sequence=str(int(a['packet_sequence'])),amount_raw='',denom='',packet_data_sha256='',
                        payload_status='invalid_or_unsupported',packet_attributes_sha256=digest(a))
        packet_id=f'{chain_id}/transfer/{a["packet_dst_channel"]}/{int(a["packet_sequence"])}'
        if packet_id in seen:raise ValueError('Duplicated reception')
        seen.add(packet_id)
        packet.update(packet_id=packet_id,event_index=index,source_port=a['packet_src_port'],packet_denom=packet['denom'],
                      denom=received_denom(a,packet['denom']) if packet['payload_status']=='valid_ics20_v1' else '',receipt_status=status,
                      acknowledgement_sha256=ack_hash,
                      settlement_status='receive_ack_observed_not_end_to_end_reconciled')
        result.append(packet)
    return result
