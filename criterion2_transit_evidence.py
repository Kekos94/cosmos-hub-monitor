"""Strict same-message PFM forwarding proofs, not wallet/time/amount heuristics.

Model: ibc-apps packet-forward-middleware v10.6.0, SDK v0.53.8.
Only first forwarding attempts in the receive transaction are attributed.
"""
import base64
import hashlib
import json
import re
from collections import Counter
from decimal import Decimal

from cosmos_tx_decode import single,repeated
from criterion2_core import digest,packets
from criterion2_receipts import received_packets,identity
from criterion2_lifecycle_evidence import attrs,messages,bank_denom,uint
from fee_evidence import coins

MODEL='pfm_v10_6_same_recv_message_bank_evidence_v1'
B32='qpzry9x8gf2tvdw0s3jn54khce6mua7l'


def strict_json(raw):
    if len(raw)>262144:raise ValueError('Oversized JSON evidence')
    def pairs(items):
        result={}
        for k,v in items:
            if k in result:raise ValueError('Duplicate JSON field')
            result[k]=v
        return result
    return json.loads(raw,object_pairs_hook=pairs,parse_float=Decimal,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Non-finite JSON')))


def semantic(value,depth=0):
    if depth>12:raise ValueError('Memo too deeply nested')
    if isinstance(value,dict):return ('object',tuple((k,semantic(v,depth+1)) for k,v in sorted(value.items())))
    if isinstance(value,list):return ('array',tuple(semantic(v,depth+1) for v in value))
    if type(value) is bool:return ('boolean',value)
    if isinstance(value,(int,Decimal)):return ('number',Decimal(value))
    if value is None:return ('null',)
    if isinstance(value,str):return ('string',value)
    raise ValueError('Unsupported JSON value')


def pfm_sender(channel,sender):
    # SDK address.Hash(type, key) = SHA256(SHA256(type) || key), then first 20 bytes.
    raw=hashlib.sha256(hashlib.sha256(b'packetfowardmiddleware').digest()+(channel+'/'+sender).encode()).digest()[:20]
    values=[];acc=bits=0
    for b in raw:
        acc=(acc<<8)|b;bits+=8
        while bits>=5:bits-=5;values.append((acc>>bits)&31)
    if bits:values.append((acc<<(5-bits))&31)
    hrp='cosmos';expanded=[ord(x)>>5 for x in hrp]+[0]+[ord(x)&31 for x in hrp];polymod=1
    for v in expanded+values+[0]*6:
        top=polymod>>25;polymod=((polymod&0x1ffffff)<<5)^v
        for i,g in enumerate((0x3b6a57b2,0x26508e6d,0x1ea119fa,0x3d4233dd,0x2a1462b3)):
            if (top>>i)&1:polymod^=g
    polymod^=1
    return hrp+'1'+''.join(B32[v] for v in values+[(polymod>>(5*(5-i)))&31 for i in range(6)])


def packet_identity(p):
    return (p.get('source_port','transfer'),p['source_channel'],p['destination_port'],p['destination_channel'],p['sequence'])


def data_bytes(a):
    raw=bytes.fromhex(a['packet_data_hex']) if 'packet_data_hex' in a else a.get('packet_data','').encode()
    if 'packet_data_hex' in a and 'packet_data' in a and raw!=a['packet_data'].encode():raise ValueError('Contradictory packet encodings')
    return raw


def payload(raw):
    v=strict_json(raw)
    if not isinstance(v,dict) or not {'denom','amount','sender','receiver'}<=set(v) or set(v)-{'denom','amount','sender','receiver','memo'}:raise ValueError('Unsupported ICS20 payload')
    if any(not isinstance(v[k],str) or not v[k] for k in ('denom','amount','sender','receiver')):raise ValueError('Invalid ICS20 payload')
    if not re.fullmatch('[0-9]+',v['amount']) or int(v['amount'])<=0:raise ValueError('Invalid ICS20 amount')
    if not isinstance(v.get('memo',''),str):raise ValueError('Unsupported memo type')
    return v


def forward(body):
    memo=strict_json(body.get('memo',''))
    if not isinstance(memo,dict) or set(memo)!={'forward'}:raise ValueError('Not a supported PFM memo')
    f=memo['forward']
    if not isinstance(f,dict) or set(f)-{'receiver','port','channel','timeout','retries','next'}:raise ValueError('Unsupported forward metadata')
    if f.get('port')!='transfer' or not re.fullmatch(r'channel-\d+',str(f.get('channel',''))) or not isinstance(f.get('receiver'),str) or not f['receiver']:raise ValueError('Invalid forwarding route')
    if 'retries' in f and (type(f['retries']) is not int or not 0<=f['retries']<=255):raise ValueError('Unsupported retries')
    if 'timeout' in f and not (isinstance(f['timeout'],str) or type(f['timeout']) is int):raise ValueError('Unsupported timeout')
    nxt=f.get('next');expected='' if nxt is None else semantic(strict_json(nxt) if isinstance(nxt,str) else nxt)
    return f,expected


def bank_proof(events,outer,inbound_index,outbound_index,body,sender,local_denom):
    scoped=[(i,e['type'],attrs(e)) for i,e in enumerate(events) if attrs(e).get('msg_index')==str(outer)]
    expected={local_denom:int(body['amount'])}
    credit=[(i,a) for i,k,a in scoped if k=='coin_received' and a.get('receiver')==sender and coins(a.get('amount',''))==expected]
    debit=[(i,a) for i,k,a in scoped if k=='coin_spent' and a.get('spender')==sender and coins(a.get('amount',''))==expected]
    received=[(i,a) for i,k,a in scoped if k=='fungible_token_packet' and a.get('sender')==body['sender'] and a.get('receiver')==sender and a.get('denom')==body['denom'] and a.get('amount')==body['amount'] and a.get('success')=='true']
    sent=[(i,a) for i,k,a in scoped if k=='transfer' and a.get('sender')==sender and a.get('recipient')!=sender and coins(a.get('amount',''))==expected]
    if any(len(v)!=1 for v in (credit,debit,received,sent)):return ''
    if not inbound_index<credit[0][0]<received[0][0]<debit[0][0]<sent[0][0]<outbound_index:return ''
    return digest(dict(credit=credit[0],debit=debit[0],received=received[0],sent=sent[0]))


def verify_tx(tx,parents,children,chain_id='cosmoshub-4'):
    if not parents or not children:raise ValueError('Not a receive/send candidate transaction')
    h=hashlib.sha256(base64.b64decode(tx['tx'],validate=True)).hexdigest().upper()
    if tx['hash'].upper()!=h or int(tx['tx_result']['code'])!=0:raise ValueError('Invalid candidate transaction/hash')
    for r in parents+children:
        if r['chain_id']!=chain_id or r['tx_hash']!=h or int(r['height'])!=int(tx['height']) or int(r['tx_index'])!=int(tx['index']):raise ValueError('Candidate differs from original ledger')
    for expected,actual in [(parents,received_packets(chain_id,tx)),(children,packets(chain_id,tx))]:
        fields=('packet_id','event_index','packet_data_sha256','amount_raw','denom')
        normalize=lambda rows:sorted(tuple(str(r[f]) for f in fields) for r in rows)
        if normalize(expected)!=normalize(actual):raise ValueError('Candidate packet evidence differs from immutable ledger')


def links(tx,parents,children):
    verify_tx(tx,parents,children);events=tx['tx_result'].get('events') or [];result=[];diagnostics=[]
    try:decoded=messages(tx,message_kinds={'MsgRecvPacket':'recv_packet'})
    except (ValueError,UnicodeError):decoded=[]
    peers=Counter(m['outer'] for m in decoded)
    for parent in parents:
        reason='unsupported_forward_or_evidence';candidates=[]
        try:
            a=attrs(events[int(parent['event_index'])]);raw=data_bytes(a);body=payload(raw);f,next_memo=forward(body)
            source=[m for m in decoded if m['identity']==packet_identity(parent) and hashlib.sha256(m['packet_data']).hexdigest()==parent['packet_data_sha256'] and str(m['outer'])==a.get('msg_index')]
            if len(source)!=1 or source[0]['response_result']!=2 or peers[source[0]['outer']]!=1:
                reason='receive_execution_unverified'
            else:
                outer=source[0]['outer'];sender=pfm_sender(parent['destination_channel'],body['sender']);local=bank_denom(parent['denom'])
                for child in children:
                    ca=attrs(events[int(child['event_index'])]);cb=payload(data_bytes(ca))
                    if ca.get('msg_index')!=str(outer) or child['source_channel']!=f['channel'] or ca.get('packet_src_port')!=f['port']:continue
                    if cb['sender']!=sender or cb['receiver']!=f['receiver'] or int(cb['amount'])!=int(body['amount']) or cb['denom']!=parent['denom']:continue
                    child_memo='' if not cb.get('memo') else semantic(strict_json(cb['memo']))
                    if child_memo!=next_memo:continue
                    proof=bank_proof(events,outer,int(parent['event_index']),int(child['event_index']),body,sender,local)
                    if not proof:continue
                    record=dict(model=MODEL,parent_packet_id=parent['packet_id'],child_packet_id=child['packet_id'],chain_id=parent['chain_id'],
                      tx_hash=tx['hash'],height=str(tx['height']),tx_index=int(tx['index']),parent_event_index=parent['event_index'],child_event_index=child['event_index'],
                      parent_data_sha256=parent['packet_data_sha256'],child_data_sha256=child['packet_data_sha256'],denom=parent['denom'],bank_denom=local,amount_raw=parent['amount_raw'],
                      incoming_channel=parent['destination_channel'],outgoing_channel=child['source_channel'],
                      source_port=parent.get('source_port','transfer'),source_channel=parent['source_channel'],next_port=child['destination_port'],next_channel=child['destination_channel'],
                      incoming_connection=a.get('packet_connection',a.get('connection_id','')),outgoing_connection=ca.get('packet_connection',ca.get('connection_id','')),
                      bank_evidence_sha256=proof,memo_sha256=hashlib.sha256(body.get('memo','').encode()).hexdigest())
                    record['link_evidence_sha256']=digest(record);candidates.append(record)
                reason='no_unique_verified_forward' if len(candidates)!=1 else 'linked'
        except (ValueError,KeyError,TypeError,UnicodeError,RecursionError):
            candidates=[]
        if len(candidates)==1:result.append(candidates[0])
        diagnostics.append(dict(parent_packet_id=parent['packet_id'],status=reason))
    counts=Counter(r['child_packet_id'] for r in result);ambiguous={r['parent_packet_id'] for r in result if counts[r['child_packet_id']]!=1}
    result=[r for r in result if r['parent_packet_id'] not in ambiguous]
    for r in diagnostics:
        if r['parent_packet_id'] in ambiguous:r['status']='ambiguous_shared_child'
    return result,diagnostics


def encode_uint(n):
    out=b''
    while n>=128:out+=bytes([(n&127)|128]);n>>=7
    return out+bytes([n])


def encode_field(n,b):return encode_uint(n*8+2)+encode_uint(len(b))+b


def query_data(port,channel):return (encode_field(1,port.encode())+encode_field(2,channel.encode())).hex()


def topology(channel_result,client_result,height):
    values=[]
    for result in (channel_result,client_result):
        r=result['response']
        if int(r['code'])!=0 or int(r['height'])!=int(height):raise ValueError('Historical channel query unavailable/wrong height')
        values.append(base64.b64decode(r['value'],validate=True))
    ch=single(values[0],1,required=True);counterparty=single(ch,3,required=True);hops=[h.decode() for h in repeated(ch,4)]
    if uint(ch,1)!=3 or len(hops)!=1:raise ValueError('Channel not OPEN or unsupported connection hops')
    version=single(ch,5,required=True).decode()
    if version!='ics20-1':
        wrapper=strict_json(version)
        if not isinstance(wrapper,dict) or wrapper.get('app_version')!='ics20-1':raise ValueError('Unsupported channel version')
    identified=single(values[1],1,required=True);client_id=single(identified,1,required=True).decode();any_state=single(identified,2,required=True)
    if single(any_state,1,required=True).decode()!='/ibc.lightclients.tendermint.v1.ClientState' or not client_id.startswith('07-tendermint-'):raise ValueError('Unsupported remote client type')
    chain_id=single(single(any_state,2,required=True),1,required=True).decode()
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}',chain_id):raise ValueError('Invalid remote chain identity')
    return dict(chain_id=chain_id,client_id=client_id,connection_id=hops[0],counterparty_port=single(counterparty,1,required=True).decode(),
                counterparty_channel=single(counterparty,2,required=True).decode(),height=str(height),evidence_sha256=digest([channel_result,client_result]))
