"""IBC v1 lifecycle evidence. A NOOP is not execution; an error is not a refund."""
import base64
import hashlib
import json
import re
from collections import Counter

from cosmos_tx_decode import fields,repeated,single
from criterion2_core import digest
from criterion2_receipts import identity
from fee_evidence import coins

PREFIX='/ibc.core.channel.v1.'
MESSAGE_KINDS={'MsgAcknowledgement':'acknowledge_packet','MsgTimeout':'timeout_packet','MsgTimeoutOnClose':'timeout_on_close_packet'}
KEYS={'sender','receiver','recipient','spender','amount','denom','tokens','refund_receiver','refund_amount','refund_denom','refund_tokens',
      'msg_index','module','action','success','error','acknowledgement'}


def attrs(event):
    result={}
    for item in event.get('attributes') or []:
        key,value=item['key'],item['value']
        if key not in KEYS and not key.startswith('packet_'):
            try:
                decoded=base64.b64decode(key,validate=True).decode()
                if decoded in KEYS or decoded.startswith('packet_'):
                    key=decoded;value=base64.b64decode(value,validate=True).decode()
            except (ValueError,UnicodeError):pass
        if key in result and result[key]!=value:raise ValueError('Conflicting lifecycle attributes')
        result[key]=value
    return result


def ack_result(raw,depth=0):
    """Only explicit, bounded schemas; never search arbitrary strings for 'success'."""
    if depth>4 or len(raw)>262144:return 'unknown','unsupported'
    try:
        value=json.loads(raw)
        if not isinstance(value,dict):return 'unknown','unsupported'
        if set(value)=={'error'} and isinstance(value['error'],str) and value['error']:
            return 'error','ics20_error'
        if set(value)=={'result'} and isinstance(value['result'],str):
            body=base64.b64decode(value['result'],validate=True)
            if body==b'\x01':return 'success','ics20_result'
            # Osmosis IBC hooks: a result envelope wraps the underlying ICS20 ack.
            wrapped=json.loads(body)
            if isinstance(wrapped,dict) and set(wrapped)=={'contract_result','ibc_ack'} and isinstance(wrapped['ibc_ack'],str):
                contract=wrapped['contract_result']
                if contract is not None:
                    if not isinstance(contract,str):return 'unknown','unsupported_hook_result'
                    base64.b64decode(contract,validate=True)
                status,_=ack_result(base64.b64decode(wrapped['ibc_ack'],validate=True),depth+1)
                return (('success','ibc_hooks') if status=='success' else ('unknown','inconsistent_hook_result'))
        if set(value)=={'app_acknowledgement','forward_relayer_address','underlying_app_success'}:
            if not isinstance(value['app_acknowledgement'],str) or not isinstance(value['forward_relayer_address'],str) or type(value['underlying_app_success']) is not bool:return 'unknown','unsupported_fee_ack'
            status,_=ack_result(base64.b64decode(value['app_acknowledgement'],validate=True),depth+1)
            if status in ('success','error') and value['underlying_app_success']==(status=='success'):return status,'ics29_fee_ack'
    except (ValueError,TypeError,UnicodeError,RecursionError):pass
    return 'unknown','unsupported'


def uint(raw,number):
    items=[(w,v) for n,w,v in fields(raw) if n==number]
    if any(w!=0 for w,_ in items):raise ValueError('Wrong protobuf integer wire type')
    values=[v for _,v in items]
    if len(values)>1:raise ValueError('Duplicate protobuf integer')
    return values[0] if values else 0


def packet(raw):
    seq=uint(raw,1)
    p=dict(packet_sequence=str(seq),packet_src_port=single(raw,2,required=True).decode(),
           packet_src_channel=single(raw,3,required=True).decode(),packet_dst_port=single(raw,4,required=True).decode(),
           packet_dst_channel=single(raw,5,required=True).decode())
    identity(p)
    data=single(raw,6,required=True)
    return p,data


def messages(tx):
    """Decode only IBC messages and authz wrappers, preserving top-level response indexes."""
    raw=base64.b64decode(tx['tx'],validate=True);body=single(raw,1,required=True);items=repeated(body,1)
    responses=[]
    response_data=tx['tx_result'].get('data')
    if response_data:
        try:
            encoded=base64.b64decode(response_data,validate=True)
            responses=[(single(a,1).decode(),single(a,2)) for a in repeated(encoded,2)]
        except (ValueError,UnicodeError):responses=[]
    result=[]
    def walk(any_bytes,outer,path,response=None,depth=0):
        if depth>4:return
        url=single(any_bytes,1,required=True).decode();data=single(any_bytes,2,required=True)
        if url=='/cosmos.authz.v1beta1.MsgExec':
            children=repeated(data,2)
            child_responses=repeated(response,1) if response is not None else []
            for i,child in enumerate(children):walk(child,outer,path+[i],child_responses[i] if len(child_responses)==len(children) else None,depth+1)
        elif url.startswith(PREFIX) and url[len(PREFIX):] in MESSAGE_KINDS:
            p,packet_data=packet(single(data,1,required=True));kind=MESSAGE_KINDS[url[len(PREFIX):]]
            result.append(dict(identity=identity(p),kind=kind,outer=outer,path=path,packet_data=packet_data,
                               acknowledgement=single(data,2) if kind=='acknowledge_packet' else b'',
                               response_result=uint(response,1) if response is not None else None))
    for i,item in enumerate(items):
        url=single(item,1,required=True).decode();response=None
        if len(responses)==len(items) and responses[i][0]==url+'Response':response=responses[i][1]
        walk(item,i,[],response)
    return result


def bank_denom(trace):
    return 'ibc/'+hashlib.sha256(trace.encode()).hexdigest().upper() if re.match(r'^[^/]+/channel-\d+/',trace) else trace


def refund_token(value):
    token=json.loads(value)
    if isinstance(token,list):
        if len(token)!=1:raise ValueError('Multiple refund tokens')
        token=token[0]
    denom=token['denom']
    if isinstance(denom,dict):denom=''.join(h['port_id']+'/'+h['channel_id']+'/' for h in denom.get('trace',[]))+denom['base']
    return denom,str(token['amount'])


def refund_proof(events,msg,data,ack_status,terminal_kind,peer_count):
    default=dict(refund_status='not_verified',refund_amount_raw='',refund_denom_local='',refund_evidence_sha256='')
    if peer_count!=1:return default
    if msg['response_result']==1:return default
    try:
        sender=data['sender'];amount=data['amount'];denom=data['denom'];local=bank_denom(denom)
        if not isinstance(amount,str) or not amount.isascii() or not amount.isdigit() or int(amount)<=0:return default
        scoped=[(e['type'],attrs(e)) for e in events if attrs(e).get('msg_index')==str(msg['outer'])]
        app=[]
        for kind,a in scoped:
            if kind=='timeout' and terminal_kind in ('timeout_packet','timeout_on_close_packet'):
                receiver=a.get('refund_receiver',a.get('receiver'))
                token=(a.get('refund_denom'),a.get('refund_amount'))
                if 'refund_tokens' in a:token=refund_token(a['refund_tokens'])
                if receiver==sender and token==(denom,amount):app.append(a)
            if kind=='fungible_token_packet' and ack_status=='error' and a.get('sender')==sender:
                token=(a.get('denom'),a.get('amount'))
                if 'tokens' in a:token=refund_token(a['tokens'])
                if token==(denom,amount):app.append(a)
        expected={local:int(amount)}
        transfers=[a for k,a in scoped if k=='transfer' and a.get('recipient')==sender and coins(a.get('amount',''))==expected]
        received=[a for k,a in scoped if k=='coin_received' and a.get('receiver')==sender and coins(a.get('amount',''))==expected]
        if len(app)!=1 or len(transfers)!=1 or len(received)!=1:return default
        transfer=transfers[0]
        spent=[a for k,a in scoped if k=='coin_spent' and a.get('spender')==transfer.get('sender') and coins(a.get('amount',''))==expected]
        if len(spent)!=1:return default
        return dict(refund_status='verified_bank_credit',refund_amount_raw=amount,refund_denom_local=local,
                    refund_evidence_sha256=digest(dict(application=app[0],transfer=transfer,credit=received[0],debit=spent[0],recipient_sha256=hashlib.sha256(sender.encode()).hexdigest())))
    except (ValueError,TypeError,KeyError):return default


def records(plan,tx):
    from criterion2_core import event_spec
    if int(tx['tx_result']['code'])!=0:return []
    event_type,port=event_spec(plan['scope']);events=tx['tx_result'].get('events') or [];decoded=[]
    if event_type!='write_acknowledgement':
        try:decoded=messages(tx)
        except (ValueError,UnicodeError):decoded=[]
    peers=Counter(m['outer'] for m in decoded);result=[]
    for index,event in enumerate(events):
        if event['type']!=event_type:continue
        a=attrs(event)
        if a.get(port)!='transfer':continue
        key=identity(a);record=dict(packet_id=f'{plan["chain_id"]}/evidence/{event_type}/{tx["hash"].upper()}/{index}',
          record_type='lifecycle_event',chain_id=plan['chain_id'],height=str(tx['height']),tx_index=int(tx['index']),
          tx_hash=tx['hash'].upper(),event_index=index,event_type=event_type,source_port=key[0],source_channel=key[1],
          destination_port=key[2],destination_channel=key[3],sequence=key[4],amount_raw='',packet_data_sha256='',
          acknowledgement_sha256='',ack_status='unknown',ack_scheme='unavailable',execution_status='unverified',
          refund_status='not_verified',refund_amount_raw='',refund_denom_local='',refund_evidence_sha256='')
        if event_type=='write_acknowledgement':
            raw=bytes.fromhex(a['packet_ack_hex']) if 'packet_ack_hex' in a else a.get('packet_ack','').encode()
            if 'packet_ack_hex' in a and 'packet_ack' in a and raw!=a['packet_ack'].encode():raise ValueError('Contradictory acknowledgement encodings')
            data=bytes.fromhex(a['packet_data_hex']) if 'packet_data_hex' in a else a.get('packet_data','').encode()
            if 'packet_data_hex' in a and 'packet_data' in a and data!=a['packet_data'].encode():raise ValueError('Contradictory packet encodings')
            record.update(execution_status='committed_write_ack',packet_data_sha256=hashlib.sha256(data).hexdigest() if data else '')
            record['ack_status'],record['ack_scheme']=ack_result(raw)
            record['acknowledgement_sha256']=hashlib.sha256(raw).hexdigest()
        else:
            candidates=[m for m in decoded if m['identity']==key and (m['kind']==event_type or event_type=='timeout_packet' and m['kind']=='timeout_on_close_packet')]
            if len(candidates)==1:
                msg=candidates[0];record['packet_data_sha256']=hashlib.sha256(msg['packet_data']).hexdigest()
                record['execution_status']='executed' if msg['response_result']==2 else 'noop' if msg['response_result']==1 else 'unverified'
                if event_type=='acknowledge_packet':
                    record['ack_status'],record['ack_scheme']=ack_result(msg['acknowledgement'])
                    record['acknowledgement_sha256']=hashlib.sha256(msg['acknowledgement']).hexdigest()
                if record['execution_status']=='executed' and (record['ack_status']=='error' or event_type.startswith('timeout')):
                    try:data=json.loads(msg['packet_data'])
                    except (ValueError,UnicodeError):data={}
                    record.update(refund_proof(events,msg,data,record['ack_status'],event_type,peers[msg['outer']]))
        result.append(record)
    return result
