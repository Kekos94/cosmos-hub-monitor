"""Reviewed RWA identities; discovery only, no modification of economic series.

An issuer claim identifies a product, not its reserves or current network support.
IBC identities need an exact trace and historical channel/client evidence.
"""
import csv
import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path

from criterion2_core import digest
from criterion2_export import all_days,write_csv
from criterion2_pipeline import ROOT,DATA,IN_DATA,read
from criterion2_lifecycle_evidence import bank_denom
from criterion2_transit_evidence import topology

CONFIG=ROOT/'config/criterion2_rwa_registry.json'
FIELDS='registry_id review_date cohort_date direction chain_id packet_id height tx_hash observed_denom product_id symbol category identity_status included_in_strict_rwa amount_raw settlement_scope evidence_sha256'.split()


def load_registry(path=CONFIG,root=ROOT):
    cfg=read(path)
    if cfg['schema_version']!=1:raise ValueError('Unsupported RWA registry')
    sources={s['id']:s for s in read(Path(root)/cfg['sources_manifest'])}
    if len(sources)!=len(read(Path(root)/cfg['sources_manifest'])):raise ValueError('Duplicate RWA source identifier')
    snapshots={}
    for sid,s in sources.items():
        if 'file' not in s:continue
        raw=gzip.decompress((Path(root)/s['file']).read_bytes())
        if hashlib.sha256(raw).hexdigest()!=s['raw_sha256']:raise ValueError('RWA registry source hash mismatch')
        snapshots[sid]=json.loads(raw)
    products={p['id']:p for p in cfg['products']}
    if len(products)!=len(cfg['products']):raise ValueError('Duplicate RWA product')
    for p in products.values():
        if not p['product_source_ids'] or any(sources[s]['kind']!='issuer_product' for s in p['product_source_ids']):raise ValueError('Missing primary product source')
        if p['include_in_strict_rwa'] and (p['category']!='rwa_product' or p['identity_status']!='verified_native'):raise ValueError('Unverified product included in strict RWA')
        if p['identity_status']=='verified_native':
            if not p['origin_chain_id'] or not p['origin_denom']:raise ValueError('Missing native token identity')
            native=[]
            for sid in p['identity_source_ids']:
                if sid in snapshots:native.extend(a for a in snapshots[sid].get('assets',[]) if a['base']==p['origin_denom'])
            if len(native)!=1:raise ValueError('Missing or ambiguous native asset metadata')
            a=native[0]
            units=[int(u['exponent']) for u in a['denom_units'] if u['denom']==a['display']]
            if units!=[p['decimals']] or not any(u['denom']==a['base'] and u['exponent']==0 for u in a['denom_units']):raise ValueError('RWA decimals conflict')
    seen=set()
    for r in cfg['representations']:
        p=products[r['product_id']];identity=(r['chain_id'],bank_denom(r['denom']))
        if identity in seen:raise ValueError('Ambiguous RWA representation')
        seen.add(identity)
        if r['kind']=='native':
            if (r['chain_id'],r['denom'])!=(p['origin_chain_id'],p['origin_denom']):raise ValueError('Wrong native RWA identity')
        elif r['kind']=='ics20_single_hop':
            if r['denom']!='transfer/'+r['channel']+'/'+p['origin_denom'] or not r.get('historical_route_required'):raise ValueError('Unsupported IBC representation')
            pairs=[snapshots[s] for s in r['source_ids'] if s in snapshots and 'channels' in snapshots[s]]
            if len(pairs)!=1:raise ValueError('Missing pinned IBC channel registry')
            pair=pairs[0];match=False
            for local,remote in [('chain_1','chain_2'),('chain_2','chain_1')]:
                if pair[local]['chain_id']!=r['chain_id'] or pair[remote]['chain_id']!=p['origin_chain_id']:continue
                match=any(ch[local]==dict(channel_id=r['channel'],port_id='transfer') and ch[remote]==dict(channel_id=r['counterparty_channel'],port_id='transfer') for ch in pair['channels'])
            if not match:raise ValueError('Pinned channel registry differs from representation')
        else:raise ValueError('Unsupported representation type')
    return cfg,digest(cfg)[:16]


def identify(packet,plan,cfg,proof_root):
    """Unknown is unreviewed, never equivalent to non-RWA or zero exposure."""
    if packet['chain_id']!=plan['chain_id']:raise ValueError('Packet/plan chain mismatch')
    default=dict(product_id='',symbol='',category='unreviewed',identity_status='not_in_reviewed_allowlist',included_in_strict_rwa='false',evidence_sha256='')
    matches=[r for r in cfg['representations'] if r['chain_id']==packet['chain_id'] and bank_denom(r['denom'])==bank_denom(packet['denom'])]
    if len(matches)>1:raise ValueError('Ambiguous exact RWA identity')
    if not matches:return default
    r=matches[0];p=next(p for p in cfg['products'] if p['id']==r['product_id'])
    result=dict(product_id=p['id'],symbol=p['symbol'],category=p['category'],identity_status='pending_historical_route',included_in_strict_rwa='false',evidence_sha256='')
    if r['kind']=='ics20_single_hop':
        path=Path(proof_root)/(packet['chain_id']+'_'+str(packet['height'])+'_'+r['channel']+'.json')
        if not path.exists():return result
        proof=read(path)
        if (proof['chain_id'],proof['height'],proof['channel'],proof['source'])!=(packet['chain_id'],str(packet['height']),r['channel'],plan['source']):raise ValueError('Historical RWA proof key/provider mismatch')
        t=topology(proof['results']['Channel'],proof['results']['ChannelClientState'],packet['height'])
        if (t['chain_id'],t['counterparty_port'],t['counterparty_channel'])!=(p['origin_chain_id'],'transfer',r['counterparty_channel']):raise ValueError('Historical RWA route differs from issuer origin')
        evidence=digest(proof)
    else:
        # Native units must also agree with the already recorded cohort snapshot.
        metadata=plan['assets']['mapping'].get(p['origin_denom'])
        if not metadata or metadata['decimals']!=p['decimals']:raise ValueError('Native RWA units disagree with historical cohort')
        evidence=digest(dict(representation=r,historical_assets=plan['assets']))
    result.update(identity_status='verified_exact_identity',included_in_strict_rwa='true' if p['include_in_strict_rwa'] else 'false',evidence_sha256=evidence)
    return result


def audit(root=ROOT,config=CONFIG,out_root=DATA,in_root=IN_DATA):
    root=Path(root);cfg,uid=load_registry(config,root);proof_root=root/Path(cfg['sources_manifest']).parent
    rows=[];counts=Counter();unreviewed=Counter()
    for direction,source in [('out',out_root),('in',in_root)]:
        for d in all_days(source):
            if not d['complete']:continue
            for packet in d['packets']:
                identified=identify(packet,d['plan'],cfg,proof_root)
                counts[(identified['category'],identified['identity_status'])]+=1
                if identified['category']=='unreviewed':
                    unreviewed[(packet['chain_id'],packet['denom'])]+=1;continue
                if identified['category']!='rwa_product':continue # Stablecoins stay in separate summary categories.
                rows.append(dict(registry_id=uid,review_date=cfg['review_date'],cohort_date=d['plan']['date'],direction=direction,chain_id=packet['chain_id'],
                    packet_id=packet['packet_id'],height=packet['height'],tx_hash=packet['tx_hash'],observed_denom=packet['denom'],amount_raw=packet['amount_raw'],
                    settlement_scope='identification_only_not_a_success_or_volume_measure',**identified))
    rows.sort(key=lambda r:(r['cohort_date'],r['chain_id'],r['direction'],int(r['height']),r['packet_id']))
    path=root/'criterion2_rwa_identification.csv';write_csv(path,rows,FIELDS)
    summary=dict(registry_id=uid,review_date=cfg['review_date'],scope=cfg['scope'],counts=[dict(category=k[0],identity_status=k[1],packet_observations=v) for k,v in sorted(counts.items())],
        unreviewed_chain_denominations=len(unreviewed),strict_rwa_packet_observations=sum(r['included_in_strict_rwa']=='true' for r in rows),
        caveats=['Incoming and outgoing observations are separate; never sum them as unique economic movements.',
                 'No reserves, balances, market value, successful settlement, or current issuer/network support is certified by this identity audit.',
                 'Absent from this non-exhaustive allowlist means unreviewed, not non-RWA.','Historical packets are classified using the dated review; current backing is not retroactively certified.'])
    (root/'criterion2_rwa_identification_summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2)+'\n')
    return rows,summary


if __name__=='__main__':
    _,summary=audit();print(json.dumps(summary,ensure_ascii=False,indent=2))
