"""Build the public dashboard from recorded exports, without mutating measurements."""
import csv
import json
import html
import hashlib
import re
import shutil
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from criterion1_daily import load_days, progress
from criterion2_pipeline import load_config,incoming_config
from criterion2_net import config as net_config
from criterion2_core import digest
from criterion2_transit_evidence import MODEL as TRANSIT_MODEL

ROOT = Path(__file__).resolve().parent


def english(document, catalog):
    def translate(match):
        raw=match.group(1); text=' '.join(html.unescape(raw).split())
        translated=catalog.get(text)
        if text.startswith('Généré : '):translated='Generated: '+text.removeprefix('Généré : ')
        if text.startswith('Périmètre configuré : '):translated='Configured scope: '+text.removeprefix('Périmètre configuré : ')
        if text.startswith('Archives historiques — '):translated='Historical archives — '+text.removeprefix('Archives historiques — ')
        if re.fullmatch(r'-?(?:\d{1,3}(?:[ \u202f]\d{3})+|\d+),\d+',text):
            translated=format(Decimal(text.replace(' ','').replace('\u202f','').replace(',','.')),',f')
        if translated is None:return match.group(0)
        return '>'+html.escape(translated,quote=False)+'<'
    document=re.sub(r'>([^<>]*)<',translate,document)
    document=document.replace('<html lang="fr">','<html lang="en">').replace('Navigation principale','Main navigation')
    for name in ('index','criterion1','criterion2'):
        document=document.replace(f'href="{name}.html"',f'href="{name}-en.html"')
    document=document.replace('href="index-en.html" lang="fr"','href="index.html" lang="fr"')
    document=document.replace('href="index-en.html" lang="en"','href="index-en.html" lang="en" aria-current="page"')
    document=document.replace('lang="fr" aria-current="page"','lang="fr"')
    document=document.replace('Observatoire du Cosmos Hub : couverture économique, activité IBC et qualité des données publiques.',
                              'Cosmos Hub observatory: economic coverage, IBC activity and public data quality.')
    return document


def rows(root, name):
    path = root / name
    if not path.exists():
        return []
    with path.open(encoding='utf-8', newline='') as stream:
        return list(csv.DictReader(stream))


def payload(root=ROOT):
    root = Path(root)
    cfg, uid = load_config(root / 'config/criterion2_chains.json')
    _,in_uid=incoming_config(root / 'config/criterion2_chains.json')
    _,_,_,net_uid=net_config(root/'config/criterion2_chains.json')
    transit_uid=digest(dict(model=TRANSIT_MODEL,incoming=in_uid,outgoing=uid))[:16]
    window_asset_fields='universe_id date window_days chain denom symbol decimals coingecko_id metadata_status confirmed_in_amount confirmed_out_amount net_amount observed_net_amount unresolved_packets excluded_packets'.split()
    # Never mix historical universes or substitute partially collected days.
    c1 = [r for r in rows(root, 'hub_criterion1_daily_complete.csv')
          if r['coverage_status'] == 'complete_utc_day_blocks']
    c2 = [r for r in rows(root, 'ecosystem_out_usd_daily.csv')
          if r['universe_id'] == uid and int(r['indexed_chains_complete']) == len(cfg['chains'])]
    return dict(generated_at=datetime.now(timezone.utc).isoformat(), universe_id=uid,
                members=[c['name'] for c in cfg['chains']],
                criterion1=sorted(c1, key=lambda r:r['date']),
                criterion1_progress=[progress(s) for s in load_days(root / 'data/criterion1_days')],
                criterion2=sorted(c2, key=lambda r:r['date']),
                chains=[r for r in rows(root, 'ibc_out_chains_daily.csv') if r['universe_id'] == uid],
                lifecycle=[r for r in rows(root,'ibc_lifecycle_daily.csv') if r['universe_id'] in (uid,in_uid)],
                lifecycle_progress=rows(root,'ibc_lifecycle_progress.csv'),
                net_chains=[r for r in rows(root,'ibc_net_chains_daily.csv') if r['universe_id']==net_uid],
                rwa=rows(root,'ibc_rwa_daily.csv'),
                net_assets=[r for r in rows(root,'ibc_net_assets_daily.csv') if r['universe_id']==net_uid],
                net_window_chains=[r for r in rows(root,'ibc_net_windows_chains_latest.csv') if r['universe_id']==net_uid],
                net_window_assets=[{k:r[k] for k in window_asset_fields} for r in rows(root,'ibc_net_windows_assets_latest.csv') if r['universe_id']==net_uid],
                transit=[r for r in rows(root,'ibc_transit_daily.csv') if r['universe_id']==transit_uid],
                transit_assets=[r for r in rows(root,'ibc_transit_assets_daily.csv') if r['universe_id']==transit_uid],
                transit_routes=[r for r in rows(root,'ibc_transit_routes_daily.csv') if r['universe_id']==transit_uid],
                incoming_universe_id=in_uid,
                incoming=[r for r in rows(root,'ecosystem_in_usd_daily.csv') if r['universe_id']==in_uid and int(r['indexed_chains_complete'])==len(cfg['chains'])],
                incoming_chains=[r for r in rows(root,'ibc_in_chains_daily.csv') if r['universe_id']==in_uid],
                incoming_progress=[r for r in rows(root,'criterion2_incoming_progress.csv') if r['universe_id']==in_uid],
                criterion2_progress=[r for r in rows(root, 'criterion2_progress.csv') if r['universe_id'] == uid])


def main(root=ROOT):
    root = Path(root); site = root / 'site'; site.mkdir(exist_ok=True)
    for name in ('ibc_net_windows_chains_latest.csv','ibc_net_windows_assets_latest.csv','ibc_rwa_daily.csv'):
        if (root/name).exists():shutil.copy2(root/name,site/name)
    data = payload(root)
    encoded = json.dumps(data, ensure_ascii=False).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
    template = (root / 'web/index.html').read_text(encoding='utf-8')
    for asset in ('dashboard.css','dashboard.js'):
        version=hashlib.sha256((root/'web'/asset).read_bytes()).hexdigest()[:12]
        template=template.replace('"'+asset+'"','"'+asset+'?v='+version+'"')
    catalog=json.loads((root / 'web/en.json').read_text(encoding='utf-8'))
    labels=json.dumps(catalog,ensure_ascii=False).replace('<','\\u003c')
    for language,name in [('fr','index.html'),('en','index-en.html')]:
        page=english(template,catalog) if language=='en' else template
        page=page.replace('__MONITOR_DATA__',encoded).replace('__MONITOR_I18N__',labels)
        (site/name).write_text(page,encoding='utf-8')
    for name in ('criterion1','criterion2'):
        source=site/(name+'.html')
        if source.exists():
            page=english(source.read_text(encoding='utf-8'),catalog)
            (site/(name+'-en.html')).write_text(page,encoding='utf-8')
    for name in ('dashboard.css', 'dashboard.js'):
        shutil.copy2(root / 'web' / name, site / name)
    print('Public dashboard generated from saved data; measurements unchanged.')


if __name__ == '__main__':
    main()
