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
    for name in ('index','criterion1','criterion2','community-pool','criterion1-services','ibc-large-movements'):
        document=re.sub(rf'href="{name}\.html(?=["#])', f'href="{name}-en.html', document)
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
    window_asset_fields='universe_id date window_days period_kind chain denom symbol decimals coingecko_id metadata_status confirmed_in_amount confirmed_out_amount net_amount observed_net_amount unresolved_packets excluded_packets'.split()
    # Never mix historical universes or substitute partially collected days.
    c1 = [r for r in rows(root, 'hub_criterion1_daily_complete.csv')
          if r['coverage_status'] == 'complete_utc_day_blocks']
    c2 = [r for r in rows(root, 'ecosystem_out_usd_daily.csv')
          if r['universe_id'] == uid and int(r['indexed_chains_complete']) == len(cfg['chains'])]
    return dict(generated_at=datetime.now(timezone.utc).isoformat(), universe_id=uid,
                members=[c['name'] for c in cfg['chains']],
                criterion1=sorted(c1, key=lambda r:r['date']),
                eureka=rows(root, 'hub_criterion1_eureka_daily.csv'),
                emission=[{key: value for key, value in row.items() if key != 'criterion3_status'}
                          for row in rows(root, 'hub_criterion1_emission_daily.csv')],
                criterion1_progress=[progress(s) for s in load_days(root / 'data/criterion1_days')],
                criterion2=sorted(c2, key=lambda r:r['date']),
                chains=[r for r in rows(root, 'ibc_out_chains_daily.csv') if r['universe_id'] == uid],
                lifecycle=[r for r in rows(root,'ibc_lifecycle_daily.csv') if r['universe_id'] in (uid,in_uid)],
                lifecycle_progress=rows(root,'ibc_lifecycle_progress.csv'),
                net_chains=[r for r in rows(root,'ibc_net_chains_daily.csv') if r['universe_id']==net_uid],
                rwa=rows(root,'ibc_rwa_daily.csv'),
                rwa_balances=rows(root,'ibc_rwa_balances_daily.csv'),
                net_assets=[r for r in rows(root,'ibc_net_assets_daily.csv') if r['universe_id']==net_uid],
                net_window_chains=[r for r in rows(root,'ibc_net_windows_chains_latest.csv') if r['universe_id']==net_uid],
                net_window_assets=[{k:r.get(k, 'rolling') if k == 'period_kind' else r[k] for k in window_asset_fields} for r in rows(root,'ibc_net_windows_assets_latest.csv') if r['universe_id']==net_uid],
                transit=[r for r in rows(root,'ibc_transit_daily.csv') if r['universe_id']==transit_uid],
                transit_assets=[r for r in rows(root,'ibc_transit_assets_daily.csv') if r['universe_id']==transit_uid],
                transit_routes=[r for r in rows(root,'ibc_transit_routes_daily.csv') if r['universe_id']==transit_uid],
                incoming_universe_id=in_uid,
                incoming=[r for r in rows(root,'ecosystem_in_usd_daily.csv') if r['universe_id']==in_uid and int(r['indexed_chains_complete'])==len(cfg['chains'])],
                incoming_chains=[r for r in rows(root,'ibc_in_chains_daily.csv') if r['universe_id']==in_uid],
                incoming_progress=[r for r in rows(root,'criterion2_incoming_progress.csv') if r['universe_id']==in_uid],
                criterion2_progress=[r for r in rows(root, 'criterion2_progress.csv') if r['universe_id'] == uid])


def build_services_page(root, site, data, catalog):
    """Render the services page using the same published observations."""
    day = data['criterion1'][-1] if data['criterion1'] else {}
    date = day.get('date')
    fees = day.get('collected_fee_atom') if day.get('fee_collection_status') == 'complete' else None
    eureka = next((row for row in data.get('eureka', []) if row['date'] == date
                   and row['start_height'] == day.get('start_height')
                   and row['end_height'] == day.get('end_height')), {})
    eureka_fees_verified = eureka.get('status') == 'indexed_fee_verified'
    template = (root / 'web/criterion1-services.html').read_text(encoding='utf-8')

    for language, page_name in (('fr', 'criterion1-services.html'), ('en', 'criterion1-services-en.html')):
        page = english(template, catalog) if language == 'en' else template
        unavailable = 'Indisponible' if language == 'fr' else 'Unavailable'

        def amount(value, decimals, unit):
            if value is None or value == '':
                return unavailable
            formatted = format(Decimal(value), f',.{decimals}f')
            if language == 'fr':
                formatted = formatted.translate(str.maketrans({',': '\u202f', '.': ','}))
            return f'{formatted} <small>{unit}</small>' if unit else formatted

        language_links = []
        for code, suffix, label in (('fr', '', 'Français'), ('en', '-en', 'English')):
            current = ' aria-current="page"' if code == language else ''
            language_links.append(f'<a href="criterion1-services{suffix}.html" lang="{code}" hreflang="{code}"{current}>{label}</a>')
        values = {
            '__LANGUAGE_SWITCH__': '<div class="language-switch" aria-label="Language">' + ''.join(language_links) + '</div>',
            '__DATE__': html.escape(date or unavailable),
            '__EUREKA_TXS__': amount(eureka.get('indexed_transactions'), 0, ''),
            '__EUREKA_FEES__': amount(eureka.get('collected_fee_atom') if eureka_fees_verified else None, 6, 'ATOM'),
            '__EUREKA_SHARE__': amount(eureka.get('share_of_hub_fees_pct') if eureka_fees_verified and fees else None, 4, '%'),
        }
        for placeholder, value in values.items():
            page = page.replace(placeholder, value)
        (site / page_name).write_text(page, encoding='utf-8')


def main(root=ROOT):
    root = Path(root); site = root / 'site'; site.mkdir(exist_ok=True)
    for name in ('ibc_net_windows_chains_latest.csv','ibc_net_windows_assets_latest.csv','ibc_rwa_daily.csv','ibc_rwa_balances_daily.csv','hub_criterion1_emission_daily.csv','hub_criterion1_eureka_daily.csv','hub_community_pool_deposits.csv'):
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
    from community_pool_page import build as build_community_pool
    build_community_pool(root)
    build_services_page(root, site, data, catalog)
    from ibc_large_movements import build as build_large_movements
    build_large_movements(root, site, data, catalog, english)
    print('Public dashboard generated from saved data; measurements unchanged.')


if __name__ == '__main__':
    main()
