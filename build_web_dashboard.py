"""Build the public dashboard from recorded exports, without mutating measurements."""
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from criterion1_daily import load_days, progress
from criterion2_pipeline import load_config

ROOT = Path(__file__).resolve().parent


def rows(root, name):
    path = root / name
    if not path.exists():
        return []
    with path.open(encoding='utf-8', newline='') as stream:
        return list(csv.DictReader(stream))


def payload(root=ROOT):
    root = Path(root)
    cfg, uid = load_config(root / 'config/criterion2_chains.json')
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
                criterion2_progress=[r for r in rows(root, 'criterion2_progress.csv') if r['universe_id'] == uid])


def main(root=ROOT):
    root = Path(root); site = root / 'site'; site.mkdir(exist_ok=True)
    data = payload(root)
    encoded = json.dumps(data, ensure_ascii=False).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
    template = (root / 'web/index.html').read_text(encoding='utf-8')
    (site / 'index.html').write_text(template.replace('__MONITOR_DATA__', encoded), encoding='utf-8')
    for name in ('dashboard.css', 'dashboard.js'):
        shutil.copy2(root / 'web' / name, site / name)
    print('Public dashboard generated from saved data; measurements unchanged.')


if __name__ == '__main__':
    main()
