import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import build_web_dashboard as dashboard
from criterion2_pipeline import load_config


class DashboardTests(unittest.TestCase):
    def setup_root(self, root):
        (root / 'config').mkdir()
        shutil.copy2(dashboard.ROOT / 'config/criterion2_chains.json', root / 'config/criterion2_chains.json')
        shutil.copytree(dashboard.ROOT / 'web', root / 'web')
        return load_config(root / 'config/criterion2_chains.json')[1]

    def write(self, root, name, records):
        with (root / name).open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(records[0]))
            writer.writeheader(); writer.writerows(records)

    def test_partial_days_and_old_universes_never_become_summary_cards(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); uid=self.setup_root(root)
            self.write(root,'hub_criterion1_daily_complete.csv',[
                dict(date='2026-11-04',coverage_status='complete_utc_day_blocks',collected_fee_atom=''),
                dict(date='2026-11-05',coverage_status='incomplete',collected_fee_atom='500')])
            self.write(root,'ecosystem_out_usd_daily.csv',[
                dict(date='2026-11-04',universe_id='old',indexed_chains_complete='4'),
                dict(date='2026-11-05',universe_id=uid,indexed_chains_complete='3'),
                dict(date='2026-11-06',universe_id=uid,indexed_chains_complete='4')])
            d=dashboard.payload(root)
            self.assertEqual([r['date'] for r in d['criterion1']],['2026-11-04'])
            self.assertEqual(d['criterion1'][0]['collected_fee_atom'],'')
            self.assertEqual([r['date'] for r in d['criterion2']],['2026-11-06'])

    def test_embedded_data_is_safe_and_source_csv_unchanged(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); self.setup_root(root)
            self.write(root,'hub_criterion1_daily_complete.csv',[
                dict(date='2026-11-04',coverage_status='complete_utc_day_blocks',note='</script><script>alert(1)</script>')])
            before=(root/'hub_criterion1_daily_complete.csv').read_bytes()
            dashboard.main(root)
            document=(root/'site/index.html').read_text()
            encoded=document.split('id="monitor-data">')[1].split('</script>')[0]
            self.assertNotIn('</script>',encoded)
            self.assertEqual(json.loads(encoded)['criterion1'][0]['note'],'</script><script>alert(1)</script>')
            self.assertEqual((root/'hub_criterion1_daily_complete.csv').read_bytes(),before)
            self.assertEqual((root/'site/dashboard.js').read_bytes(),(root/'web/dashboard.js').read_bytes())

    def test_empty_history_remains_empty(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); self.setup_root(root)
            d=dashboard.payload(root)
            self.assertEqual(d['criterion1'],[]); self.assertEqual(d['criterion2'],[])
            self.assertEqual(d['incoming'],[])
            self.assertEqual(len(d['members']),4)

    def test_english_and_french_share_exact_data_and_correct_language_links(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.setup_root(root)
            dashboard.main(root)
            fr=(root/'site/index.html').read_text();en=(root/'site/index-en.html').read_text()
            def data(s):return json.loads(s.split('id="monitor-data">')[1].split('</script>')[0])
            self.assertEqual(data(fr),data(en))
            self.assertIn('<html lang="en">',en)
            self.assertIn('href="index.html" lang="fr" hreflang="fr"',en)
            self.assertIn('href="index-en.html" lang="en" aria-current="page"',en)
            self.assertIn('href="criterion1-en.html"',en)
            self.assertIn('href="criterion2-en.html"',en)
            self.assertIn('1.3 — Transaction fees and issuance coverage',en)
            self.assertIn('<h1>Activité du Hub</h1>', fr)
            self.assertIn('1.2 — Flux, soldes et transits IBC', fr)
            self.assertIn('1.2 — IBC flows, balances and transit', en)
            self.assertIn('1.1 — Transactions and IBC activity', en)
            self.assertIn('Hub transaction fees', en)
            self.assertIn('Inflows and outflows for the selected asset', en)
            self.assertIn('id="net-flow-bars"', fr)
            self.assertIn('ATOM issuance and fee coverage', en)
            self.assertNotIn('id="eureka"', en)
            services = (root/'site/criterion1-services-en.html').read_text()
            self.assertIn('Live services and verified payments', services)
            self.assertIn('href="index-en.html#activity"', services)
            self.assertIn('Network fees from Eureka transactions', services)
            self.assertIn('href="hub_criterion1_eureka_daily.csv"', services)
            self.assertIn('href="hub_criterion1_emission_daily.csv"', en)
            self.assertIn('365-day monetary inflation', en)
            self.assertNotIn('critère 3', fr.casefold())
            self.assertNotIn('criterion 3', en.casefold())
            self.assertIn('href="criterion2-en.html#transit"', en)
            self.assertIn('href="community-pool-en.html"', en)
            self.assertIn('href="criterion1-services.html"', fr)
            self.assertIn('href="criterion1-services-en.html"', en)
            self.assertIn('href="criterion1-services-en.html#services"', (root/'site/community-pool-en.html').read_text())

    def test_translated_detail_pages_keep_data_and_locale_navigation(self):
        catalog=json.loads((dashboard.ROOT/'web/en.json').read_text())
        source='<html lang="fr"><a href="index.html">Retour au dashboard</a><h1>1.2 — Centralité des flux IBC</h1><table><tr><th>Paquets non valorisés</th><td>376</td></tr></table></html>'
        result=dashboard.english(source,catalog)
        self.assertIn('href="index-en.html"',result)
        self.assertIn('1.2 — IBC flow centrality', result)
        self.assertIn('Unvalued packets',result)
        self.assertIn('<td>376</td>',result)
        self.assertIn('6,457,369.77',dashboard.english('<td>6 457 369,77</td>',catalog))

    def test_services_show_only_matching_eureka_fees_and_link_to_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.setup_root(root)
            day = dict(date='2026-11-10', fee_collection_status='complete',
                       collected_fee_atom='5', start_height='1', end_height='10')
            eureka = dict(date=day['date'], status='indexed_fee_verified',
                          collected_fee_atom='1', share_of_hub_fees_pct='20',
                          start_height='1', end_height='9')
            data = dict(criterion1=[day], eureka=[eureka])
            catalog = json.loads((root / 'web/en.json').read_text())
            page_path = root / 'criterion1-services-en.html'
            dashboard.build_services_page(root, root, data, catalog)
            self.assertNotIn('1.000000 <small>ATOM</small>', page_path.read_text())
            self.assertNotIn('class="eureka-fee-bar"', page_path.read_text())

            eureka['end_height'] = '10'
            dashboard.build_services_page(root, root, data, catalog)
            page = page_path.read_text()
            self.assertIn('1.000000 <small>ATOM</small>', page)
            self.assertIn('Daily Eureka network fees (ATOM)', page)
            self.assertIn('style="width:100.0000%"', page)
            self.assertNotIn('5.000000 <small>ATOM</small>', page)
            self.assertIn('href="index-en.html#coverage-cards"', page)
            self.assertIn('Service revenue / issuance</h2>', page)
            self.assertIn('<span>Unavailable</span> · <span>Service payment unverified</span>', page)


if __name__=='__main__': unittest.main()
