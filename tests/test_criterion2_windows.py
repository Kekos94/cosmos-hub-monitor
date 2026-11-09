import copy
import unittest
from datetime import date,timedelta
from decimal import Decimal,localcontext
from pathlib import Path
import tempfile

from criterion2_windows import calculate
from criterion2_net import calculate as daily
from test_criterion2_net import inputs,add,OUT,IN,UID
from test_criterion2 import price
from criterion2_pipeline import atomic
from criterion2_core import decimal_amount

END='2026-11-05';DAYS=[str(date.fromisoformat(END)-timedelta(days=i)) for i in reversed(range(7))]


class WindowTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        self.cfg,self.outs,self.ins,self.js=inputs()
        atomic(self.root/END/'cosmos.json',price('cosmos','2'));atomic(self.root/END/'injective-protocol.json',price('injective-protocol','2'))
    def tearDown(self):self.tmp.cleanup()
    def template(self,denom='uatom',incoming='3000000',outgoing='1000000',unknown=False,extra=False):
        add(self.ins[0],self.js,101,incoming,denom);add(self.outs[0],self.js,1,outgoing,denom,status='unknown' if unknown else 'success')
        if extra:
            add(self.ins[0],self.js,102,'3000000000000000000','inj');add(self.outs[0],self.js,2,'1000000000000000000','inj')
        return daily(self.outs,self.ins,self.js,self.cfg,OUT,IN,UID,self.root)
    def repeat(self,dates,template=None):
        chain,asset=template or self.template();cc=[];aa=[]
        for d in dates:
            cc.extend(dict(r,date=d) for r in chain);aa.extend(dict(r,date=d,reference_time_utc=d+'T00:00:00+00:00' if r['reference_time_utc'] else '') for r in asset)
        return cc,aa
    def result(self,cc,aa,end=END):return calculate(cc,aa,self.cfg,UID,end_date=end)
    def asset(self,rows,length=7,denom='uatom'):return next(r for r in rows if r['window_days']==length and r['chain']=='cosmoshub' and r['denom']==denom)
    def chain(self,rows,length=7):return next(r for r in rows if r['window_days']==length and r['chain']=='cosmoshub')
    def test_one_day_is_observed_not_a_seven_day_balance(self):
        cc,aa=self.repeat([END]);chains,assets=self.result(cc,aa);r=self.asset(assets);c=self.chain(chains)
        self.assertEqual(r['covered_days'],1);self.assertEqual(r['missing_days'],6);self.assertEqual(r['observed_net_amount'],'2');self.assertEqual(r['net_amount'],'')
        self.assertEqual(c['valued_net_usd'],'4');self.assertEqual(c['total_net_usd'],'')
        self.assertEqual(r['start_date'],DAYS[0]);self.assertIn('2026-11-04',r['missing_dates'])
        self.assertEqual(self.asset(assets,30)['missing_days'],29);self.assertEqual(self.asset(assets,90)['missing_days'],89)
    def test_seven_complete_days_sum_exactly(self):
        cc,aa=self.repeat(DAYS);chains,assets=self.result(cc,aa);r=self.asset(assets);c=self.chain(chains)
        self.assertEqual(r['net_amount'],'14');self.assertEqual(r['confirmed_in_amount'],'21');self.assertEqual(r['confirmed_out_amount'],'7');self.assertEqual(c['total_net_usd'],'28')
        self.assertEqual(r['coverage_status'],'complete_window');self.assertEqual(r['missing_dates'],'')
    def test_empty_history_is_not_measured_zero(self):
        chains,assets=self.result([],[]);self.assertEqual(assets,[])
        self.assertTrue(all(r['covered_days']==0 and r['valued_net_usd']=='' and r['total_net_usd']=='' for r in chains))
    def test_internal_gap_and_missing_latest_days_are_explicit(self):
        selected=[d for d in DAYS if d!='2026-11-01'];cc,aa=self.repeat(selected);chains,assets=self.result(cc,aa)
        self.assertEqual(self.asset(assets)['observed_net_amount'],'12');self.assertEqual(self.chain(chains)['total_net_usd'],'');self.assertEqual(self.chain(chains)['missing_dates'],'2026-11-01')
        cc,aa=self.repeat(['2026-11-03']);chains,_=self.result(cc,aa)
        self.assertEqual(self.chain(chains)['end_date'],END);self.assertIn('2026-11-05',self.chain(chains)['missing_dates'])
    def test_real_zero_on_absent_asset_day_requires_covered_cohorts(self):
        seed=self.template();cc,aa=self.repeat(DAYS,seed)
        # One day is known to contain zero packets, on every chain, not missing.
        empty_cfg,oo,ii,jj=inputs();empty,_=daily(oo,ii,jj,empty_cfg,OUT,IN,UID,self.root)
        cc=[r for r in cc if r['date']!=DAYS[0]]+[dict(r,date=DAYS[0]) for r in empty]
        aa=[r for r in aa if r['date']!=DAYS[0]]
        chains,assets=self.result(cc,aa);self.assertEqual(self.asset(assets)['net_amount'],'12');self.assertEqual(self.chain(chains)['covered_days'],7)
    def test_daily_prices_are_summed_not_latest_price_applied_to_history(self):
        cc,aa=self.repeat(DAYS)
        for i,d in enumerate(DAYS):
            r=next(r for r in aa if r['date']==d);rate=Decimal(i+2)
            r.update(reference_price_usd=str(rate),confirmed_in_usd=str(3*rate),confirmed_out_usd=str(rate),net_usd=str(2*rate))
        chains,assets=self.result(cc,aa);self.assertEqual(Decimal(self.asset(assets)['net_usd']),Decimal(70));self.assertEqual(Decimal(self.chain(chains)['total_net_usd']),Decimal(70))
    def test_priced_subset_is_the_same_for_both_sides_over_entire_window(self):
        cc,aa=self.repeat(DAYS,self.template(extra=True))
        for r in aa:
            if r['denom']=='inj' and r['date']==DAYS[0]:r.update(confirmed_in_usd='',confirmed_out_usd='',net_usd='',valuation_status='missing_historical_price')
        chains,assets=self.result(cc,aa);c=self.chain(chains)
        self.assertEqual(c['valued_assets'],1);self.assertEqual(c['valued_in_usd'],'42');self.assertEqual(c['valued_out_usd'],'14');self.assertEqual(c['valued_net_usd'],'28');self.assertEqual(c['total_net_usd'],'')
        self.assertEqual(self.asset(assets,denom='inj')['net_amount'],'14');self.assertEqual(self.asset(assets,denom='inj')['net_usd'],'')
    def test_unresolved_asset_day_prevents_period_net(self):
        cc,aa=self.repeat(DAYS,self.template(unknown=True));chains,assets=self.result(cc,aa)
        self.assertEqual(self.asset(assets)['observed_net_amount'],'');self.assertEqual(self.asset(assets)['net_amount'],'');self.assertEqual(self.chain(chains)['total_net_usd'],'')
    def test_18_decimals_and_large_cancellation_stay_exact(self):
        big=10**70;cc,aa=self.repeat(DAYS,self.template('inj',str(big+1),str(big)))
        _,assets=self.result(cc,aa);r=self.asset(assets,denom='inj');self.assertEqual(r['net_raw'],'7');self.assertEqual(Decimal(r['net_amount']),Decimal('7E-18'))
    def test_mixed_evidence_cutoffs_do_not_create_a_false_complete_window(self):
        cc,aa=self.repeat(DAYS)
        for r in cc+aa:
            if r['date']==DAYS[-1]:r['evidence_through_utc']='2026-11-07T00:00:00Z'
        chains,assets=self.result(cc,aa);c=self.chain(chains)
        self.assertEqual(c['covered_days'],7);self.assertEqual(c['evidence_status'],'mixed_cutoffs');self.assertEqual(c['valued_net_usd'],'');self.assertEqual(self.asset(assets)['confirmed_in_amount'],'')
    def test_one_chain_or_direction_incomplete_excludes_that_day_for_every_chain(self):
        cc,aa=self.repeat(DAYS)
        for r in cc:
            if r['date']==DAYS[0] and r['chain']=='noble':r['coverage_status']='incomplete_cohorts'
        chains,assets=self.result(cc,aa);self.assertEqual(self.chain(chains)['covered_days'],6);self.assertEqual(self.asset(assets)['observed_net_amount'],'12');self.assertEqual(self.asset(assets)['net_amount'],'')
    def test_duplicate_or_missing_asset_rows_fail_closed(self):
        cc,aa=self.repeat(DAYS)
        with self.assertRaisesRegex(ValueError,'Duplicate'):self.result(cc+cc[:1],aa)
        with self.assertRaisesRegex(ValueError,'Duplicate'):self.result(cc,aa+aa[:1])
        with self.assertRaisesRegex(ValueError,'missing'):self.result(cc,aa[1:])
    def test_different_units_across_days_are_not_summed_as_one_display_amount(self):
        cc,aa=self.repeat(DAYS)
        r=aa[0];r.update(decimals=18,confirmed_in_amount='3E-12',confirmed_out_amount='1E-12',known_net_amount='2E-12',net_amount='2E-12',confirmed_in_usd='6E-12',confirmed_out_usd='2E-12',net_usd='4E-12')
        chains,assets=self.result(cc,aa);r=self.asset(assets);self.assertEqual(r['metadata_status'],'conflict');self.assertEqual(r['net_amount'],'');self.assertEqual(self.chain(chains)['total_net_usd'],'')
    def test_old_universes_and_out_of_window_days_are_not_counted(self):
        cc,aa=self.repeat(DAYS)
        cc.append(dict(cc[0],universe_id='old',date='1999-01-01'));aa.append(dict(aa[0],universe_id='old',date='1999-01-01'))
        chains,assets=self.result(cc,aa);self.assertEqual(self.asset(assets)['net_amount'],'14')
    def test_full_30_and_90_day_calendar_windows(self):
        dates=[str(date.fromisoformat(END)-timedelta(days=i)) for i in reversed(range(90))]
        cc,aa=self.repeat(dates);chains,assets=self.result(cc,aa)
        for length in [7,30,90]:
            r=self.asset(assets,length);self.assertEqual(r['covered_days'],length);self.assertEqual(Decimal(r['net_amount']),Decimal(2*length));self.assertEqual(Decimal(self.chain(chains,length)['total_net_usd']),Decimal(4*length))
    def test_calendar_totals_split_week_month_and_year_boundaries(self):
        cc, aa = self.repeat(['2026-10-31', '2026-11-01', '2026-11-02', END])
        chains, assets = calculate(cc, aa, self.cfg, UID, end_date=END, lengths=(), calendar=True)
        periods = {(r['period_kind'], r['end_date']): r for r in chains if r['chain'] == 'cosmoshub'}
        self.assertEqual(periods['week', '2026-11-01']['start_date'], '2026-10-26')
        self.assertEqual(periods['week', END]['start_date'], '2026-11-02')
        self.assertEqual(periods['month', '2026-10-31']['covered_days'], 1)
        self.assertEqual(periods['month', END]['start_date'], '2026-11-01')
        self.assertEqual(periods['month', END]['covered_days'], 3)
        self.assertEqual(periods['year', END]['start_date'], '2026-01-01')
        self.assertEqual(periods['year', END]['covered_days'], 4)
        self.assertEqual(periods['year', END]['total_net_usd'], '')
        year_atom = next(r for r in assets if r['period_kind'] == 'year' and r['chain'] == 'cosmoshub')
        self.assertEqual(year_atom['observed_net_amount'], '8')
        self.assertEqual(year_atom['net_amount'], '')

    def test_wrong_daily_price_reference_is_rejected(self):
        cc,aa=self.repeat(DAYS);aa[0]['reference_time_utc']='2026-01-01T00:00:00Z'
        with self.assertRaisesRegex(ValueError,'reference date'):self.result(cc,aa)
    def test_export_preserves_daily_sources_and_anchors_to_closed_day(self):
        import shutil
        from criterion2_windows import exports
        from criterion2_export import write_csv
        from criterion2_net import CHAIN,ASSET,config
        from criterion2_pipeline import ROOT
        cc,aa=self.repeat([END]);(self.root/'config').mkdir();shutil.copy2(ROOT/'config/criterion2_chains.json',self.root/'config/criterion2_chains.json')
        cfg,out_uid,in_uid,uid=config(self.root/'config/criterion2_chains.json')
        for r in cc+aa:r.update(universe_id=uid,outgoing_universe_id=out_uid,incoming_universe_id=in_uid)
        write_csv(self.root/'ibc_net_chains_daily.csv',cc,CHAIN);write_csv(self.root/'ibc_net_assets_daily.csv',aa,ASSET)
        before={p.name:p.read_bytes() for p in self.root.glob('*daily.csv')}
        chains,assets=exports(self.root,end_date=END)
        self.assertEqual(self.chain(chains)['covered_days'],1)
        self.assertEqual(before,{p.name:p.read_bytes() for p in self.root.glob('*daily.csv')})
        self.assertTrue((self.root/'ibc_net_windows_assets_latest.csv').exists())
    def test_same_symbol_does_not_merge_two_local_denominations(self):
        seed=self.template(extra=True)
        for r in seed[1]:r['symbol']='TOKEN'
        cc,aa=self.repeat(DAYS,seed);_,assets=self.result(cc,aa)
        self.assertEqual(len([r for r in assets if r['window_days']==7 and r['chain']=='cosmoshub']),2)


if __name__=='__main__':unittest.main()
