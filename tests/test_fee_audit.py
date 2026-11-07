import base64
import copy
import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from criterion1_data import enrich
from criterion1_daily import DAY_DIR, daily_summary
from criterion1_fee_audit import apply_fee_audit, load_fee_audit, state_hash, compress_audit, audit_pending
from test_daily import plan, chunk, TIMES
from test_fee_evidence import fixture as failed_fixture
from test_rpc_bulk import field, tx_raw
from cosmos_tx_decode import single
from fee_evidence import verified_no_fee


def fixture():
    state = plan()
    original = chunk(11, 14)
    original.update(declared_ibc_fee_atom='6', fee_collection_status='incomplete', tx_without_fee_event='1',
                    collected_fee_atom='', collected_ibc_fee_atom='', collected_tip_atom='', collected_ibc_tip_atom='')
    state['chunks'] = [enrich(original)]
    record = dict(start_height=11, end_height=14, blocks=[], transactions=[])
    for height in range(11,15):
        hashes=[]
        for index in range(2):
            tx_hash=hashlib.sha256(f'{height}/{index}'.encode()).hexdigest().upper()
            hashes.append(tx_hash)
            tx,proof=failed_fixture()
            tx['tx']['auth_info']['fee']['amount'][0]['amount']='1500000'
            tx['tx']['body']['messages'][0]['@type']='/ibc.core.client.v1.MsgUpdateClient' if index==0 else '/cosmos.bank.v1beta1.MsgSend'
            tx['tx_response'].update(height=str(height),txhash=tx_hash)
            proof['height']=height
            if height==11 and index==0:
                for e in tx['tx_response']['events']:
                    for a in e['attributes']:
                        if a['key']=='amount':a['value']='1500000uatom'
            else:
                tx['tx_response'].update(code=0,events=[{'type':'fee_pay','attributes':[{'key':'fee','value':'1500000uatom'}]}])
                proof=None
            record['transactions'].append(dict(height=height,index=index,hash=tx_hash,tx=tx,collector=proof))
        record['blocks'].append(dict(height=height,time=TIMES[height],tx_hashes=hashes))
    audit=dict(schema_version=1,date=state['date'],state_sha256=state_hash(state),complete=True,chunks=[record])
    return state,audit


class FeeAuditTests(unittest.TestCase):
    def test_evidence_recovers_ratios_without_mutating_source_or_repricing(self):
        state,audit=fixture();before=copy.deepcopy(state)
        self.assertEqual(daily_summary(state)['collected_fee_atom'],'')
        row=daily_summary(state,audit=audit)
        self.assertEqual(row['collected_fee_atom'],'12')
        self.assertEqual(row['collected_ibc_fee_atom'],'6')
        self.assertEqual(row['tx_without_fee_event'],'0')
        self.assertEqual(row['fee_evidence_recovered'],1)
        self.assertNotEqual(row['fee_to_estimated_emission_pct'],'')
        self.assertEqual(row['effective_burn_atom'],'')
        self.assertEqual(row['daily_fee_usd'],'')
        self.assertEqual(state,before)

    def test_partial_audit_never_unlocks_daily_ratios(self):
        state,audit=fixture();audit['complete']=False
        with self.assertRaises(ValueError):daily_summary(state,audit=audit)
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/(state['date']+'.json');path.write_text(json.dumps(audit))
            self.assertIsNone(load_fee_audit(state,tmp))

    def test_final_evidence_compression_preserves_exact_bytes(self):
        state,audit=fixture()
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/(state['date']+'.json')
            source=json.dumps(audit).encode()
            path.write_bytes(source)
            target=compress_audit(path)
            self.assertFalse(path.exists())
            self.assertEqual(gzip.decompress(target.read_bytes()),source)
            loaded=load_fee_audit(state,tmp)
            self.assertEqual(daily_summary(state,audit=loaded)['collected_fee_atom'],'12')

    def test_rejects_source_changes_missing_blocks_or_wrong_tx_counts(self):
        for change in ('source','blocks','txs','declared'):
            state,audit=fixture()
            if change=='source':state['chunks'][0]['atom_price_usd']='99'
            if change=='blocks':audit['chunks'][0]['blocks'].pop()
            if change=='txs':audit['chunks'][0]['transactions'].pop()
            if change=='declared':audit['chunks'][0]['transactions'][-1]['tx']['tx']['auth_info']['fee']['amount'][0]['amount']='999'
            with self.subTest(change=change),self.assertRaises(ValueError):apply_fee_audit(state,audit)

    def test_failed_proof_leaves_fees_unknown_instead_of_using_declaration(self):
        state,audit=fixture()
        audit['chunks'][0]['transactions'][0]['collector']['account']['name']='gov'
        row=daily_summary(state,audit=audit)
        self.assertEqual(row['collected_fee_atom'],'')
        self.assertEqual(row['fee_to_estimated_emission_pct'],'')
        self.assertEqual(row['tx_without_fee_event'],'1')


    def test_expired_signed_transaction_verifies_zero_without_fee_transfer(self):
        state, audit = fixture()
        receipt = audit['chunks'][0]['transactions'][0]
        # TxRaw.body_bytes contains TxBody.timeout_height (field 3).
        signed = b'\x0a\x02\x18\x0a'
        tx_hash = hashlib.sha256(signed).hexdigest().upper()
        receipt['hash'] = tx_hash
        audit['chunks'][0]['blocks'][0]['tx_hashes'][0] = tx_hash
        receipt['collector'] = None
        response = receipt['tx']['tx_response']
        response.update(code=30, codespace='sdk', txhash=tx_hash, events=[],
                        rpc_event_count=0, signed_tx=base64.b64encode(signed).decode(),
                        raw_log='block height: 11, timeout height: 10: tx timeout height')
        row = daily_summary(state, audit=audit)
        self.assertEqual(row['collected_fee_atom'], '10.5')
        self.assertEqual(row['fee_evidence_recovered'], 1)
        self.assertEqual(row['tx_without_fee_event'], '0')
        for field, value in [('code', 5), ('codespace', 'ibc'), ('rpc_event_count', 1),
                             ('signed_tx', 'CgIYCw=='), ('raw_log', 'message failure')]:
            with self.subTest(field=field), patch.dict(response, {field: value}):
                self.assertEqual(daily_summary(state, audit=audit)['collected_fee_atom'], '')

    def test_ante_escrow_and_sequence_rejections_need_specific_proof(self):
        raw = tx_raw(amount='1915')
        cases = [
            (5, raw, 'error escrowing funds: spendable balance 3uatom is smaller than 1915uatom: insufficient funds'),
            (32, field(2, field(1, b'\x18\x25') + single(raw, 2)),
             'account sequence mismatch, expected 34, got 37: incorrect account sequence'),
        ]
        for code, signed, log in cases:
            response = dict(code=code, codespace='sdk', rpc_event_count=0, events=[],
                            signed_tx=base64.b64encode(signed).decode(), raw_log=log,
                            txhash=hashlib.sha256(signed).hexdigest().upper())
            with self.subTest(code=code):
                self.assertTrue(verified_no_fee(response))
                self.assertFalse(verified_no_fee(dict(response, raw_log='failed to execute message: ' + log)))
                self.assertFalse(verified_no_fee(dict(response, raw_log=log.replace('1915', '2000').replace('got 37', 'got 38'))))

    def test_complete_day_with_all_fee_evidence_gets_one_audit_without_rpc(self):
        state = plan()
        state['chunks'] = [enrich(chunk(11, 14))]
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / 'days'
            directory.mkdir()
            audit_dir = Path(tmp) / 'audits'
            (directory / (state['date'] + '.json')).write_text(json.dumps(state))
            with patch('criterion1_fee_audit.payloads') as fetch:
                self.assertFalse(audit_pending(budget_seconds=30, directory=directory, audit_directory=audit_dir))
                audit = load_fee_audit(state, audit_dir)
                self.assertTrue(audit['complete'])
                self.assertEqual(audit['chunks'], [])
                self.assertEqual(daily_summary(state, audit=audit)['fee_audit_status'], 'verified')
                with patch('criterion1_fee_audit.audit_day') as audit_again:
                    self.assertFalse(audit_pending(budget_seconds=30, directory=directory, audit_directory=audit_dir))
                    audit_again.assert_not_called()
                fetch.assert_not_called()

    def test_pending_audit_resumes_only_complete_days_with_missing_evidence(self):
        state, audit = fixture()
        partial = dict(state, date='2026-11-06', chunks=[])
        complete = dict(state, date='2026-11-07')
        with tempfile.TemporaryDirectory() as tmp:
            audit_dir = Path(tmp)
            (audit_dir / (state['date'] + '.json.gz')).write_bytes(gzip.compress(json.dumps(audit).encode()))
            (audit_dir / (complete['date'] + '.rejections.json')).write_text(json.dumps(dict(audit, date=complete['date'], state_sha256=state_hash(complete), complete=False)))
            with patch('criterion1_daily.load_days', return_value=[state, partial, complete]), \
                 patch('criterion1_daily.progress', side_effect=[{'status': 'complete'}, {'status': 'incomplete'}, {'status': 'complete'}]), \
                 patch('criterion1_fee_audit.audit_day') as run:
                self.assertFalse(audit_pending(budget_seconds=30, audit_directory=audit_dir))
            run.assert_called_once_with(
                complete['date'], directory=DAY_DIR,
                audit_directory=audit_dir,
            )


if __name__=='__main__':unittest.main()
