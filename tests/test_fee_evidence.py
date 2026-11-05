import copy
import unittest
import json
from unittest.mock import patch
import hub_fee_monitor_v41 as monitor
from hub_fee_monitor_v41 import collected_fee_components
from fee_evidence import failed_ante_fee
from criterion1_rpc_bulk import normalized_events

ADDRESS='cosmos1collector'
PAYER='cosmos1payer'


def fixture():
    def event(kind, **attrs):
        return {'type':kind,'attributes':[{'key':k,'value':v} for k,v in attrs.items()]}
    tx={'tx':{'auth_info':{'fee':{'amount':[{'denom':'uatom','amount':'3240'}]}},'body':{'messages':[{'@type':'/cosmos.staking.v1beta1.MsgUndelegate'}]}},
        'tx_response':{'height':'10','code':19,'events':[
          event('tx',acc_seq=PAYER+'/16'),event('coin_spent',spender=PAYER,amount='3240uatom'),
          event('coin_received',receiver=ADDRESS,amount='3240uatom'),event('transfer',sender=PAYER,recipient=ADDRESS,amount='3240uatom'),
          event('message',sender=PAYER)]}}
    proof={'height':10,'source':'https://lcd.test','account':{'@type':'/cosmos.auth.v1beta1.ModuleAccount','name':'feemarket-fee-collector','base_account':{'address':ADDRESS}}}
    return tx,proof


class FeeEvidenceTests(unittest.TestCase):
    def test_failed_message_payment_requires_verified_ante_transfer(self):
        tx,proof=fixture()
        self.assertEqual(collected_fee_components(tx),(None,None))
        self.assertEqual(collected_fee_components(tx,collector=proof),(3240,0))

    def test_collector_preserves_historical_module_proof(self):
        tx,proof=fixture()
        boundary={'header':{'time':'2026-11-04T00:00:00Z'}}
        block={'header':{'time':'2026-11-04T00:00:06Z'}}
        with patch.object(monitor,'block',return_value=(boundary,'rpc')), patch.object(monitor,'block_payload',return_value=(10,block,'rpc',[(tx,'lcd')])), patch.object(monitor,'collector_proof',return_value=proof), patch.object(monitor,'macro',return_value={}), patch.object(monitor,'price_snapshot',return_value={}):
            row=monitor.collect(10,10)
        self.assertEqual(row['collected_fee_atom'],'0.00324')
        self.assertEqual(row['fee_ante_transfer_count'],1)
        self.assertEqual(row['tx_without_fee_event'],0)
        self.assertEqual(json.loads(row['fee_ante_transfer_proofs'])['10'],proof)

    def test_feegrant_payment_uses_granter_not_first_signer(self):
        tx,proof=fixture()
        tx['tx']['auth_info']['fee']['granter']='cosmos1granter'
        for e in tx['tx_response']['events']:
            for a in e['attributes']:
                if a['key'] in ('sender','spender'):a['value']='cosmos1granter'
        self.assertEqual(failed_ante_fee(tx,proof),(3240,0))

    def test_arbitrary_successful_transfer_is_not_a_fee(self):
        tx,proof=fixture();tx['tx_response']['code']=0
        self.assertEqual(failed_ante_fee(tx,proof),(None,None))

    def test_wrong_height_module_recipient_or_amount_remains_unknown(self):
        for mutation in ('height','module','recipient','amount','payer','action','refund'):
            tx,proof=fixture()
            if mutation=='height':proof['height']=11
            if mutation=='module':proof['account']['name']='gov'
            if mutation=='recipient':proof['account']['base_account']['address']='cosmos1other'
            if mutation=='amount':tx['tx']['auth_info']['fee']['amount'][0]['amount']='9999'
            if mutation=='payer':tx['tx']['auth_info']['fee']['payer']='cosmos1other'
            if mutation=='action':tx['tx_response']['events'][-1]['attributes'].append({'key':'action','value':'/cosmos.bank.v1beta1.MsgSend'})
            if mutation=='refund':tx['tx_response']['events'].append({'type':'coin_spent','attributes':[{'key':'spender','value':ADDRESS},{'key':'amount','value':'1uatom'}]})
            with self.subTest(mutation=mutation):self.assertEqual(failed_ante_fee(tx,proof),(None,None))

    def test_missing_or_duplicate_bank_evidence_stays_unknown(self):
        tx,proof=fixture();tx['tx_response']['events'].pop(1)
        self.assertEqual(failed_ante_fee(tx,proof),(None,None))
        tx,proof=fixture();tx['tx_response']['events'].append(copy.deepcopy(tx['tx_response']['events'][3]))
        self.assertEqual(failed_ante_fee(tx,proof),(None,None))

    def test_base64_bank_event_attributes_are_normalized(self):
        events=[{'type':'transfer','attributes':[{'key':'cmVjaXBpZW50','value':'Y29zbW9zMWNvbGxlY3Rvcg=='}]}]
        self.assertEqual(normalized_events(events)[0]['attributes'][0],{'key':'recipient','value':ADDRESS})


if __name__=='__main__':unittest.main()
