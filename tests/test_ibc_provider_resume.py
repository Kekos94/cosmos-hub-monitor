import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import criterion2_pipeline as pipeline
from test_criterion2 import FakeClient, plan, mapping

class ProviderResumeTests(unittest.TestCase):
    def test_retired_incomplete_snapshot_is_archived_before_replanning(self):
        old = plan(FakeClient('injective'))
        chain = dict(name='injective', chain_id='injective-1', rpc=['https://new.test'])
        new = dict(old, source=chain['rpc'][0])
        cfg = dict(chains=[chain], scope=old['scope'])
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            folder = root / old['universe_id'] / 'injective' / old['date']
            pipeline.write_once(folder / 'plan.json', old)
            with patch.object(pipeline, 'registry_assets', return_value=dict(mapping=mapping())), \
                 patch.object(pipeline, 'build_plan', return_value=new) as build, \
                 patch.object(pipeline, 'Client') as client, \
                 patch.object(pipeline, 'collect_pages', side_effect=TimeoutError):
                client.return_value.search.return_value = dict(total_count='0', txs=[])
                pipeline.run_collection(cfg, old['universe_id'], root=root, budget=10)
            build.assert_called_once()
            self.assertEqual(pipeline.read(folder / 'plan.json')['source'], 'https://new.test')
            archived = list((root / 'rejected').glob('*/*/*/*/plan.json'))
            self.assertEqual(len(archived), 1)
            self.assertEqual(pipeline.read(archived[0]), old)
