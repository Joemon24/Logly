"""Exercise the consumer loop with fake transports, including write failures."""
import contextlib
import io
import os
from pathlib import Path
import runpy
import sys
import tempfile
import types
import unittest
import joblib
import numpy
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from unittest.mock import patch

CONSUMER = Path(__file__).resolve().parents[1] / 'api/kafka_consumer.py'

class DeliveryTests(unittest.TestCase):
    def run_consumer(self, fail=False):
        calls = []
        event = {'agent': {'agent_id': 'test'}, 'raw': 'user=joe ip=192.0.2.2 success'}
        message = types.SimpleNamespace(topic='ids.events.v1', partition=0, offset=7, value=[event])
        class Consumer:
            def __init__(self, *args, **kwargs):
                if kwargs['enable_auto_commit']:
                    raise AssertionError('Auto commit enabled')
            def __iter__(self): return iter([message])
            def commit(self, offsets): calls.append(('commit', offsets))
            def close(self): calls.append(('close', None))
        def bulk(es, actions):
            calls.append(('bulk', list(actions)))
            if fail: raise RuntimeError('storage unavailable')
        fake_es = types.ModuleType('elasticsearch')
        fake_es.Elasticsearch = lambda *a, **kw: object()
        fake_es.helpers = types.SimpleNamespace(bulk=bulk)
        fake_kafka = types.ModuleType('kafka')
        fake_kafka.KafkaConsumer = Consumer
        structs = types.ModuleType('kafka.structs')
        structs.TopicPartition = lambda topic, partition: (topic, partition)
        structs.OffsetAndMetadata = lambda offset, metadata: (offset, metadata)
        # Re-import connection-owning modules against this test's fake client.
        owned = ['api.settings', 'api.soar.alert_manager', 'api.soar.incident_manager']
        previous = {name: sys.modules.pop(name, None) for name in owned}
        original_cwd = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as directory, patch.dict(sys.modules, {'kafka': fake_kafka, 'kafka.structs': structs, 'elasticsearch': fake_es}), contextlib.redirect_stdout(io.StringIO()):
                os.chdir(directory)
                if fail:
                    with self.assertRaisesRegex(RuntimeError, 'storage unavailable'):
                        runpy.run_path(str(CONSUMER))
                else:
                    runpy.run_path(str(CONSUMER))
        finally:
            os.chdir(original_cwd)
            for name in owned:
                sys.modules.pop(name, None)
                if previous[name] is not None: sys.modules[name] = previous[name]
        return calls

    def test_small_batch_persisted_before_explicit_commit(self):
        calls = self.run_consumer()
        self.assertEqual([c[0] for c in calls], ['bulk', 'commit', 'close'])
        self.assertEqual(calls[0][1][0]['_id'], 'ids.events.v1:0:7:0')
        self.assertEqual(calls[1][1], {('ids.events.v1', 0): (8, '')})

    def test_failed_storage_never_commits(self):
        self.assertEqual([c[0] for c in self.run_consumer(fail=True)], ['bulk', 'close'])
