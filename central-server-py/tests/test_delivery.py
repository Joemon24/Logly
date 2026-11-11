"""Exercise the consumer loop with fake transports, including claim boundaries."""
import contextlib
import copy
import io
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import types
from datetime import datetime, timedelta, timezone

# Preload modules imported inside the patched sys.modules block; patch.dict
# restores its snapshot on exit and would otherwise drop them.
import joblib  # noqa: F401
import numpy  # noqa: F401
import sklearn  # noqa: F401
import unittest
from unittest.mock import patch

CONSUMER = Path(__file__).resolve().parents[1] / 'api/kafka_consumer.py'
FIXTURE = Path(__file__).resolve().parents[2] / 'agent/events/testdata/authentication.json'


def load_v2_fixture():
    return json.loads(FIXTURE.read_text())


class FakeConflictError(Exception):
    pass


class FakeNotFoundError(Exception):
    pass


class FakeElasticsearch:
    """In-memory subset of the Elasticsearch client used by the consumer."""

    def __init__(self, calls, documents=None, fail_create=False, fail_search=False,
                 fail_incident_type=None):
        self.calls = calls
        self.store = {
            key: copy.deepcopy(document)
            for key, document in (documents or {}).items()
        }
        self.fail_create = fail_create
        self.fail_search = fail_search
        self.fail_incident_type = fail_incident_type
        self.indices = types.SimpleNamespace(refresh=self.refresh)

    def create(self, index, id, document):
        self.calls.append(('create', index, id))
        if self.fail_create:
            raise RuntimeError('elasticsearch unavailable')
        if (index == 'ids-incidents-v2' and self.fail_incident_type
                and document['attack_type'] == self.fail_incident_type):
            raise RuntimeError('incident write unavailable')
        if (index, id) in self.store:
            raise FakeConflictError('document already exists')
        self.store[(index, id)] = copy.deepcopy(document)

    def get(self, index, id):
        self.calls.append(('get', index, id))
        if (index, id) not in self.store:
            raise FakeNotFoundError('document missing')
        return {
            '_source': copy.deepcopy(self.store[(index, id)]),
            '_seq_no': 1,
            '_primary_term': 1,
        }

    def index(self, index, id, document, **kwargs):
        self.calls.append(('index', index, id))
        self.store[(index, id)] = copy.deepcopy(document)

    def search(self, index, query=None, size=10, sort=None, from_=0, **kwargs):
        self.calls.append(('search', index))
        if self.fail_search == 'partial':
            return {'timed_out': True, 'hits': {'hits': []}}
        if self.fail_search:
            raise RuntimeError('elasticsearch unavailable')
        hits = []
        for (document_index, _), document in self.store.items():
            if document_index != index or not document.get('processed'):
                continue
            for condition in query['bool']['filter']:
                if 'term' in condition:
                    field, expected = next(iter(condition['term'].items()))
                    if document.get(field.removesuffix('.keyword')) != expected:
                        break
                else:
                    bounds = condition['range']['timestamp']
                    timestamp = datetime.fromisoformat(document['timestamp'])
                    if not datetime.fromisoformat(bounds['gte']) <= timestamp <= datetime.fromisoformat(bounds['lte']):
                        break
            else:
                hits.append({'_source': copy.deepcopy(document)})
        return {'hits': {'total': {'value': len(hits)}, 'hits': hits[:size]}}

    def refresh(self, index):
        self.calls.append(('refresh', index))


class DeliveryTests(unittest.TestCase):
    def run_consumer(self, fail=False, messages=None, documents=None,
                     fail_create=False, fail_search=False, fail_bulk=None,
                     partial_bulk=False, expect_error=None, fail_incident_type=None):
        calls = []
        event = {'agent': {'agent_id': 'test'}, 'raw': 'user=joe ip=192.0.2.2 success'}
        message = types.SimpleNamespace(topic='ids.events.v1', partition=0, offset=7, value=[event])
        messages = messages or [message]
        client = FakeElasticsearch(
            calls, documents, fail_create=fail_create, fail_search=fail_search,
            fail_incident_type=fail_incident_type,
        )

        class Consumer:
            def __init__(self, *args, **kwargs):
                if kwargs['enable_auto_commit']:
                    raise AssertionError('Auto commit enabled')
            def __iter__(self): return iter(messages)
            def commit(self, offsets): calls.append(('commit', offsets))
            def close(self): calls.append(('close', None))

        def bulk(es, actions):
            calls.append(('bulk', list(actions)))
            if partial_bulk:
                for action in actions:
                    client.store[(action['_index'], action['_id'])] = copy.deepcopy(action['_source'])
                raise RuntimeError('storage unavailable')
            if fail or (fail_bulk and fail_bulk(actions)):
                raise RuntimeError('storage unavailable')
            for action in actions:
                client.store[(action['_index'], action['_id'])] = copy.deepcopy(action['_source'])

        fake_es = types.ModuleType('elasticsearch')
        fake_es.Elasticsearch = lambda *a, **kw: client
        fake_es.helpers = types.SimpleNamespace(bulk=bulk)
        fake_es.ConflictError = FakeConflictError
        fake_es.NotFoundError = FakeNotFoundError
        fake_kafka = types.ModuleType('kafka')
        fake_kafka.KafkaConsumer = Consumer
        structs = types.ModuleType('kafka.structs')
        structs.TopicPartition = lambda topic, partition: (topic, partition)
        structs.OffsetAndMetadata = (
            lambda offset, metadata, leader_epoch:
            (offset, metadata, leader_epoch)
        )
        # Re-import connection-owning modules against this test's fake client.
        owned = [
            'api.settings', 'api.soar.alert_manager', 'api.soar.incident_manager',
            'api.incident_intelligence',
        ]
        previous = {name: sys.modules.pop(name, None) for name in owned}
        original_cwd = os.getcwd()
        try:
            with tempfile.TemporaryDirectory() as directory, patch_modules(fake_kafka, structs, fake_es), contextlib.redirect_stdout(io.StringIO()):
                os.chdir(directory)
                if expect_error:
                    with self.assertRaisesRegex(*expect_error):
                        runpy.run_path(str(CONSUMER))
                else:
                    runpy.run_path(str(CONSUMER))
        finally:
            os.chdir(original_cwd)
            for name in owned:
                sys.modules.pop(name, None)
                if previous[name] is not None: sys.modules[name] = previous[name]
        return calls, client

    def test_small_batch_persisted_before_explicit_commit(self):
        calls, _ = self.run_consumer()
        self.assertEqual([c[0] for c in calls], ['bulk', 'commit', 'close'])
        self.assertEqual(calls[0][1][0]['_id'], 'ids.events.v1:0:7:0')
        self.assertEqual(
            calls[1][1],
            {('ids.events.v1', 0): (8, '', -1)}
        )

    def test_failed_storage_never_commits(self):
        calls, _ = self.run_consumer(
            fail=True, expect_error=(RuntimeError, 'storage unavailable'),
        )
        self.assertEqual([c[0] for c in calls], ['bulk', 'close'])

    def test_v2_claims_before_write_and_commits_after_incident_analysis(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=3, value=event)
        calls, client = self.run_consumer(messages=[message])

        self.assertEqual(
            [c[0] for c in calls],
            ['create', 'bulk', 'refresh', 'search', 'commit', 'close'],
        )
        self.assertEqual(calls[0][1:], ('ids-events', event['event_id']))
        document = calls[1][1][0]
        self.assertEqual(document['_index'], 'ids-events')
        self.assertEqual(document['_id'], event['event_id'])
        self.assertEqual(document['_source']['storage_id'], event['event_id'])
        self.assertTrue(document['_source']['processed'])
        self.assertEqual(document['_source']['raw_event'], event['raw_event'])
        self.assertEqual(document['_source']['collection'], event['collection'])
        self.assertEqual(calls[-2][1], {('ids.events.v2', 0): (4, '', -1)})
        self.assertTrue(client.store[('ids-events', event['event_id'])]['processed'])

    def test_v2_processed_duplicate_skips_features_but_commits(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=4, value=event)
        documents = {
            ('ids-events', event['event_id']): {'processed': True, 'event_id': event['event_id']},
        }
        calls, _ = self.run_consumer(messages=[message], documents=documents)

        self.assertEqual(
            [c[0] for c in calls],
            ['create', 'get', 'refresh', 'search', 'commit', 'close'],
        )
        self.assertEqual(calls[-2][1], {('ids.events.v2', 0): (5, '', -1)})

    def test_v2_legacy_document_without_claim_flag_is_a_duplicate(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=13, value=event)
        documents = {
            ('ids-events', event['event_id']): {
                'event_id': event['event_id'],
                'storage_id': event['event_id'],
                'raw_event': event['raw_event'],
            },
        }
        calls, _ = self.run_consumer(messages=[message], documents=documents)

        self.assertEqual(
            [c[0] for c in calls],
            ['create', 'get', 'refresh', 'search', 'commit', 'close'],
        )
        self.assertEqual(calls[-2][1], {('ids.events.v2', 0): (14, '', -1)})

    def test_v2_fresh_inflight_claim_stops_without_commit(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=5, value=event)
        documents = {
            ('ids-events', event['event_id']): {
                'processed': False,
                'claimed_at': datetime.now(timezone.utc).isoformat(),
            },
        }
        calls, _ = self.run_consumer(
            messages=[message], documents=documents,
            expect_error=(RuntimeError, 'claimed by another consumer'),
        )
        self.assertEqual([c[0] for c in calls], ['create', 'get', 'close'])

    def test_v2_stale_claim_is_taken_over(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=6, value=event)
        stale = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        documents = {
            ('ids-events', event['event_id']): {
                'processed': False,
                'claimed_at': stale,
            },
        }
        calls, client = self.run_consumer(messages=[message], documents=documents)

        self.assertEqual(
            [c[0] for c in calls],
            ['create', 'get', 'index', 'bulk', 'refresh', 'search', 'commit', 'close'],
        )
        self.assertTrue(client.store[('ids-events', event['event_id'])]['processed'])

    def test_v2_partial_bulk_then_replay_is_idempotent(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=7, value=event)
        calls, client = self.run_consumer(
            messages=[message], partial_bulk=True,
            expect_error=(RuntimeError, 'storage unavailable'),
        )
        self.assertNotIn('commit', [c[0] for c in calls])
        self.assertTrue(client.store[('ids-events', event['event_id'])]['processed'])

        replay_calls, replay_client = self.run_consumer(
            messages=[message], documents=client.store,
        )
        self.assertEqual(
            [c[0] for c in replay_calls],
            ['create', 'get', 'refresh', 'search', 'commit', 'close'],
        )
        self.assertEqual(
            [key for key in replay_client.store if key[0] == 'ids-events'],
            [('ids-events', event['event_id'])],
        )

    def test_v2_incident_analysis_failure_never_commits(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=8, value=event)
        calls, _ = self.run_consumer(
            messages=[message], fail_search=True,
            expect_error=(RuntimeError, 'elasticsearch unavailable'),
        )
        self.assertEqual(
            [c[0] for c in calls],
            ['create', 'bulk', 'refresh', 'search', 'close'],
        )

    def test_v2_incomplete_search_never_commits(self):
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=8, value=load_v2_fixture(),
        )
        calls, _ = self.run_consumer(
            messages=[message], fail_search='partial',
            expect_error=(RuntimeError, 'incomplete Elasticsearch incident evidence search'),
        )
        self.assertEqual(
            [c[0] for c in calls],
            ['create', 'bulk', 'refresh', 'search', 'close'],
        )

    def test_v2_replays_after_one_incident_was_written_and_the_next_failed(self):
        from api.schema_v2 import compute_event_id

        events = []
        for i in range(6):
            event = load_v2_fixture()
            event['timestamp'] = f'2026-11-09T10:15:{30 + i}Z'
            event['collection']['position']['record_number'] += i
            event['collection']['position']['byte_offset'] += i * 100
            if i == 5:
                event['event']['outcome'] = 'success'
                event['raw_event']['data'] = event['raw_event']['data'].replace(
                    'Failed password for', 'Accepted password for',
                )
            event['event_id'] = compute_event_id(event)
            events.append(event)

        def message():
            return types.SimpleNamespace(
                topic='ids.events.v2', partition=0, offset=8,
                value=copy.deepcopy(events),
            )

        with patch('api.soar.response_engine.block_ip') as block_ip, patch(
            'api.soar.notifier.send_notification',
        ) as notify:
            calls, client = self.run_consumer(
                messages=[message()],
                fail_incident_type='suspicious_login_success',
                expect_error=(RuntimeError, 'incident write unavailable'),
            )
            self.assertNotIn('commit', [call[0] for call in calls])
            self.assertEqual(calls[-1][0], 'close')
            indexed = {
                key: doc for key, doc in client.store.items() if key[0] == 'ids-events'
            }
            self.assertEqual(len(indexed), 6)
            triggered_ids = {
                doc['event_id'] for doc in indexed.values() if doc['soar_triggered']
            }
            self.assertTrue(triggered_ids)
            legacy_keys = {
                (index, event_id)
                for index in ('ids-alerts', 'ids-incidents')
                for event_id in triggered_ids
            }
            self.assertEqual(block_ip.call_count, len(triggered_ids))
            self.assertEqual(notify.call_count, len(triggered_ids))

            incidents = {
                key: doc for key, doc in client.store.items() if key[0] == 'ids-incidents-v2'
            }
            self.assertEqual([doc['attack_type'] for doc in incidents.values()], ['ssh_brute_force'])
            brute_key, brute = next(iter(incidents.items()))
            self.assertEqual(set(client.store), set(indexed) | legacy_keys | {brute_key})
            legacy_docs = {key: client.store[key] for key in legacy_keys}
            block_ip.reset_mock()
            notify.reset_mock()

            replay_calls, replay_client = self.run_consumer(
                messages=[message()], documents=client.store,
            )
            block_ip.assert_not_called()
            notify.assert_not_called()

        self.assertEqual(replay_calls[-2:], [
            ('commit', {('ids.events.v2', 0): (9, '', -1)}), ('close', None),
        ])
        self.assertNotIn('bulk', [call[0] for call in replay_calls])
        self.assertFalse(any(
            call[0] in {'create', 'index'} and call[1] in {'ids-alerts', 'ids-incidents'}
            for call in replay_calls
        ))
        self.assertEqual({key: replay_client.store[key] for key in legacy_keys}, legacy_docs)
        self.assertEqual(
            {key: doc for key, doc in replay_client.store.items() if key[0] == 'ids-events'},
            indexed,
        )
        incidents = {
            key: doc for key, doc in replay_client.store.items() if key[0] == 'ids-incidents-v2'
        }
        self.assertEqual(len(incidents), 2)
        self.assertEqual(incidents[brute_key]['detected_at'], brute['detected_at'])
        failure_ids = sorted(event['event_id'] for event in events[:5])
        self.assertEqual(incidents[brute_key]['failure_event_ids'], failure_ids)
        suspicious, = [doc for key, doc in incidents.items() if key != brute_key]
        self.assertEqual(suspicious['attack_type'], 'suspicious_login_success')
        self.assertEqual(suspicious['failure_event_ids'], failure_ids)
        self.assertEqual(suspicious['success_event_ids'], [events[-1]['event_id']])

    def test_v2_invalid_event_is_rejected_and_committed(self):
        event = load_v2_fixture()
        event['event_id'] = 'evt2_' + '0' * 64
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=9, value=event)
        calls, _ = self.run_consumer(messages=[message])

        self.assertEqual([c[0] for c in calls], ['bulk', 'commit', 'close'])
        document = calls[0][1][0]
        self.assertEqual(document['_index'], 'ids-rejected')
        self.assertIn('event_id mismatch', document['_source']['reason'])
        self.assertEqual(calls[1][1], {('ids.events.v2', 0): (10, '', -1)})

    def test_malformed_json_is_rejected_and_committed(self):
        payload = {
            '__logly_malformed__': 'Expecting value',
            'raw': 'not json',
        }
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=10, value=payload)
        calls, _ = self.run_consumer(messages=[message])

        self.assertEqual([c[0] for c in calls], ['bulk', 'commit', 'close'])
        document = calls[0][1][0]
        self.assertEqual(document['_index'], 'ids-rejected')
        self.assertEqual(
            document['_id'], 'ids.events.v2:0:10:invalid:json')
        self.assertEqual(calls[1][1], {('ids.events.v2', 0): (11, '', -1)})

    def test_v2_claim_failure_never_commits(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=11, value=event)
        calls, _ = self.run_consumer(
            messages=[message], fail_create=True,
            expect_error=(RuntimeError, 'elasticsearch unavailable'),
        )
        self.assertEqual([c[0] for c in calls], ['create', 'close'])

    def test_v2_rejection_write_failure_never_commits(self):
        event = load_v2_fixture()
        event['event_id'] = 'evt2_' + 'f' * 64
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=12, value=event)
        calls, _ = self.run_consumer(
            messages=[message],
            fail_bulk=lambda actions: actions[0]['_index'] == 'ids-rejected',
            expect_error=(RuntimeError, 'storage unavailable'),
        )
        self.assertEqual([c[0] for c in calls], ['bulk', 'close'])


def patch_modules(kafka_module, structs_module, elasticsearch_module):
    from unittest.mock import patch
    return patch.dict(sys.modules, {
        'kafka': kafka_module,
        'kafka.structs': structs_module,
        'elasticsearch': elasticsearch_module,
    })


if __name__ == '__main__':
    unittest.main()
