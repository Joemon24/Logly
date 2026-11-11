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
                 fail_incident_type=None, refresh_failed=False, competing_write=None):
        self.refresh_failed = refresh_failed
        self.calls = calls
        self.queries = []
        self.store = {
            key: copy.deepcopy(document)
            for key, document in (documents or {}).items()
        }
        self.versions = dict.fromkeys(self.store, 1)
        self.competing_write = competing_write
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
        return self.write(index, id, document)

    def get(self, index, id):
        self.calls.append(('get', index, id))
        if (index, id) not in self.store:
            raise FakeNotFoundError('document missing')
        return {
            '_source': copy.deepcopy(self.store[(index, id)]),
            '_seq_no': self.versions[(index, id)],
            '_primary_term': 1,
        }

    def index(self, index, id, document, **kwargs):
        self.calls.append(('index', index, id))
        return self.write(index, id, document, stage='takeover', **kwargs)

    def write(self, index, id, document, stage=None, if_seq_no=None, if_primary_term=None):
        key = (index, id)
        version = self.versions.get(key, 0)
        if self.competing_write and self.competing_write[0] == stage:
            self.store[key] = copy.deepcopy(self.competing_write[1])
            version += 1
            self.versions[key] = version
        if if_seq_no is not None and (if_seq_no != version or if_primary_term != 1):
            raise FakeConflictError('version conflict')
        self.store[key] = copy.deepcopy(document)
        self.versions[key] = version + 1
        return {'_seq_no': version + 1, '_primary_term': 1}

    def search(self, index, query=None, size=10, sort=None, from_=0, **kwargs):
        self.calls.append(('search', index))
        self.queries.append(query)
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
        return {'_shards': {'failed': int(self.refresh_failed)}}


class DeliveryTests(unittest.TestCase):
    def run_consumer(self, fail=False, messages=None, documents=None,
                     fail_create=False, fail_search=False, fail_bulk=None,
                     partial_bulk=False, expect_error=None, fail_incident_type=None,
                     refresh_failed=False, competing_write=None):
        calls = []
        event = {'agent': {'agent_id': 'test'}, 'raw': 'user=joe ip=192.0.2.2 success'}
        message = types.SimpleNamespace(topic='ids.events.v1', partition=0, offset=7, value=[event])
        messages = messages or [message]
        client = FakeElasticsearch(
            calls, documents, fail_create=fail_create, fail_search=fail_search,
            fail_incident_type=fail_incident_type, refresh_failed=refresh_failed,
            competing_write=competing_write,
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
            if fail or (fail_bulk and fail_bulk(actions)):
                raise RuntimeError('storage unavailable')
            for action in actions[:partial_bulk] if partial_bulk else actions:
                client.write(
                    action['_index'], action['_id'], action['_source'], stage='bulk',
                    if_seq_no=action.get('if_seq_no'), if_primary_term=action.get('if_primary_term'),
                )
            if partial_bulk:
                raise RuntimeError('storage unavailable')

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
            ('ids-events', event['event_id']): {**event, 'processed': True, 'storage_id': event['event_id']},
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
                **event,
                'storage_id': event['event_id'],
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

    def test_claim_and_bulk_cannot_overwrite_a_newer_completion(self):
        event = load_v2_fixture()
        key = ('ids-events', event['event_id'])
        completed = {**event, 'processed': True, 'storage_id': event['event_id']}
        stale = {
            'processed': False,
            'claimed_at': (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat(),
        }
        for stage, documents in [('takeover', {key: stale}), ('bulk', {})]:
            with self.subTest(stage=stage):
                message = types.SimpleNamespace(
                    topic='ids.events.v2', partition=0, offset=6, value=copy.deepcopy(event),
                )
                calls, client = self.run_consumer(
                    messages=[message], documents=documents, competing_write=(stage, completed),
                    expect_error=(FakeConflictError, 'version conflict'),
                )
                self.assertNotIn('commit', [call[0] for call in calls])
                self.assertEqual(client.store[key], completed)
                replay, client = self.run_consumer(messages=[message], documents=client.store)
                self.assertEqual(replay[-2], ('commit', {('ids.events.v2', 0): (7, '', -1)}))
                self.assertEqual(client.store[key], completed)

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
            self.assertTrue(all(doc['soar_triggered'] is False for doc in indexed.values()))
            block_ip.assert_not_called()
            notify.assert_not_called()

            incidents = {
                key: doc for key, doc in client.store.items() if key[0] == 'ids-incidents-v2'
            }
            self.assertEqual([doc['attack_type'] for doc in incidents.values()], ['ssh_brute_force'])
            brute_key, brute = next(iter(incidents.items()))
            self.assertEqual(set(client.store), set(indexed) | {brute_key})
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

    def test_duplicate_id_in_one_message_does_not_block_offset(self):
        event = load_v2_fixture()
        message = types.SimpleNamespace(
            topic='ids.events.v2', partition=0, offset=9, value=[event, copy.deepcopy(event)],
        )
        calls, client = self.run_consumer(messages=[message])
        self.assertEqual([call[0] for call in calls], ['create', 'bulk', 'refresh', 'search', 'commit', 'close'])
        self.assertEqual(len(client.store), 1)

    def test_replay_uses_stored_event_and_retries_after_repeated_analysis_failures(self):
        event = load_v2_fixture()
        original_id = event['event_id']

        def message(value):
            return types.SimpleNamespace(topic='ids.events.v2', partition=0, offset=10, value=copy.deepcopy(value))

        calls, client = self.run_consumer(
            messages=[message(event)], fail_search=True,
            expect_error=(RuntimeError, 'elasticsearch unavailable'),
        )
        self.assertNotIn('commit', [call[0] for call in calls])
        altered = copy.deepcopy(event)
        altered['source']['ip'] = '198.51.100.99'
        altered['event']['outcome'] = 'success'
        for _ in range(2):
            calls, client = self.run_consumer(
                messages=[message(altered)], documents=client.store, fail_search=True,
                expect_error=(RuntimeError, 'elasticsearch unavailable'),
            )
            self.assertEqual([call[0] for call in calls], ['create', 'get', 'refresh', 'search', 'close'])
            self.assertEqual(client.store[('ids-events', original_id)]['source'], event['source'])
        calls, recovered = self.run_consumer(messages=[message(altered)], documents=client.store)
        self.assertEqual(calls[-2], ('commit', {('ids.events.v2', 0): (11, '', -1)}))
        self.assertEqual(recovered.store, client.store)
        self.assertIn({'term': {'src_ip.keyword': event['source']['ip']}}, recovered.queries[0]['bool']['filter'])

    def test_partial_refresh_leaves_offset_uncommitted_and_replay_recovers(self):
        def message():
            return types.SimpleNamespace(topic='ids.events.v2', partition=0, offset=11, value=load_v2_fixture())
        calls, client = self.run_consumer(
            messages=[message()], refresh_failed=True,
            expect_error=(RuntimeError, 'incomplete Elasticsearch event refresh'),
        )
        self.assertEqual([call[0] for call in calls], ['create', 'bulk', 'refresh', 'close'])
        calls, recovered = self.run_consumer(messages=[message()], documents=client.store)
        self.assertEqual(calls[-2], ('commit', {('ids.events.v2', 0): (12, '', -1)}))
        self.assertEqual(recovered.store, client.store)

    def test_mixed_partial_bulk_waits_for_stale_claim_then_recovers(self):
        from api.schema_v2 import compute_event_id

        first, second = load_v2_fixture(), load_v2_fixture()
        second['collection']['position']['record_number'] += 1
        second['event_id'] = compute_event_id(second)

        def message():
            return types.SimpleNamespace(topic='ids.events.v2', partition=0, offset=12,
                                         value=copy.deepcopy([first, second]))

        calls, client = self.run_consumer(
            messages=[message()], partial_bulk=1, expect_error=(RuntimeError, 'storage unavailable'),
        )
        self.assertNotIn('commit', [call[0] for call in calls])
        first_key = ('ids-events', first['event_id'])
        second_key = ('ids-events', second['event_id'])
        original = copy.deepcopy(client.store[first_key])
        self.assertTrue(original['processed'])
        self.assertFalse(client.store[second_key]['processed'])
        calls, client = self.run_consumer(
            messages=[message()], documents=client.store,
            expect_error=(RuntimeError, 'claimed by another consumer'),
        )
        self.assertNotIn('commit', [call[0] for call in calls])
        client.store[second_key]['claimed_at'] = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        calls, recovered = self.run_consumer(messages=[message()], documents=client.store)
        self.assertEqual(calls[-2], ('commit', {('ids.events.v2', 0): (13, '', -1)}))
        self.assertEqual(recovered.store[first_key], original)
        self.assertTrue(recovered.store[second_key]['processed'])

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
