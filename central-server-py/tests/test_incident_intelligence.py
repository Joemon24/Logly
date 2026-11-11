"""Deterministic incident detection, evidence, and replay tests."""
import copy
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from elasticsearch import ConflictError, NotFoundError

from api.incident_intelligence import (
    BRUTE_FORCE,
    INCIDENT_INDEX,
    PASSWORD_SPRAYING,
    SUSPICIOUS_SUCCESS,
    analyze_event,
    event_time,
    extract_ip,
    extract_outcome,
    extract_user,
    incident_id,
    merge_incident,
    risk_for,
)

FIXTURES = Path(__file__).resolve().parent / 'fixtures/incidents'


def load_fixture(name):
    return json.loads((FIXTURES / f'{name}.json').read_text())


def parse_time(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


class FakeElasticsearch:
    """In-memory event store that understands the engine's query shape."""

    def __init__(self, events=()):
        self.events = {event['event_id']: copy.deepcopy(event) for event in events}
        self.incidents = {}
        self.seq_no = 0

    def search(self, index, query=None, size=10, sort=None, from_=0, **kwargs):
        hits = []
        for event in self.events.values():
            if self._matches(event, query or {}):
                hits.append({'_id': event['event_id'], '_source': copy.deepcopy(event)})
        hits.sort(key=lambda hit: event_time(hit['_source']) or datetime.min.replace(tzinfo=timezone.utc))
        return {'hits': {'total': {'value': len(hits)}, 'hits': hits[:size]}}

    def get(self, index, id):
        if id not in self.incidents:
            raise NotFoundError('missing', None, None)
        self.seq_no += 1
        return {
            '_source': copy.deepcopy(self.incidents[id]),
            '_seq_no': self.seq_no,
            '_primary_term': 1,
        }

    def create(self, index, id, document):
        if id in self.incidents:
            raise ConflictError('exists', None, None)
        self.incidents[id] = copy.deepcopy(document)

    def index(self, index, id, document, if_seq_no=None, if_primary_term=None):
        self.incidents[id] = copy.deepcopy(document)

    def _matches(self, event, query):
        for clause in query.get('bool', {}).get('filter', []):
            if 'term' in clause:
                ((field, value),) = clause['term'].items()
                if self._field(event, field.removesuffix('.keyword')) != value:
                    return False
            elif 'range' in clause:
                ((field, bounds),) = clause['range'].items()
                timestamp = event_time(event)
                if timestamp is None:
                    return False
                if 'gte' in bounds and timestamp < parse_time(bounds['gte']):
                    return False
                if 'lte' in bounds and timestamp > parse_time(bounds['lte']):
                    return False
        return True

    @staticmethod
    def _field(event, name):
        if name == 'src_ip':
            return extract_ip(event)
        if name == 'outcome':
            return extract_outcome(event)
        if name == 'username':
            return extract_user(event)
        return event.get(name)


class IncidentScenarioTests(unittest.TestCase):
    def analyze_fixture(self, fixture, passes=1):
        client = FakeElasticsearch(fixture['events'])
        for _ in range(passes):
            for event in fixture['events']:
                analyze_event(client, event)
        return client

    def test_brute_force_positive(self):
        fixture = load_fixture('brute_force_positive')
        client = self.analyze_fixture(fixture)
        expected = fixture['expected']

        self.assertEqual(len(client.incidents), expected['incidents'])
        incident = next(iter(client.incidents.values()))
        self.assertEqual(incident['attack_type'], expected['attack_type'])
        self.assertEqual(len(incident['failure_event_ids']), expected['failure_event_ids'])
        self.assertEqual(incident['usernames'], expected['usernames'])
        self.assertEqual(incident['hosts'], expected['hosts'])
        self.assertEqual(incident['severity'], expected['severity'])
        self.assertEqual(incident['risk_score'], expected['risk_score'])
        self.assertEqual(incident['mitre'], 'T1110.001')
        self.assertEqual(incident['incident_id'], incident_id(
            BRUTE_FORCE, incident['source_ip'],
            parse_time(incident['last_seen']),
        ))

    def test_password_spraying_takes_precedence_over_single_user_brute_force(self):
        fixture = load_fixture('password_spraying_positive')
        client = self.analyze_fixture(fixture)
        expected = fixture['expected']

        self.assertEqual(len(client.incidents), expected['incidents'])
        incident = next(iter(client.incidents.values()))
        self.assertEqual(incident['attack_type'], expected['attack_type'])
        self.assertEqual(incident['usernames'], expected['usernames'])
        self.assertEqual(len(incident['failure_event_ids']), expected['failure_event_ids'])
        self.assertEqual(incident['mitre'], 'T1110.003')

    def test_suspicious_success_after_failures(self):
        fixture = load_fixture('suspicious_success_positive')
        client = self.analyze_fixture(fixture)
        expected = fixture['expected']

        self.assertEqual(len(client.incidents), expected['incidents'])
        incident = next(iter(client.incidents.values()))
        self.assertEqual(incident['attack_type'], expected['attack_type'])
        self.assertEqual(len(incident['failure_event_ids']), expected['failure_event_ids'])
        self.assertEqual(len(incident['success_event_ids']), expected['success_event_ids'])
        self.assertEqual(incident['username'], 'bob')
        self.assertEqual(incident['mitre'], 'T1078')

    def test_benign_negative_cases_create_no_incident(self):
        fixture = load_fixture('benign_negative')
        client = self.analyze_fixture(fixture)
        self.assertEqual(client.incidents, {})

    def test_replay_does_not_duplicate_or_shrink_incidents(self):
        for name in (
            'brute_force_positive',
            'password_spraying_positive',
            'suspicious_success_positive',
        ):
            with self.subTest(name=name):
                fixture = load_fixture(name)
                client = self.analyze_fixture(fixture, passes=3)
                self.assertEqual(len(client.incidents), 1)
                incident = next(iter(client.incidents.values()))
                self.assertEqual(
                    incident['event_ids'], sorted(set(incident['event_ids'])),
                )
                self.assertEqual(
                    len(incident['failure_event_ids']),
                    fixture['expected']['failure_event_ids'],
                )
                self.assertEqual(
                    len(incident['success_event_ids']),
                    fixture['expected']['success_event_ids'],
                )

    def test_merge_never_removes_existing_evidence(self):
        fixture = load_fixture('brute_force_positive')
        client = self.analyze_fixture(fixture)
        incident = next(iter(client.incidents.values()))

        merged = merge_incident(incident, incident)
        self.assertEqual(merged['event_ids'], incident['event_ids'])
        self.assertEqual(merged['risk_score'], incident['risk_score'])
        self.assertEqual(merged['first_seen'], incident['first_seen'])
        self.assertEqual(merged['last_seen'], incident['last_seen'])

        incoming = copy.deepcopy(incident)
        incoming['failure_event_ids'] = ['evt_extra']
        incoming['event_ids'] = ['evt_extra']
        incoming['first_seen'] = '2026-11-09T09:59:00Z'
        merged = merge_incident(incident, incoming)
        self.assertIn('evt_extra', merged['event_ids'])
        for event_id in incident['event_ids']:
            self.assertIn(event_id, merged['event_ids'])
        self.assertEqual(merged['first_seen'], '2026-11-09T09:59:00Z')

    def test_incident_id_is_deterministic_per_bucket(self):
        anchor = datetime(2026, 11, 9, 10, 2, tzinfo=timezone.utc)
        first = incident_id(BRUTE_FORCE, '203.0.113.200', anchor)
        self.assertEqual(first, incident_id(BRUTE_FORCE, '203.0.113.200', anchor))
        self.assertNotEqual(
            first,
            incident_id(BRUTE_FORCE, '203.0.113.200', anchor + timedelta(minutes=5)),
        )
        self.assertNotEqual(
            first,
            incident_id(PASSWORD_SPRAYING, '203.0.113.200', anchor),
        )

    def test_risk_and_severity_are_evidence_derived(self):
        self.assertEqual(risk_for(BRUTE_FORCE, 4, 1), (40, 'medium'))
        self.assertEqual(risk_for(BRUTE_FORCE, 5, 1), (40, 'medium'))
        self.assertEqual(risk_for(BRUTE_FORCE, 12, 1), (82, 'critical'))
        self.assertEqual(risk_for(PASSWORD_SPRAYING, 5, 5), (50, 'medium'))
        self.assertEqual(risk_for(PASSWORD_SPRAYING, 10, 10), (90, 'critical'))
        self.assertEqual(risk_for(SUSPICIOUS_SUCCESS, 3, 1), (60, 'high'))
        self.assertEqual(risk_for(SUSPICIOUS_SUCCESS, 6, 1), (84, 'critical'))

    def test_non_authentication_and_unknown_source_are_ignored(self):
        client = FakeElasticsearch()
        heartbeat = {
            'event_id': 'evt_heartbeat',
            'timestamp': '2026-11-09T10:00:00Z',
            'agent': {'agent_id': 'web-01'},
            'event': {'category': 'heartbeat', 'action': 'heartbeat', 'outcome': 'success'},
        }
        self.assertIsNone(analyze_event(client, heartbeat))

        unknown = {
            'event_id': 'evt_unknown',
            'timestamp': '2026-11-09T10:00:00Z',
            'agent': {'agent_id': 'web-01'},
            'event': {'category': 'authentication', 'action': 'ssh_login', 'outcome': 'failure'},
            'raw_event': {'format': 'text', 'data': 'Failed password from nowhere'},
        }
        self.assertIsNone(analyze_event(client, unknown))
        self.assertEqual(client.incidents, {})


if __name__ == '__main__':
    unittest.main()
