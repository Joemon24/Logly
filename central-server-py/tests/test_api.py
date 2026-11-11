"""Read API handler tests using canned Elasticsearch responses."""
import unittest

from elasticsearch import NotFoundError
from fastapi import HTTPException

from api.main import (
    get_incident,
    host_history,
    list_events,
    list_hosts,
    list_incidents,
    overview,
)


class FakeElasticsearch:
    def __init__(self, search_responses=(), count_responses=(), get_response=None,
                 missing=False):
        self.search_responses = list(search_responses)
        self.count_responses = list(count_responses)
        self.get_response = get_response
        self.missing = missing
        self.search_calls = []
        self.count_calls = []

    def search(self, **kwargs):
        if self.missing:
            raise NotFoundError('index missing', None, None)
        self.search_calls.append(kwargs)
        return self.search_responses.pop(0)

    def count(self, **kwargs):
        if self.missing:
            raise NotFoundError('index missing', None, None)
        self.count_calls.append(kwargs)
        return self.count_responses.pop(0)

    def get(self, index, id):
        if self.get_response is None:
            raise NotFoundError('missing', None, None)
        return self.get_response


class OverviewTests(unittest.TestCase):
    def test_overview_reports_counts_and_incident_breakdown(self):
        client = FakeElasticsearch(
            count_responses=[{'count': 12}, {'count': 3}, {'count': 2}],
            search_responses=[
                {'aggregations': {
                    'by_severity': {'buckets': [
                        {'key': 'high', 'doc_count': 2},
                        {'key': 'medium', 'doc_count': 1},
                    ]},
                    'by_attack_type': {'buckets': [
                        {'key': 'ssh_brute_force', 'doc_count': 2},
                        {'key': 'ssh_password_spraying', 'doc_count': 1},
                    ]},
                }},
                {'aggregations': {'hosts': {'value': 4}}},
            ],
        )

        result = overview(client=client)

        self.assertEqual(result, {
            'events': 12,
            'incidents': 3,
            'open_incidents': 2,
            'hosts': 4,
            'by_severity': {'high': 2, 'medium': 1},
            'by_attack_type': {'ssh_brute_force': 2, 'ssh_password_spraying': 1},
        })
        self.assertEqual(
            [call['index'] for call in client.count_calls],
            ['ids-events', 'ids-incidents-v2', 'ids-incidents-v2'],
        )

    def test_overview_handles_missing_indices(self):
        result = overview(client=FakeElasticsearch(missing=True))

        self.assertEqual(result, {
            'events': 0,
            'incidents': 0,
            'open_incidents': 0,
            'hosts': 0,
            'by_severity': {},
            'by_attack_type': {},
        })


class EventApiTests(unittest.TestCase):
    def test_events_are_filtered_and_summarised(self):
        client = FakeElasticsearch(search_responses=[{
            'hits': {
                'total': {'value': 1},
                'hits': [{'_source': {
                    'event_id': 'evt_1',
                    'timestamp': '2026-11-09T10:00:00Z',
                    'agent': {'agent_id': 'web-01'},
                    'src_ip': '203.0.113.9',
                    'outcome': 'failure',
                    'features': {'fail_streak': 5},
                    'raw_event': {'data': 'evidence'},
                }}],
            },
        }])

        result = list_events(
            limit=10, offset=5, agent_id='web-01',
            source_ip='203.0.113.9', outcome='failure',
            since='2026-11-09T00:00:00Z', client=client,
        )

        self.assertEqual(result['total'], 1)
        self.assertEqual(result['limit'], 10)
        self.assertEqual(result['offset'], 5)
        self.assertEqual(result['events'][0]['event_id'], 'evt_1')
        self.assertNotIn('features', result['events'][0])
        self.assertNotIn('raw_event', result['events'][0])

        call = client.search_calls[0]
        self.assertEqual(call['index'], 'ids-events')
        self.assertEqual(call['from_'], 5)
        self.assertEqual(call['size'], 10)
        filters = call['query']['bool']['filter']
        self.assertIn({'term': {'agent.agent_id.keyword': 'web-01'}}, filters)
        self.assertIn({'term': {'src_ip.keyword': '203.0.113.9'}}, filters)
        self.assertIn({'term': {'outcome.keyword': 'failure'}}, filters)
        self.assertIn({'range': {'timestamp': {'gte': '2026-11-09T00:00:00Z'}}}, filters)


class IncidentApiTests(unittest.TestCase):
    def test_incidents_are_filtered_by_host_and_severity(self):
        client = FakeElasticsearch(search_responses=[{
            'hits': {
                'total': {'value': 1},
                'hits': [{'_source': {
                    'incident_id': 'inc_1',
                    'attack_type': 'ssh_brute_force',
                    'severity': 'medium',
                    'hosts': ['web-01', 'web-02'],
                    'event_ids': ['evt_1'],
                }}],
            },
        }])

        result = list_incidents(
            severity='medium', attack_type='ssh_brute_force',
            status='open', source_ip='203.0.113.200',
            host='web-02', client=client,
        )

        self.assertEqual(result['total'], 1)
        self.assertEqual(result['incidents'][0]['incident_id'], 'inc_1')
        filters = client.search_calls[0]['query']['bool']['filter']
        self.assertIn({'term': {'hosts.keyword': 'web-02'}}, filters)
        self.assertIn({'term': {'severity.keyword': 'medium'}}, filters)

    def test_missing_incident_index_returns_empty_list(self):
        result = list_incidents(client=FakeElasticsearch(missing=True))

        self.assertEqual(result, {
            'total': 0,
            'limit': 50,
            'offset': 0,
            'incidents': [],
        })

    def test_incident_detail_includes_ordered_timeline_and_missing_evidence(self):
        client = FakeElasticsearch(
            get_response={'_source': {
                'incident_id': 'inc_1',
                'attack_type': 'ssh_brute_force',
                'event_ids': ['evt_1', 'evt_2'],
            }},
            search_responses=[{
                'hits': {
                    'total': {'value': 1},
                    'hits': [{'_id': 'evt_1', '_source': {
                        'event_id': 'evt_1',
                        'timestamp': '2026-11-09T10:00:00Z',
                        'outcome': 'failure',
                    }}],
                },
            }],
        )

        result = get_incident('inc_1', client=client)

        self.assertEqual(result['incident_id'], 'inc_1')
        self.assertEqual([event['event_id'] for event in result['timeline']], ['evt_1'])
        self.assertEqual(result['evidence_event_count'], 1)
        self.assertEqual(result['missing_event_ids'], ['evt_2'])
        self.assertEqual(client.search_calls[0]['query'], {'ids': {'values': ['evt_1', 'evt_2']}})

    def test_missing_incident_returns_404(self):
        with self.assertRaises(HTTPException) as raised:
            get_incident('inc_missing', client=FakeElasticsearch())
        self.assertEqual(raised.exception.status_code, 404)


class HostApiTests(unittest.TestCase):
    def test_hosts_list_uses_terms_aggregation(self):
        client = FakeElasticsearch(search_responses=[{
            'aggregations': {'hosts': {'buckets': [{
                'key': 'web-01',
                'doc_count': 7,
                'last_seen': {'value_as_string': '2026-11-09T10:00:00Z'},
            }]}},
        }])

        result = list_hosts(limit=10, client=client)

        self.assertEqual(result['hosts'], [{
            'host': 'web-01',
            'events': 7,
            'last_seen': '2026-11-09T10:00:00Z',
        }])
        aggregation = client.search_calls[0]['aggs']['hosts']
        self.assertEqual(aggregation['terms']['field'], 'agent.agent_id.keyword')

    def test_host_history_combines_events_and_incidents(self):
        client = FakeElasticsearch(search_responses=[
            {'hits': {'total': {'value': 1}, 'hits': [{'_source': {
                'event_id': 'evt_1',
                'agent': {'agent_id': 'web-01'},
            }}]}},
            {'hits': {'total': {'value': 1}, 'hits': [{'_source': {
                'incident_id': 'inc_1',
                'hosts': ['web-01'],
            }}]}},
        ])

        result = host_history('web-01', client=client)

        self.assertEqual(result['host'], 'web-01')
        self.assertEqual(result['event_total'], 1)
        self.assertEqual(result['events'][0]['event_id'], 'evt_1')
        self.assertEqual(result['incident_total'], 1)
        self.assertEqual(result['incidents'][0]['incident_id'], 'inc_1')
        self.assertEqual(client.search_calls[0]['query']['bool']['filter'],
                         [{'term': {'agent.agent_id.keyword': 'web-01'}}])


if __name__ == '__main__':
    unittest.main()
