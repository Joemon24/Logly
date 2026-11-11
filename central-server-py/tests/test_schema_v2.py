"""Schema-v2 contract and deterministic ID compatibility with the Go agent."""
import json
import unittest
from pathlib import Path

from api.schema_v2 import (
    SchemaV2Error,
    compute_event_id,
    is_v2_event,
    validate_event_v2,
)

FIXTURES = Path(__file__).resolve().parents[2] / 'agent/events/testdata'


class SchemaV2Tests(unittest.TestCase):
    def fixture(self, name):
        return json.loads((FIXTURES / name).read_text())

    def test_go_fixture_validates_and_id_matches(self):
        for name in ('authentication.json', 'sudo.json', 'account.json',
                     'process.json', 'file_integrity.json', 'heartbeat.json'):
            with self.subTest(name=name):
                event = self.fixture(name)
                validate_event_v2(event)
                self.assertEqual(compute_event_id(event), event['event_id'])

    def test_is_v2_event(self):
        self.assertTrue(is_v2_event(self.fixture('authentication.json')))
        self.assertFalse(is_v2_event({'event_id': 'x'}))
        self.assertFalse(is_v2_event([self.fixture('authentication.json')]))

    def test_tampered_raw_evidence_is_rejected(self):
        event = self.fixture('authentication.json')
        event['raw_event']['data'] += 'tampered'
        with self.assertRaisesRegex(SchemaV2Error, 'event_id mismatch'):
            validate_event_v2(event)

    def test_tampered_position_is_rejected(self):
        event = self.fixture('authentication.json')
        event['collection']['position']['record_number'] += 1
        with self.assertRaisesRegex(SchemaV2Error, 'event_id mismatch'):
            validate_event_v2(event)

    def test_bad_event_id_format(self):
        event = self.fixture('authentication.json')
        event['event_id'] = 'evt2_' + 'A' * 64
        with self.assertRaisesRegex(SchemaV2Error, 'event_id must be'):
            validate_event_v2(event)

    def test_missing_required_fields(self):
        for key in ('agent', 'event', 'collection', 'raw_event'):
            with self.subTest(key=key):
                event = self.fixture('authentication.json')
                del event[key]
                with self.assertRaises(SchemaV2Error):
                    validate_event_v2(event)

    def test_non_utc_timestamp_is_rejected(self):
        event = self.fixture('authentication.json')
        event['timestamp'] = '2026-11-09T10:15:30+02:00'
        with self.assertRaisesRegex(SchemaV2Error, 'UTC'):
            validate_event_v2(event)

    def test_ipv6_source_survives_validation(self):
        event = self.fixture('authentication.json')
        event['source']['ip'] = '2001:db8::1'
        event['raw_event']['data'] = (
            'Accepted publickey for alice from 2001:db8::1 port 50000 ssh2\n')
        event['event']['outcome'] = 'success'
        event['event_id'] = compute_event_id(event)
        validate_event_v2(event)

    def test_byte_offset_omitted_from_canonical_id_when_absent(self):
        event = self.fixture('authentication.json')
        event['collection']['position'].pop('byte_offset', None)
        event['event_id'] = compute_event_id(event)
        validate_event_v2(event)


if __name__ == '__main__':
    unittest.main()
