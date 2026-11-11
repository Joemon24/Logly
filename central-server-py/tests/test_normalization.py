import unittest
from api.normalization import extract_ip, extract_user, extract_outcome

class NormalizationTests(unittest.TestCase):
    def test_real_openssh_failure(self):
        event = {'raw_event': {'data': 'sshd[123]: Failed password for invalid user admin from 192.0.2.7 port 52100 ssh2'}}
        self.assertEqual((extract_ip(event), extract_user(event), extract_outcome(event)), ('192.0.2.7', 'admin', 'failure'))

    def test_real_openssh_ipv6_success(self):
        event = {'raw_event': {'data': 'sshd[123]: Accepted publickey for joe from 2001:db8::1 port 52100 ssh2'}}
        self.assertEqual((extract_ip(event), extract_user(event), extract_outcome(event)), ('2001:db8::1', 'joe', 'success'))

    def test_unknown_not_fabricated(self):
        event = {'raw_event': {'data': {'unexpected': True}}}
        self.assertEqual((extract_ip(event), extract_user(event), extract_outcome(event)), ('unknown', 'unknown', 'unknown'))

    def test_invalid_ip(self):
        self.assertEqual(extract_ip({'raw': 'user=joe ip=999.1.2.3 failure'}), 'unknown')

    def test_v2_uses_structured_username_without_reparsing_raw_text(self):
        event = {
            'schema_version': 2,
            'target': {'entity_type': 'user', 'name': 'user=équipe+ops@example.test'},
            'raw_event': {'data': 'Invalid user user=équipe+ops@example.test from 192.0.2.7 port 22'},
        }
        self.assertEqual(extract_user(event), 'user=équipe+ops@example.test')

    def test_v2_does_not_infer_missing_fields_from_misleading_text(self):
        event = {
            'schema_version': 2,
            'event': {'category': 'authentication', 'outcome': 'unknown'},
            'raw_event': {'data': 'Accepted password for root from 192.0.2.7'},
        }
        self.assertEqual((extract_ip(event), extract_user(event), extract_outcome(event)), ('unknown',) * 3)

    def test_legacy_ip_does_not_accept_a_valid_prefix_of_a_malformed_token(self):
        self.assertEqual(extract_ip({'raw': 'user=joe ip=192.0.2.7junk failure'}), 'unknown')
