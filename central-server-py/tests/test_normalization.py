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
