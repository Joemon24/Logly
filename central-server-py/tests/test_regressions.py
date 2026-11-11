import unittest
from datetime import datetime, timedelta, timezone
from api.soar.policy import alert_due
from api.soar.response_engine import block_ip, is_blocked
from api.rule_engine import load_rules, evaluate_rules

class PolicyTests(unittest.TestCase):
    def test_high_severity_alerts(self):
        event = {'agent': {'agent_id': 'a'}, 'alert_type': 'brute_force', 'src_ip': '192.0.2.1', 'severity': 'high'}
        _, due = alert_due(event, datetime.now(timezone.utc), {})
        self.assertTrue(due)

    def test_v2_evidence_does_not_trigger_legacy_soar(self):
        event = {'schema_version': 2, 'agent': {'agent_id': 'a'}, 'alert_type': 'brute_force', 'src_ip': '192.0.2.1', 'severity': 'critical'}
        self.assertFalse(alert_due(event, datetime.now(timezone.utc), {})[1])

    def test_cooldown_keeps_evidence(self):
        event = {'agent': {'agent_id': 'a'}, 'alert_type': 'brute_force', 'src_ip': '192.0.2.1', 'severity': 'high', 'raw': 'evidence'}
        now = datetime.now(timezone.utc)
        key, _ = alert_due(event, now, {})
        _, due = alert_due(event, now, {key: now - timedelta(seconds=1)})
        self.assertFalse(due)
        self.assertEqual(event['raw'], 'evidence')
        self.assertTrue(event['alert_suppressed'])
        self.assertTrue(alert_due(event, now + timedelta(seconds=61), {key: now})[1])

    def test_agents_do_not_suppress_each_other(self):
        now = datetime.now(timezone.utc)
        event = {'agent': {'agent_id': 'a'}, 'alert_type': 'brute_force', 'src_ip': '192.0.2.1', 'severity': 'high'}
        key, _ = alert_due(event, now, {})
        event['agent']['agent_id'] = 'b'
        self.assertTrue(alert_due(event, now, {key: now})[1])

    def test_recommendation_does_not_hide_traffic(self):
        self.assertFalse(block_ip('192.0.2.1')['enforced'])
        self.assertFalse(is_blocked('192.0.2.1'))
        self.assertEqual(block_ip('bad')['status'], 'invalid_address')

    def test_highest_severity_rule_wins(self):
        load_rules()
        matches = evaluate_rules(dict(fail_streak=10, failed_auth_count_5m=10, failed_ratio=.9, unique_users=8, ip_failed_ratio=.9, ip_rate_1m=10))
        self.assertEqual(matches[0]['type'], 'password_spraying')
        self.assertEqual(len(matches), 2)

if __name__ == '__main__':
    unittest.main()
