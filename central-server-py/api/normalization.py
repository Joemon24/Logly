"""Parse the agent envelope and Linux OpenSSH authentication messages."""
import ipaddress
import re


def raw_text(event):
    raw = event.get('raw')
    if not isinstance(raw, str) or not raw:
        block = event.get('raw_event', {})
        raw = block.get('data', '') if isinstance(block, dict) else ''
    return raw if isinstance(raw, str) else ''


def extract_ip(event):
    source = event.get('source', {})
    candidate = source.get('ip') if isinstance(source, dict) else None
    if not candidate:
        match = re.search(r'(?:\bip=|\bfrom\s+)([0-9a-fA-F:.]+)', raw_text(event))
        candidate = match.group(1) if match else None
    try:
        return str(ipaddress.ip_address(candidate))
    except (ValueError, TypeError):
        return 'unknown'


def extract_user(event):
    raw = raw_text(event)
    match = re.search(r'\buser=([^\s]+)', raw)
    if not match:
        match = re.search(r'\b(?:Failed|Accepted)\s+\S+\s+for\s+(?:invalid user\s+)?(\S+)\s+from\s+', raw, re.I)
    return match.group(1) if match else 'unknown'


def extract_outcome(event):
    metadata = event.get('event', {})
    outcome = metadata.get('outcome') if isinstance(metadata, dict) else None
    if outcome in {'failure', 'success'}:
        return outcome
    raw = raw_text(event).lower()
    if 'failed' in raw or 'failure' in raw:
        return 'failure'
    if 'accepted' in raw or 'success' in raw:
        return 'success'
    return 'unknown'
