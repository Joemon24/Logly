"""Detection-only response recommendations; no firewall enforcement is claimed."""
import ipaddress
import logging


def block_ip(ip):
    """Legacy entry point: recommend investigation, never suppress telemetry."""
    try:
        address = ipaddress.ip_address(ip)
    except (ValueError, TypeError):
        return {"status": "invalid_address", "enforced": False}
    logging.warning("Response recommended for %s; enforcement is disabled", address)
    return {"status": "recommended", "enforced": False, "ip": str(address)}


def is_blocked(ip):
    return False
