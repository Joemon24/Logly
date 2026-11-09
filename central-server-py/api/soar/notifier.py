def send_notification(event):

    print(
        f"[NOTIFIER] "
        f"{event.get('severity', '').upper()} ALERT | "
        f"{event.get('alert_type')} | "
        f"IP={event.get('src_ip')} | "
        f"MITRE={event.get('mitre_technique')}"
    )
