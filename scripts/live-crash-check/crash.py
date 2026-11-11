import os
import runpy
import signal

from api import incident_intelligence

analyze = incident_intelligence.analyze_event


def stop_at_final_analysis(client, event):
    if event["event_id"] == os.environ["LOGLY_TEST_CRASH_EVENT"]:
        print("Crash test reached incident-analysis entry", flush=True)
        os.kill(os.getpid(), signal.SIGKILL)
    return analyze(client, event)


incident_intelligence.analyze_event = stop_at_final_analysis
runpy.run_module("api.kafka_consumer", run_name="__main__")
