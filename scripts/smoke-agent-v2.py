"""Exercise the built agent with a fixture log and a private SQLite outbox."""

from contextlib import closing, contextmanager
import json
from pathlib import Path
import platform
import re
import socket
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import time


SOURCE = "auth-log"
FIXTURE = Path(__file__).resolve().parents[1] / "agent/runtimev2/testdata/auth.txt"


@contextmanager
def running_agent(binary, directory, env, run):
    log = directory / f"agent-{run}.log"
    with log.open("wb") as output:
        agent = subprocess.Popen([binary], env=env, stdout=output, stderr=subprocess.STDOUT)
        try:
            try:
                yield agent
            finally:
                if agent.poll() is None:
                    agent.terminate()
                try:
                    agent.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    agent.kill()
                    agent.wait()
                    raise
            assert agent.returncode == 0, f"agent exit status: {agent.returncode}"
        except BaseException:
            print(log.read_text(errors="replace")[-8000:], file=sys.stderr, flush=True)
            raise


def snapshot(db):
    with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True, timeout=0.2)) as connection:
        connection.execute("BEGIN")
        rows = connection.execute("""
            SELECT event_id, source_generation, record_number, byte_offset, payload
            FROM outbox WHERE source_id = ? ORDER BY seq
        """, (SOURCE,)).fetchall()
        checkpoints = connection.execute("""
            SELECT generation, record_number, byte_offset
            FROM source_checkpoints WHERE source_id = ?
        """, (SOURCE,)).fetchall()
        generations = connection.execute("""
            SELECT generation, active FROM source_generations
            WHERE source_id = ? ORDER BY generation_sequence
        """, (SOURCE,)).fetchall()
        return rows, set(checkpoints), generations


def wait_for_rows(db, agent, expected):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        assert agent.poll() is None, f"agent stopped before persisting {expected} events"
        try:
            state = snapshot(db)
        except sqlite3.OperationalError:
            # The database can be absent or still initializing on the first poll.
            time.sleep(0.05)
            continue
        count = len(state[0])
        assert count <= expected, f"found {count} events, expected {expected}"
        if count == expected:
            return state
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {expected} durable events")


def append(path, line):
    with path.open("ab") as stream:
        stream.write(line)


def smoke(binary, directory, broker):
    auth = directory / "auth.log"
    db = directory / "outbox.db"
    line = FIXTURE.read_bytes().splitlines(keepends=True)[0]
    size = len(line)
    auth.write_bytes(line)
    env = {
        "LOGLY_V2_LOG_PATH": str(auth),
        "LOGLY_V2_DB_PATH": str(db),
        "LOGLY_V2_SOURCE_ID": SOURCE,
        "LOGLY_V2_AGENT_ID": "ubuntu-smoke",
        "LOGLY_V2_AGENT_TYPE": "linux",
        "LOGLY_V2_KAFKA_BROKERS": broker,
        "LOGLY_V2_KAFKA_TOPIC": "agent-v2-smoke",
        "LOGLY_V2_POLL_INTERVAL": "50ms",
        "LOGLY_V2_PUBLISH_TIMEOUT": "200ms",
        "LOGLY_V2_SHUTDOWN_TIMEOUT": "5s",
    }

    with running_agent(binary, directory, env, 1) as agent:
        initial, checkpoints, generations = wait_for_rows(db, agent, 1)
        old = initial[0][1]
        assert initial[0][2:4] == (1, 0), initial[0][:4]
        assert checkpoints == {(old, 1, 0)}, checkpoints
        assert generations == [(old, 1)], generations
    print("PASS: collected the fixture and committed its first checkpoint")

    append(auth, line)
    with running_agent(binary, directory, env, 2) as agent:
        resumed, checkpoints, _ = wait_for_rows(db, agent, 2)
        assert resumed[:1] == initial, "restart altered the stored event ID or payload"
        assert resumed[1][1:4] == (old, 2, size), resumed[1][:4]
        assert checkpoints == {(old, 2, size)}, checkpoints
        assert resumed[1][0] != initial[0][0], "different positions shared an event ID"
        print("PASS: restart resumed at record 2 and preserved the original event ID and payload")

        rotated = auth.with_suffix(".log.1")
        auth.rename(rotated)
        append(rotated, line)
        auth.write_bytes(line)

        final, checkpoints, generations = wait_for_rows(db, agent, 4)
        new = final[3][1]
        assert final[:2] == resumed, "rotation altered previously stored events"
        assert final[2][1:4] == (old, 3, 2 * size), final[2][:4]
        assert new != old and final[3][2:4] == (1, 0), final[3][:4]
        assert checkpoints == {(old, 3, 2 * size), (new, 1, 0)}, checkpoints
        assert generations == [(old, 0), (new, 1)], generations

    assert snapshot(db)[0] == final, "outbox changed before shutdown completed"
    assert len({row[0] for row in final}) == 4, "outbox IDs are not unique"
    for event_id, generation, record, offset, payload in final:
        event = json.loads(payload)
        assert re.fullmatch(r"evt2_[a-f0-9]{64}", event_id), "invalid v2 ID format"
        assert event["event_id"] == event_id and event["schema_version"] == 2
        assert event["raw_event"]["data"] == line.decode("utf-8")
        assert event["event"]["outcome"] == "failure"
        assert event["collection"]["generation"] == generation
        assert event["collection"]["position"] == {"record_number": record, "byte_offset": offset}
    print("PASS: rotation drained record 3 from the renamed inode and started a new generation at record 1")
    print("PASS: all four durable events have distinct v2 IDs and matching payloads")


if __name__ == "__main__":
    print(f"agent-v2 executable smoke test on {platform.system()} ({platform.machine()})", flush=True)
    with TemporaryDirectory(prefix="logly-agent-v2-") as temporary, socket.socket() as reserved:
        # Reserve a closed loopback port. No broker can acknowledge the events,
        # so the production dispatcher leaves the evidence in the outbox.
        reserved.bind(("127.0.0.1", 0))
        smoke(Path(sys.argv[1]).resolve(strict=True), Path(temporary), f"127.0.0.1:{reserved.getsockname()[1]}")
