"""Run from central-server-py: python -m experiments.evaluate --help."""
import argparse
import hashlib
import json
from pathlib import Path

from experiments.replay import FEATURES, FEATURE_VERSION, features, replay
from api.incident_intelligence import SUSPICIOUS_WINDOW, is_authentication_event
from api.incident_intelligence import BRUTE_FORCE, PASSWORD_SPRAYING, SUSPICIOUS_SUCCESS, event_time
from api.normalization import extract_ip, extract_outcome, extract_user

FIXTURES = Path(__file__).resolve().parents[1] / "tests/fixtures/incidents"
SCENARIOS = (
    ("brute_force_positive", "SSH brute force", BRUTE_FORCE),
    ("password_spraying_positive", "SSH password spraying", PASSWORD_SPRAYING),
    ("suspicious_success_positive", "Suspicious successful login", SUSPICIOUS_SUCCESS),
    ("benign_negative", "Benign authentication", None),
)


def fixture_cases():
    return [
        {"id": name, "title": title, "expected_rule": expected, "synthetic": True,
         "events": json.loads((FIXTURES / f"{name}.json").read_text())["events"]}
        for name, title, expected in SCENARIOS
    ]


def metrics(expected, predicted):
    tp = sum(a and b for a, b in zip(expected, predicted, strict=True))
    fp = sum(not a and b for a, b in zip(expected, predicted, strict=True))
    fn = sum(a and not b for a, b in zip(expected, predicted, strict=True))
    tn = len(expected) - tp - fp - fn
    return {"tp": tp, "fp": fp, "tn": tn, "fn": fn,
            "precision": tp / (tp + fp) if tp + fp else None,
            "recall": tp / (tp + fn) if tp + fn else None,
            "false_positive_rate": fp / (fp + tn) if fp + tn else None}


def recordings(cases):
    return [
        {"id": case["id"], "title": case["title"], "expected_rule": case["expected_rule"],
         "events": [{"id": row["event_id"], "time": row["timestamp"], "outcome": extract_outcome(row),
                     "user": extract_user(row), "ip": extract_ip(row)} for row in case["events"]],
         "findings": replay(case["events"])}
        for case in cases
    ]


def read_cases(path):
    cases = json.loads(path.read_text())
    if not isinstance(cases, list) or not cases:
        raise ValueError(f"{path}: expected a nonempty JSON list")
    ids = set()
    for case in cases:
        if not isinstance(case, dict):
            raise ValueError(f"{path}: expected a case object")
        if (not isinstance(case.get("id"), str) or not case["id"] or case["id"] in ids
                or type(case.get("label")) is not bool or not isinstance(case.get("events"), list)
                or not case["events"] or any(not isinstance(row, dict) or not isinstance(row.get("event_id"), str)
                                             or not row["event_id"] or not event_time(row)
                                             or not is_authentication_event(row) for row in case["events"])):
            raise ValueError(f"{path}: each case needs a unique id, boolean label and timestamped event IDs")
        rows = case["events"]
        times = [event_time(row) for row in rows]
        sources = {extract_ip(row) for row in rows}
        if (len({row["event_id"] for row in rows}) != len(rows) or len(sources) != 1 or "unknown" in sources
                or max(times) - min(times) > SUSPICIOUS_WINDOW):
            raise ValueError(f"{path}: windows must have distinct events for one known source IP over at most ten minutes")
        ids.add(case["id"])
    return cases


def train_and_evaluate(train_path, eval_path, artifact):
    import joblib
    import sklearn
    from sklearn.ensemble import IsolationForest

    train, heldout = read_cases(train_path), read_cases(eval_path)
    for keys in (
        lambda case: [case["id"]],
        lambda case: [row["event_id"] for row in case["events"]],
        lambda case: [tuple(sorted((str(event_time(row)), extract_ip(row), extract_user(row), extract_outcome(row))
                                  for row in case["events"]))],
    ):
        if {key for case in train for key in keys(case)} & {key for case in heldout for key in keys(case)}:
            raise ValueError("training and evaluation share case IDs, event IDs or identical windows")
    normal = [features(case["events"]) for case in train if not case["label"]]
    if len(normal) < 20 or len({tuple(row) for row in normal}) < 5:
        raise ValueError("need at least 20 normal training windows and 5 distinct feature vectors")
    if len({case["label"] for case in heldout}) < 2:
        raise ValueError("held-out data needs positive and negative labels")
    model = IsolationForest(n_estimators=100, contamination=0.05, random_state=42, n_jobs=1)
    model.fit(normal)
    expected = [case["label"] for case in heldout]
    report = {
        "feature_version": FEATURE_VERSION, "features": FEATURES, "sklearn_version": sklearn.__version__,
        "train_sha256": hashlib.sha256(train_path.read_bytes()).hexdigest(),
        "eval_sha256": hashlib.sha256(eval_path.read_bytes()).hexdigest(),
        "training_normal_windows": len(normal), "heldout_windows": len(heldout),
        "isolation_forest": metrics(expected, [value == -1 for value in model.predict([features(case["events"]) for case in heldout])]),
        "rule_baseline": metrics(expected, [bool(replay(case["events"])) for case in heldout]),
    }
    if artifact:
        joblib.dump({"model": model, "manifest": report}, artifact)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record-lab", type=Path)
    parser.add_argument("--train", type=Path)
    parser.add_argument("--holdout", type=Path)
    parser.add_argument("--artifact", type=Path)
    args = parser.parse_args()
    if args.train or args.holdout or args.artifact:
        if not args.train or not args.holdout:
            parser.error("--train and --holdout are both required for an experimental fit")
        print(json.dumps(train_and_evaluate(args.train, args.holdout, args.artifact), indent=2))
        return
    recorded = recordings(fixture_cases())
    observed = [{finding["rule"] for finding in case["findings"]} for case in recorded]
    expected = [{case["expected_rule"]} if case["expected_rule"] else set() for case in recorded]
    if args.record_lab:
        args.record_lab.write_text(json.dumps(recorded, indent=2) + "\n")
    print(json.dumps({"dataset": "four checked-in synthetic fixtures", "exact_rule_sets": sum(a == b for a, b in zip(expected, observed)),
                      "scenarios": len(recorded), "rule_baseline": metrics([bool(x) for x in expected], [bool(x) for x in observed]),
                      "isolation_forest": "not fitted: no separate representative labeled dataset"}, indent=2))
    if expected != observed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
