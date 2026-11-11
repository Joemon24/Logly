"""Measure topic routing using the checked-in, labeled synthetic questions."""

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
from statistics import median
from time import perf_counter, sleep

from api.investigator_routing import TOPICS, choose_topic, configured_provider, configured_selection

DATASET = Path(__file__).with_name("investigator_questions.json")
PROVIDERS = ("deterministic", "ollama", "groq")


def evaluate(cases, selection, *, interval):
    provider = configured_provider(selection)
    if provider is None and selection != "deterministic":
        missing = "GROQ_API_KEY" if selection == "groq" else "LOGLY_OLLAMA_MODEL"
        return {"status": "skipped", "reason": f"{missing} is unset"}
    modes = Counter()
    fallback_reasons = Counter()

    def record_fallback(reason):
        fallback_reasons[reason] += 1

    durations = []
    predictions = []
    correct = provider_correct = 0
    for row in cases:
        started = perf_counter()
        topic, mode = choose_topic(row["question"], provider, on_fallback=record_fallback)
        durations.append((perf_counter() - started) * 1000)
        predictions.append({
            "id": row["id"],
            "expected": row["topic"],
            "predicted": topic,
            "mode": mode,
        })
        matched = topic == row["topic"]
        modes[mode] += 1
        correct += matched
        provider_correct += matched and mode == selection
        if selection == "groq":
            # Pace real requests on the free tier. Exclude pacing from latency.
            sleep(max(0, interval - (perf_counter() - started)))
    total = len(cases)
    routed = modes[selection]
    fallbacks = modes["deterministic"] if provider is not None else 0
    return {
        "status": "ok" if routed else "no_provider_routes",
        "model": provider.model if provider is not None else None,
        "questions": total,
        "correct": correct,
        "accuracy": round(correct / total, 4),
        "selected_mode_count": routed,
        "selected_mode_accuracy": round(provider_correct / routed, 4) if routed else None,
        "fallback_count": fallbacks,
        "fallback_reasons": dict(sorted(fallback_reasons.items())),
        "fallback_frequency": round(fallbacks / total, 4),
        "predictions": predictions,
        "latency_ms": {
            "p50": round(median(durations), 3),
            "p95": round(sorted(durations)[math.ceil(total * 0.95) - 1], 3),
            "max": round(max(durations), 3),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", action="append", choices=PROVIDERS,
                        help="repeat to select providers; default: configured provider or all if unset")
    parser.add_argument("--groq-interval", type=float, default=2.2,
                        help="minimum seconds between Groq questions (default: 2.2)")
    args = parser.parse_args()
    if not math.isfinite(args.groq_interval) or args.groq_interval < 2:
        parser.error("--groq-interval must be at least two seconds")
    selection = configured_selection()
    if not args.provider and selection and selection not in PROVIDERS:
        parser.error("LOGLY_INVESTIGATOR_PROVIDER must be deterministic, ollama, or groq")
    selected = args.provider or ([selection] if selection else PROVIDERS)
    raw = DATASET.read_bytes()
    cases = json.loads(raw)
    if (len(cases) != 40 or len({row["id"] for row in cases}) != 40
            or len({row["question"] for row in cases}) != 40
            or {row["topic"] for row in cases} != set(TOPICS)):
        raise ValueError("invalid Investigator dataset")
    print(json.dumps({
        "dataset": DATASET.name,
        "dataset_sha256": hashlib.sha256(raw).hexdigest(),
        "label_counts": dict(Counter(row["topic"] for row in cases)),
        "providers": {
            selection: evaluate(cases, selection, interval=args.groq_interval)
            for selection in dict.fromkeys(selected)
        },
    }, indent=2))


if __name__ == "__main__":
    main()
