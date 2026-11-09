import os
import yaml

RULES_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "rules")

rules = []


def load_rules():

    global rules

    rules.clear()

    for file in sorted(os.listdir(RULES_DIR)):

        if not file.endswith(".yaml"):
            continue

        path = os.path.join(RULES_DIR, file)

        try:

            with open(path, "r") as f:

                rule = yaml.safe_load(f)

                if rule is None:

                    print(
                        f"⚠️ Empty rule skipped: {file}"
                    )

                    continue

                if not isinstance(rule, dict):

                    print(
                        f"⚠️ Invalid rule format: {file}"
                    )

                    continue

                rules.append(rule)

                print(
                    f"✅ Loaded rule: "
                    f"{rule.get('title', file)}"
                )

        except Exception as e:

            print(
                f"❌ Failed loading rule "
                f"{file}: {e}"
            )

    print(f"✅ Loaded {len(rules)} rules")


def check_condition(value, operator, expected):

    if operator == "gte":
        return value >= expected

    if operator == "gt":
        return value > expected

    if operator == "lte":
        return value <= expected

    if operator == "lt":
        return value < expected

    if operator == "eq":
        return value == expected

    return False


def evaluate_rules(features):

    matches = []

    for rule in rules:

        matched = True

        conditions = rule.get("conditions", {})

        for field, checks in conditions.items():

            feature_value = features.get(field)

            if feature_value is None:

                matched = False
                break

            for operator, expected in checks.items():

                if not check_condition(
                    feature_value,
                    operator,
                    expected
                ):

                    matched = False
                    break

        if matched:

            matches.append({

                "title":
                    rule["title"],

                "type":
                    rule["type"],

                "severity":
                    rule["severity"],

                "mitre":
                    rule["mitre"]["technique"]
            })

    priority = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    return sorted(matches, key=lambda rule: (priority.get(rule["severity"], 4), rule["type"]))
