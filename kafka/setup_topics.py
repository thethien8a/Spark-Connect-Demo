"""Create missing local Kafka topics and check declared configuration without altering data."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


MANIFEST = Path(__file__).resolve().with_name("topics.json")


def load_topics(path: Path) -> list[dict]:
    def unique_object(pairs):
        record = {}
        for key, value in pairs:
            if key in record:
                raise ValueError(f"Duplicate JSON field: {key}")
            record[key] = value
        return record

    topics = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    if not isinstance(topics, list) or not topics:
        raise ValueError("Manifest must contain a non-empty list of topics")

    names = set()
    fields = {"name", "partitions", "replication_factor", "config"}
    for topic in topics:
        if not isinstance(topic, dict) or set(topic) != fields:
            raise ValueError(f"Each topic must define exactly: {', '.join(sorted(fields))}")
        name = topic["name"]
        if (
            not isinstance(name, str)
            or not re.fullmatch(r"[a-zA-Z0-9._-]{1,249}", name)
            or name in {".", ".."}
            or name.startswith("__")
        ):
            raise ValueError(f"Invalid or reserved topic name: {name!r}")
        if name in names:
            raise ValueError(f"Duplicate topic name: {name}")
        names.add(name)
        for field in ("partitions", "replication_factor"):
            if type(topic[field]) is not int or topic[field] < 1:
                raise ValueError(f"{name}: {field} must be a positive integer")
        config = topic["config"]
        if not isinstance(config, dict) or not config:
            raise ValueError(f"{name}: config must be a non-empty object")
        for key, value in config.items():
            if (
                not re.fullmatch(r"[a-zA-Z0-9._-]+", key)
                or not isinstance(value, str)
                or not value
                or value != value.strip()
            ):
                raise ValueError(f"{name}: config keys and values must be non-empty strings without surrounding whitespace")
    return topics


def run_kafka(container: str, tool: str, arguments: list[str]) -> str:
    result = subprocess.run(
        [
            "docker", "exec", container, f"/opt/kafka/bin/{tool}.sh",
            "--bootstrap-server", "localhost:9092", *arguments,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=60,
    )
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"{tool} exited {result.returncode}: {detail}")
    return result.stdout


def check_topic(container: str, topic: dict) -> list[str]:
    name = topic["name"]
    description = run_kafka(container, "kafka-topics", ["--describe", "--topic", re.escape(name)])
    summary = re.search(
        r"^Topic:\s+(\S+)\s+.*?PartitionCount:\s+(\d+)\s+ReplicationFactor:\s+(\d+)",
        description,
        re.MULTILINE,
    )
    if not summary or summary[1] != name:
        raise RuntimeError(f"{name}: cannot parse Kafka topic description")
    issues = []
    for field, actual in (("partitions", int(summary[2])), ("replication_factor", int(summary[3]))):
        if actual != topic[field]:
            issues.append(f"{name}: {field} expected {topic[field]}, found {actual}")

    # Pending removals still appear in the CLI's raw replica assignments.
    if re.search(r"\b(?:Adding|Removing) Replicas:\s+\d", description):
        issues.append(f"{name}: replica reassignment in progress; retry after it completes")
    replicas = re.findall(r"^\s+Topic:.*?\sReplicas:\s+([\d,]+)", description, re.MULTILINE)
    if len(replicas) != int(summary[2]):
        raise RuntimeError(f"{name}: cannot parse all partition replica assignments")
    for partition, brokers in enumerate(replicas):
        count = len(brokers.split(","))
        if count != topic["replication_factor"]:
            issues.append(f"{name}: partition {partition} expected {topic['replication_factor']} replicas, found {count}")

    output = run_kafka(container, "kafka-configs", [
        "--describe", "--all", "--entity-type", "topics", "--entity-name", name,
    ])
    # --all includes effective broker defaults, not just explicit topic overrides.
    config = dict(re.findall(r"^\s+([\w.-]+)=(.*?)\s+sensitive=(?:true|false)\b", output, re.MULTILINE))
    for key, expected in topic["config"].items():
        actual = config.get(key)
        if key == "cleanup.policy" and actual is not None:
            matches = set(actual.split(",")) == set(expected.split(","))
        else:
            matches = actual == expected
        if not matches:
            issues.append(f"{name}: {key} expected {expected!r}, found {actual!r}")
    return issues


def setup_topics(topics: list[dict], container: str, check_only: bool) -> int:
    existing = set(run_kafka(container, "kafka-topics", ["--list"]).splitlines())
    missing = []
    issues = []
    for topic in topics:
        name = topic["name"]
        if name not in existing:
            missing.append(topic)
            print(f"MISSING {name}")
            continue
        drift = check_topic(container, topic)
        issues.extend(drift)
        if not drift:
            print(f"OK {name}")

    # Check all existing topics before allowing any writes for missing topics.
    if issues:
        for issue in issues:
            print(f"DRIFT {issue}", file=sys.stderr)
        print("No topics were created or changed. Resolve drift explicitly before retrying.", file=sys.stderr)
        return 1
    if check_only:
        if missing:
            print(f"Check failed: {len(missing)} topic(s) missing. No changes made.", file=sys.stderr)
            return 1
        print(f"Check passed: {len(topics)} topic(s) match the manifest. No changes made.")
        return 0

    for topic in missing:
        arguments = [
            "--create", "--if-not-exists", "--topic", topic["name"],
            "--partitions", str(topic["partitions"]),
            "--replication-factor", str(topic["replication_factor"]),
        ]
        for key, value in topic["config"].items():
            arguments.extend(["--config", f"{key}={value}"])
        run_kafka(container, "kafka-topics", arguments)
        drift = check_topic(container, topic)
        if drift:
            raise RuntimeError("\n".join(drift) + "\nStopped without altering or deleting topics.")
        print(f"CREATED/VERIFIED {topic['name']}")
    print(f"Setup passed: {len(topics)} topic(s) match; {len(missing)} creation request(s).")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=MANIFEST, help="Topic manifest (default: sibling topics.json)")
    parser.add_argument("--container", default="kafka-1", help="Local broker container running Kafka CLI (default: kafka-1)")
    parser.add_argument("--check", action="store_true", help="Read-only check; fail for missing topics or configuration drift")
    args = parser.parse_args()
    try:
        topics = load_topics(args.manifest)
        return setup_topics(topics, args.container, args.check)
    except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"ERROR {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
