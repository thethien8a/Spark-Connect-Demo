"""Unit tests for safe topic provisioning; no Docker or Kafka required."""

import copy
import io
import json
import subprocess
import sys
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

import setup_topics


TOPIC = {
    "name": "ecommerce.public.orders",
    "partitions": 3,
    "replication_factor": 3,
    "config": {"min.insync.replicas": "2", "retention.ms": "604800000", "cleanup.policy": "delete"},
}
DESCRIPTION = """Topic: ecommerce.public.orders\tTopicId: example\tPartitionCount: 3\tReplicationFactor: 3\tConfigs:
    Topic: ecommerce.public.orders\tPartition: 0\tLeader: 1\tReplicas: 1,2,3\tIsr: 1,2,3
    Topic: ecommerce.public.orders\tPartition: 1\tLeader: 2\tReplicas: 2,3,1\tIsr: 1,2,3
    Topic: ecommerce.public.orders\tPartition: 2\tLeader: 3\tReplicas: 3,1,2\tIsr: 1,2,3
"""
CONFIG = """All configs for topic ecommerce.public.orders are:
  cleanup.policy=delete sensitive=false synonyms={DEFAULT_CONFIG:log.cleanup.policy=delete}
  min.insync.replicas=2 sensitive=false synonyms={STATIC_BROKER_CONFIG:min.insync.replicas=2}
  retention.ms=604800000 sensitive=false synonyms={}
"""


class ManifestTests(unittest.TestCase):
    def load(self, topics):
        with patch.object(Path, "read_text", return_value=json.dumps(topics)):
            return setup_topics.load_topics(Path("topics.json"))

    def test_repository_manifest_declares_five_topics(self):
        topics = setup_topics.load_topics(setup_topics.MANIFEST)
        self.assertEqual(len(topics), 5)
        self.assertEqual({topic["name"] for topic in topics}, {
            "ecommerce.public.customers", "ecommerce.public.products", "ecommerce.public.orders",
            "ecommerce.public.order_items", "ecommerce.dlq",
        })
        for topic in topics:
            with self.subTest(name=topic["name"]):
                self.assertEqual(
                    {key: value for key, value in topic.items() if key != "name"},
                    {key: value for key, value in TOPIC.items() if key != "name"},
                )

    def test_invalid_manifest_is_rejected_before_docker(self):
        invalid = []
        for field, value in (("name", "__consumer_offsets"), ("name", "orders|.*"), ("partitions", True),
                             ("replication_factor", 0), ("config", {"retention.ms": 7})):
            topic = copy.deepcopy(TOPIC)
            topic[field] = value
            invalid.append([topic])
        invalid.extend([[], {}, [TOPIC, TOPIC], [{**TOPIC, "partition": 3}]])
        for topics in invalid:
            with self.subTest(topics=topics), patch.object(setup_topics, "run_kafka") as run:
                with self.assertRaises(ValueError):
                    self.load(topics)
                run.assert_not_called()

    def test_padded_config_values_are_rejected(self):
        for value in ("604800000 ", " 604800000", "\t604800000\n"):
            topic = copy.deepcopy(TOPIC)
            topic["config"]["retention.ms"] = value
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "surrounding whitespace"):
                    self.load([topic])

    def test_duplicate_json_fields_are_rejected(self):
        for field, value in (("retention.ms", "604800000"), ("name", TOPIC["name"])):
            member = json.dumps(field) + ": " + json.dumps(value)
            duplicate = json.dumps(field) + ": " + json.dumps("1000" if field == "retention.ms" else "other")
            document = json.dumps([TOPIC]).replace(member, member + ", " + duplicate)
            with self.subTest(field=field), patch.object(Path, "read_text", return_value=document):
                with self.assertRaisesRegex(ValueError, "Duplicate JSON field"):
                    setup_topics.load_topics(Path("topics.json"))


class TopicTests(unittest.TestCase):
    def test_effective_defaults_are_accepted(self):
        with patch.object(setup_topics, "run_kafka", side_effect=[DESCRIPTION, CONFIG]) as run:
            self.assertEqual(setup_topics.check_topic("kafka-1", TOPIC), [])
            self.assertIn("--all", run.call_args.args[2])
            self.assertEqual(run.call_args_list[0].args[2][-1], r"ecommerce\.public\.orders")

    def test_config_and_topology_drift_are_reported(self):
        description = DESCRIPTION.replace("PartitionCount: 3", "PartitionCount: 4")
        description += "    Topic: ecommerce.public.orders\tPartition: 3\tLeader: 1\tReplicas: 1,2\tIsr: 1,2\n"
        config = CONFIG.replace("retention.ms=604800000", "retention.ms=1000")
        with patch.object(setup_topics, "run_kafka", side_effect=[description, config]):
            issues = setup_topics.check_topic("kafka-1", TOPIC)
        self.assertTrue(any("partitions expected 3, found 4" in issue for issue in issues))
        self.assertTrue(any("partition 3 expected 3 replicas, found 2" in issue for issue in issues))
        self.assertTrue(any("retention.ms" in issue and "1000" in issue for issue in issues))

    def test_missing_config_is_drift(self):
        with patch.object(setup_topics, "run_kafka", side_effect=[DESCRIPTION, ""]):
            self.assertEqual(len(setup_topics.check_topic("kafka-1", TOPIC)), 3)

    def test_pending_replica_changes_are_not_reported_as_matching(self):
        for field in ("Adding Replicas", "Removing Replicas"):
            description = DESCRIPTION.replace("Isr: 1,2,3", f"Isr: 1,2,3\t{field}: 3")
            with self.subTest(field=field), patch.object(setup_topics, "run_kafka", side_effect=[description, CONFIG]):
                issues = setup_topics.check_topic("kafka-1", TOPIC)
                self.assertTrue(any("reassignment in progress" in issue for issue in issues))

    def test_unparseable_description_fails_closed(self):
        for description in ("unexpected output", DESCRIPTION.replace("ecommerce.public.orders", "other"),
                            DESCRIPTION.splitlines()[0]):
            with self.subTest(description=description), patch.object(setup_topics, "run_kafka", return_value=description):
                with self.assertRaises(RuntimeError):
                    setup_topics.check_topic("kafka-1", TOPIC)

    def test_cleanup_policy_order_is_not_drift(self):
        topic = copy.deepcopy(TOPIC)
        topic["config"]["cleanup.policy"] = "compact,delete"
        with patch.object(setup_topics, "run_kafka", side_effect=[DESCRIPTION, CONFIG.replace("cleanup.policy=delete", "cleanup.policy=delete,compact")]):
            self.assertEqual(setup_topics.check_topic("kafka-1", topic), [])


class SetupTests(unittest.TestCase):
    def setUp(self):
        self.stdout = redirect_stdout(io.StringIO())
        self.stderr = redirect_stderr(io.StringIO())
        self.stdout.__enter__()
        self.stderr.__enter__()
        self.addCleanup(self.stdout.__exit__, None, None, None)
        self.addCleanup(self.stderr.__exit__, None, None, None)

    def test_check_missing_topic_never_creates(self):
        with patch.object(setup_topics, "run_kafka", return_value="") as run:
            self.assertEqual(setup_topics.setup_topics([TOPIC], "kafka-1", True), 1)
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.args[2], ["--list"])

    def test_create_missing_topic_then_verify(self):
        with patch.object(setup_topics, "run_kafka", side_effect=["", "", DESCRIPTION, CONFIG]) as run:
            self.assertEqual(setup_topics.setup_topics([TOPIC], "kafka-1", False), 0)
            arguments = run.call_args_list[1].args[2]
            self.assertIn("--if-not-exists", arguments)
            self.assertIn("--replication-factor", arguments)
            self.assertIn("retention.ms=604800000", arguments)

    def test_rerun_and_check_only_do_not_write(self):
        for check_only in (False, True):
            with self.subTest(check_only=check_only), patch.object(setup_topics, "run_kafka", side_effect=[TOPIC["name"], DESCRIPTION, CONFIG]) as run:
                self.assertEqual(setup_topics.setup_topics([TOPIC], "kafka-1", check_only), 0)
                self.assertFalse(any("--create" in call.args[2] for call in run.call_args_list))

    def test_drift_aborts_before_creating_another_missing_topic(self):
        missing = {**TOPIC, "name": "ecommerce.public.customers"}
        with patch.object(setup_topics, "run_kafka", side_effect=[TOPIC["name"], DESCRIPTION, CONFIG.replace("cleanup.policy=delete", "cleanup.policy=compact")]) as run:
            self.assertEqual(setup_topics.setup_topics([missing, TOPIC], "kafka-1", False), 1)
            self.assertEqual(run.call_count, 3)
            self.assertFalse(any("--create" in call.args[2] for call in run.call_args_list))

    def test_racing_creator_is_checked_instead_of_assumed_correct(self):
        with patch.object(setup_topics, "run_kafka", side_effect=["", "", DESCRIPTION, CONFIG.replace("retention.ms=604800000", "retention.ms=1000")]):
            with self.assertRaisesRegex(RuntimeError, "retention.ms"):
                setup_topics.setup_topics([TOPIC], "kafka-1", False)

    def test_list_failure_aborts_before_any_writes(self):
        with patch.object(setup_topics, "run_kafka", side_effect=RuntimeError("Kafka unavailable")) as run:
            with self.assertRaisesRegex(RuntimeError, "Kafka unavailable"):
                setup_topics.setup_topics([TOPIC], "kafka-1", False)
            self.assertEqual(run.call_count, 1)

    def test_existing_topic_read_failure_aborts_before_creating_missing_topic(self):
        missing = {**TOPIC, "name": "ecommerce.public.customers"}
        for responses in ([TOPIC["name"], RuntimeError("Describe failed")],
                          [TOPIC["name"], DESCRIPTION, RuntimeError("Config read failed")]):
            with self.subTest(responses=responses), patch.object(setup_topics, "run_kafka", side_effect=responses) as run:
                with self.assertRaises(RuntimeError):
                    setup_topics.setup_topics([missing, TOPIC], "kafka-1", False)
                self.assertFalse(any("--create" in call.args[2] for call in run.call_args_list))


class CommandTests(unittest.TestCase):
    def test_docker_command_is_bounded_and_not_shell_interpolated(self):
        with patch.object(subprocess, "run", return_value=Mock(returncode=0, stdout="topics", stderr="")) as run:
            self.assertEqual(setup_topics.run_kafka("kafka-2", "kafka-topics", ["--list"]), "topics")
        self.assertEqual(run.call_args.args[0], [
            "docker", "exec", "kafka-2", "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server", "localhost:9092", "--list",
        ])
        self.assertEqual(run.call_args.kwargs["timeout"], 60)
        self.assertFalse(run.call_args.kwargs.get("shell", False))

    def test_command_failure_preserves_error(self):
        with patch.object(subprocess, "run", return_value=Mock(returncode=1, stdout="", stderr="broker unavailable")):
            with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
                setup_topics.run_kafka("kafka-1", "kafka-topics", ["--list"])

    def test_cli_reports_errors_and_exits_nonzero(self):
        for error in (OSError("Docker missing"), subprocess.TimeoutExpired("docker", 60)):
            with self.subTest(error=error), patch.object(sys, "argv", ["setup_topics.py", "--check"]), \
                    patch.object(setup_topics, "load_topics", return_value=[TOPIC]), \
                    patch.object(setup_topics, "run_kafka", side_effect=error), redirect_stderr(io.StringIO()):
                self.assertEqual(setup_topics.main(), 1)


if __name__ == "__main__":
    unittest.main()
