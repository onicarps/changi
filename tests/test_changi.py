from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
import tempfile
import time
import unittest
import socket
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
COMMAND = ROOT / "changi"
sys.path.insert(0, str(ROOT))

from changi_runtime import (  # noqa: E402
    ChangiError,
    append_event,
    idle_seconds,
    ledger_status,
    process_start_identity,
    sqlite_connection,
    state_for,
    stop_workspace,
)


def unix_socket_available() -> bool:
    """Determine whether this test runner permits the required local transport."""
    with tempfile.TemporaryDirectory() as directory:
        endpoint = Path(directory) / "probe.sock"
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.bind(str(endpoint))
        except OSError:
            return False
        finally:
            probe.close()
    return True


class ChangiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.workspace = self.base / "workspace"
        self.workspace.mkdir()
        self.config = self.base / "config"
        self.environment = {**os.environ, "XDG_CONFIG_HOME": str(self.config)}
        self.socket_available = unix_socket_available()

    def tearDown(self) -> None:
        if self.socket_available:
            self.run_command("stop", check=False)
        self.temp.cleanup()

    def run_command(
        self, *arguments: str, check: bool = True, requires_socket: bool = True
    ) -> subprocess.CompletedProcess[str]:
        if requires_socket and not self.socket_available:
            self.skipTest("runner policy denies AF_UNIX bind; transport behavior requires an unsandboxed Linux runner")
        result = subprocess.run(
            [sys.executable, str(COMMAND), "--workspace", str(self.workspace), *arguments],
            env=self.environment,
            capture_output=True,
            text=True,
            timeout=15,
        )
        if check and result.returncode != 0:
            self.fail(f"command failed: {result.stderr}")
        return result

    def status(self) -> dict[str, object]:
        return json.loads(self.run_command("status", "--json").stdout)

    def test_ledger_projection_validation_without_transport(self) -> None:
        state = state_for(self.workspace)
        connection = sqlite_connection(state)
        try:
            append_event(
                connection,
                topic="attention",
                producer_id="test",
                payload={"changi": {"kind": "signal", "state": "pending"}},
                causation_id=None,
            )
            append_event(
                connection,
                topic="work.done",
                producer_id="test",
                payload={
                    "changi": {"kind": "work_receipt"},
                    "work_receipt": {
                        "executor_id": "terminal",
                        "input_hash": "abc",
                        "exit_status": 0,
                        "started_at": "2026-10-03T00:00:00Z",
                        "duration_ms": 1,
                        "receipt_id": "receipt-1",
                    },
                },
                causation_id=1,
            )
            append_event(
                connection,
                topic="work.invalid",
                producer_id="test",
                payload={"changi": {"kind": "work_receipt"}},
                causation_id=2,
            )
            health = ledger_status(connection, pid=1, started_at="now", instance_id="test")
        finally:
            connection.close()
        self.assertEqual(health["record"]["count"], 3)
        self.assertEqual(health["signal"]["pending_count"], 1)
        self.assertEqual(health["work"]["verified_count"], 1)
        self.assertEqual(stat.S_IMODE((self.workspace / ".changi").stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.workspace / ".changi" / "events.db").stat().st_mode), 0o600)

    def test_signal_counts_follow_latest_causal_state(self) -> None:
        state = state_for(self.workspace)
        connection = sqlite_connection(state)
        try:
            pending = append_event(
                connection,
                topic="attention",
                producer_id="test",
                payload={"changi": {"kind": "signal", "state": "pending"}},
                causation_id=None,
            )
            claimed = append_event(
                connection,
                topic="attention.claimed",
                producer_id="test",
                payload={"changi": {"kind": "signal", "state": "claimed"}},
                causation_id=pending["event_id"],
            )
            halfway = ledger_status(connection, pid=1, started_at="now", instance_id="test")
            self.assertEqual(halfway["signal"]["pending_count"], 0)
            self.assertEqual(halfway["signal"]["claimed_count"], 1)
            append_event(
                connection,
                topic="attention.cleared",
                producer_id="test",
                payload={"changi": {"kind": "signal", "state": "cleared"}},
                causation_id=claimed["event_id"],
            )
            status = ledger_status(connection, pid=1, started_at="now", instance_id="test")
            self.assertEqual(status["record"]["count"], 3)
            self.assertEqual(status["signal"]["pending_count"], 0)
            self.assertEqual(status["signal"]["claimed_count"], 0)
            self.assertEqual(status["signal"]["cleared_count"], 1)
            self.assertEqual(status["work"]["verified_count"], 0)
            with self.assertRaisesRegex(ChangiError, "causal signal"):
                append_event(
                    connection,
                    topic="orphan.claim",
                    producer_id="test",
                    payload={"changi": {"kind": "signal", "state": "claimed"}},
                    causation_id=None,
                )
        finally:
            connection.close()

    def test_linked_pending_keeps_one_signal_root(self) -> None:
        connection = sqlite_connection(state_for(self.workspace))
        try:
            pending = append_event(
                connection, topic="attention", producer_id="test",
                payload={"changi": {"kind": "signal", "state": "pending"}}, causation_id=None,
            )
            repeated = append_event(
                connection, topic="attention", producer_id="test",
                payload={"changi": {"kind": "signal", "state": "pending"}},
                causation_id=pending["event_id"],
            )
            status = ledger_status(connection, pid=1, started_at="now", instance_id="test")
            self.assertEqual(status["record"]["count"], 2)
            self.assertEqual(status["signal"]["pending_count"], 1)
            append_event(
                connection, topic="attention.cleared", producer_id="test",
                payload={"changi": {"kind": "signal", "state": "cleared"}},
                causation_id=repeated["event_id"],
            )
            status = ledger_status(connection, pid=1, started_at="now", instance_id="test")
            self.assertEqual(status["signal"]["pending_count"], 0)
            self.assertEqual(status["signal"]["cleared_count"], 1)
        finally:
            connection.close()

    def test_linked_pending_rejects_non_signal_parent(self) -> None:
        connection = sqlite_connection(state_for(self.workspace))
        try:
            record = append_event(
                connection, topic="note", producer_id="test",
                payload={"message": "not a signal"}, causation_id=None,
            )
            with self.assertRaisesRegex(ChangiError, "signal state changes require a causal signal event"):
                append_event(
                    connection, topic="attention", producer_id="test",
                    payload={"changi": {"kind": "signal", "state": "pending"}},
                    causation_id=record["event_id"],
                )
            status = ledger_status(connection, pid=1, started_at="now", instance_id="test")
            self.assertEqual(status["record"]["count"], 1)
            self.assertEqual(status["signal"]["pending_count"], 0)
        finally:
            connection.close()

    def test_core_envelope_and_plane_separation(self) -> None:
        self.status()
        signal_event = json.loads(
            self.run_command("emit", "attention", '{"changi":{"kind":"signal","state":"pending"}}').stdout
        )
        work_payload = json.dumps(
            {
                "changi": {"kind": "work_receipt"},
                "work_receipt": {
                    "executor_id": "terminal",
                    "input_hash": "abc",
                    "exit_status": 0,
                    "started_at": "2026-10-03T00:00:00Z",
                    "finished_at": "2026-10-03T00:00:01Z",
                    "receipt_id": "receipt-1",
                },
            }
        )
        valid_work = json.loads(self.run_command("emit", "work.done", work_payload).stdout)
        invalid_work = json.loads(self.run_command("emit", "work.malformed", '{"changi":{"kind":"work_receipt"}}').stdout)
        self.assertEqual(
            set(signal_event),
            {"event_id", "topic", "producer_id", "timestamp", "schema_version", "payload", "causation_id"},
        )
        self.assertEqual(signal_event["schema_version"], "1.0")
        self.assertIsNone(signal_event["causation_id"])
        self.assertTrue(valid_work["payload"]["changi"]["receipt_valid"])
        self.assertFalse(invalid_work["payload"]["changi"]["receipt_valid"])
        health = self.status()
        self.assertEqual(health["record"]["count"], 3)
        self.assertEqual(health["signal"]["pending_count"], 1)
        self.assertEqual(health["work"]["verified_count"], 1)
        self.assertEqual(health["work"]["in_flight_count"], 0)

    def test_runtime_modes_and_explicit_stop(self) -> None:
        health = self.status()
        state = self.workspace / ".changi"
        self.assertEqual(stat.S_IMODE(state.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((state / "events.db").stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE((state / "changi.sock").stat().st_mode), 0o600)
        self.assertTrue((state / "changi.pid").exists())
        self.run_command("stop")
        self.assertFalse((state / "changi.sock").exists())
        self.assertFalse((state / "changi.pid").exists())
        self.assertGreater(health["daemon"]["pid"], 0)

    def test_stale_state_is_reclaimed(self) -> None:
        state = self.workspace / ".changi"
        state.mkdir(mode=0o700)
        (state / "changi.pid").write_text('{"pid":999999,"start_identity":"0"}', encoding="utf-8")
        (state / "changi.sock").write_text("stale", encoding="utf-8")
        health = self.status()
        self.assertTrue((state / "changi.sock").exists())
        self.assertGreater(health["daemon"]["pid"], 0)

    def test_parallel_starts_share_one_daemon(self) -> None:
        if not self.socket_available:
            self.skipTest("runner policy denies AF_UNIX bind; transport behavior requires an unsandboxed Linux runner")
        processes = [
            subprocess.Popen(
                [sys.executable, str(COMMAND), "--workspace", str(self.workspace), "status", "--json"],
                env=self.environment,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            for _ in range(12)
        ]
        results = [process.communicate(timeout=15) for process in processes]
        self.assertTrue(all(process.returncode == 0 for process in processes))
        pids = {json.loads(stdout)["daemon"]["pid"] for stdout, _ in results}
        self.assertEqual(len(pids), 1)

    def test_clean_workspace_boundary_does_not_touch_git_files(self) -> None:
        subprocess.run(["git", "init", "-q", str(self.workspace)], check=True, capture_output=True, text=True)
        before = subprocess.run(["git", "-C", str(self.workspace), "status", "--porcelain"], check=True, capture_output=True, text=True).stdout
        self.assertEqual(before, "")
        self.status()
        after = subprocess.run(["git", "-C", str(self.workspace), "status", "--porcelain"], check=True, capture_output=True, text=True).stdout.splitlines()
        self.assertEqual(after, ["?? .changi/"])
        tracked = subprocess.run(
            ["git", "-C", str(self.workspace), "status", "--porcelain", "--untracked-files=no"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        self.assertEqual(tracked, "")

    def test_ignore_previews_require_yes_without_unprompted_mutation(self) -> None:
        subprocess.run(["git", "init", "-q", str(self.workspace)], check=True, capture_output=True, text=True)
        targets = (
            ("--git-ignore", self.workspace / ".gitignore"),
            ("--exclude", self.workspace / ".git" / "info" / "exclude"),
        )
        for option, target in targets:
            with self.subTest(option=option):
                before = target.read_bytes() if target.exists() else None
                preview = self.run_command("init", option, check=False, requires_socket=False)
                self.assertEqual(preview.returncode, 2)
                self.assertIn("--- ", preview.stdout)
                self.assertIn("+++ ", preview.stdout)
                self.assertIn("+.changi/", preview.stdout)
                self.assertIn("rerun with --yes", preview.stderr)
                self.assertEqual(target.read_bytes() if target.exists() else None, before)

                confirmed = self.run_command("init", option, "--yes", requires_socket=False)
                self.assertIn("updated", confirmed.stdout)
                self.assertIn(".changi/", target.read_text(encoding="utf-8").splitlines())
                after_confirmation = target.read_bytes()

                repeated = self.run_command("init", option, "--yes", requires_socket=False)
                self.assertIn("already contains", repeated.stdout)
                self.assertEqual(target.read_bytes(), after_confirmation)

    def test_idle_seconds_accepts_positive_float_and_rejects_invalid_values(self) -> None:
        previous = os.environ.get("CHANGI_IDLE_SECONDS")
        try:
            os.environ["CHANGI_IDLE_SECONDS"] = "0.25"
            self.assertEqual(idle_seconds(), 0.25)
            for invalid in ("0", "-1", "not-a-number"):
                with self.subTest(invalid=invalid):
                    os.environ["CHANGI_IDLE_SECONDS"] = invalid
                    with self.assertRaisesRegex(ChangiError, "positive number"):
                        idle_seconds()
        finally:
            if previous is None:
                os.environ.pop("CHANGI_IDLE_SECONDS", None)
            else:
                os.environ["CHANGI_IDLE_SECONDS"] = previous

    def test_headless_non_tty_invocation_prints_health(self) -> None:
        result = self.run_command()
        self.assertIn("monitor unavailable without a TTY", result.stderr)
        health = json.loads(result.stdout)
        self.assertGreater(health["daemon"]["pid"], 0)

    def test_identifier_sweep_has_no_legacy_runtime_name(self) -> None:
        forbidden = "".join(("ca", "irn"))
        for path in ROOT.rglob("*"):
            if path.is_file() and not {".git", "__pycache__", "node_modules"}.intersection(path.parts) and (
                path.suffix in {".py", ".js", ".md", ".json", ".yml"} or path.name in {"changi", "changid"}
            ):
                self.assertNotIn(forbidden, path.read_text(encoding="utf-8").lower(), path)

    def test_invalid_pid_record_does_not_break_stale_recovery(self) -> None:
        state = state_for(self.workspace)
        state.pid.write_text('{"pid":"not-a-number","start_identity":"123"}', encoding="utf-8")
        state.socket.write_text("stale", encoding="utf-8")
        with mock.patch.dict(os.environ, {"XDG_CONFIG_HOME": str(self.config)}):
            self.assertTrue(stop_workspace(self.workspace))
        self.assertFalse(state.socket.exists())
        self.assertFalse(state.pid.exists())

    def test_stop_refuses_to_signal_unresponsive_live_process(self) -> None:
        state = state_for(self.workspace)
        state.pid.write_text(
            json.dumps({"pid": os.getpid(), "start_identity": process_start_identity(os.getpid()), "instance_id": "foreign"}),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ChangiError, "refusing to signal"):
            stop_workspace(self.workspace)
        self.assertTrue(state.pid.exists())

    def test_version_matches_package_manifest(self) -> None:
        version = json.loads((ROOT / "package.json").read_text(encoding="utf-8"))["version"]
        output = self.run_command("--version", requires_socket=False).stdout.strip()
        self.assertEqual(output, f"changi {version}")

    def test_workspace_argument_works_before_and_after_command(self) -> None:
        subprocess.run(["git", "init", "-q", str(self.workspace)], check=True, capture_output=True)
        before = self.run_command("init", "--git-ignore", "--dry-run", requires_socket=False)
        after = self.run_command("init", "--workspace", str(self.workspace), "--git-ignore", "--dry-run", requires_socket=False)
        self.assertEqual(before.stdout, after.stdout)
        self.assertFalse((self.workspace / ".gitignore").exists())


if __name__ == "__main__":
    unittest.main()
