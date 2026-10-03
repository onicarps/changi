"""Local-only Changi runtime, independent of AgentBus production code."""

from __future__ import annotations

import argparse
import contextlib
import difflib
import fcntl
import json
import os
import secrets
import selectors
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


PROTOCOL_VERSION = "1"
STATE_DIRECTORY = ".changi"
DEFAULT_IDLE_SECONDS = 4 * 60 * 60
MAX_MESSAGE_BYTES = 1024 * 1024


def package_version() -> str:
    manifest = Path(__file__).with_name("package.json")
    try:
        return str(json.loads(manifest.read_text(encoding="utf-8"))["version"])
    except (FileNotFoundError, KeyError, ValueError, OSError):
        return "development"


class ChangiError(RuntimeError):
    """An actionable, user-facing runtime error."""


@dataclass(frozen=True)
class State:
    workspace: Path
    root: Path
    database: Path
    socket: Path
    pid: Path
    lock: Path


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def canonical_workspace(value: str | Path) -> Path:
    try:
        root = Path(value).expanduser().resolve(strict=True)
    except OSError as exc:
        raise ChangiError(f"workspace is unavailable: {value}: {exc.strerror}") from exc
    if not root.is_dir():
        raise ChangiError(f"workspace is not a directory: {root}")
    return root


def state_for(workspace: str | Path, *, create: bool = True) -> State:
    root = canonical_workspace(workspace)
    state_root = root / STATE_DIRECTORY
    if state_root.exists() or state_root.is_symlink():
        info = state_root.lstat()
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise ChangiError(f"refusing non-directory runtime path: {state_root}")
    elif create:
        state_root.mkdir(mode=0o700)
    else:
        raise ChangiError(f"no Changi runtime state in {root}")
    if create:
        os.chmod(state_root, 0o700)
    return State(
        workspace=root,
        root=state_root,
        database=state_root / "events.db",
        socket=state_root / "changi.sock",
        pid=state_root / "changi.pid",
        lock=state_root / "bootstrap.lock",
    )


def assert_owned_file(path: Path) -> None:
    """Reject symlinks before writing an owned runtime file."""
    if path.exists() or path.is_symlink():
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            raise ChangiError(f"refusing symlinked runtime file: {path}")


def write_owner_file(path: Path, contents: str) -> None:
    assert_owned_file(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    flags |= getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, 0o600)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", closefd=False) as handle:
            handle.write(contents)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(fd)


def unlink_owned(path: Path) -> None:
    if path.exists() or path.is_symlink():
        path.unlink()


@contextlib.contextmanager
def bootstrap_lock(state: State) -> Iterator[None]:
    assert_owned_file(state.lock)
    fd = os.open(state.lock, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def process_start_identity(pid: int) -> str | None:
    """Return Linux's immutable process start tick, if it is available."""
    try:
        rest = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").rsplit(")", 1)[1]
        fields = rest.split()
        return fields[19]
    except (FileNotFoundError, IndexError, OSError):
        return None


def process_alive(pid: int, expected_start: str | None) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return False
    if expected_start is None:
        return False
    actual = process_start_identity(pid)
    return actual is not None and actual == str(expected_start)


def read_json_file(path: Path) -> dict[str, Any] | None:
    try:
        if path.is_symlink():
            return None
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else None
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None


def live_record_process(record: dict[str, Any] | None) -> bool:
    if not record:
        return False
    pid = record.get("pid")
    start_identity = record.get("start_identity")
    return type(pid) is int and isinstance(start_identity, str) and process_alive(pid, start_identity)


def config_directory() -> Path:
    base = Path(os.environ.get("XDG_CONFIG_HOME", str(Path.home() / ".config"))).expanduser()
    root = (base / "changi").resolve()
    if root.exists() or root.is_symlink():
        mode = root.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise ChangiError(f"refusing invalid user configuration path: {root}")
    else:
        root.mkdir(parents=True, mode=0o700)
    os.chmod(root, 0o700)
    return root


def registry_path() -> Path:
    return config_directory() / "workspaces.json"


@contextlib.contextmanager
def registry_lock() -> Iterator[None]:
    path = config_directory() / "registry.lock"
    assert_owned_file(path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.fchmod(fd, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def load_registry() -> list[dict[str, Any]]:
    path = registry_path()
    try:
        if path.is_symlink():
            raise ChangiError(f"refusing symlinked registry: {path}")
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except json.JSONDecodeError as exc:
        raise ChangiError(f"invalid workspace registry: {exc}") from exc
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise ChangiError("invalid workspace registry shape")
    return value


def write_registry(rows: list[dict[str, Any]]) -> None:
    path = registry_path()
    assert_owned_file(path)
    fd, temporary = tempfile.mkstemp(prefix="registry-", suffix=".tmp", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(rows, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def register_workspace(state: State, instance_id: str, pid: int, start_identity: str | None) -> None:
    with registry_lock():
        rows = [row for row in load_registry() if row.get("workspace") != str(state.workspace)]
        rows.append(
            {
                "workspace": str(state.workspace),
                "instance_id": instance_id,
                "pid": pid,
                "start_identity": start_identity,
                "last_seen": utc_now(),
            }
        )
        write_registry(rows)


def unregister_workspace(state: State) -> None:
    with registry_lock():
        rows = load_registry()
        retained = [row for row in rows if row.get("workspace") != str(state.workspace)]
        if retained != rows:
            write_registry(retained)


def sqlite_connection(state: State) -> sqlite3.Connection:
    assert_owned_file(state.database)
    connection = sqlite3.connect(state.database, timeout=5.0)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA busy_timeout=5000")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT,
          topic TEXT NOT NULL,
          producer_id TEXT NOT NULL,
          timestamp TEXT NOT NULL,
          schema_version TEXT NOT NULL,
          payload TEXT NOT NULL,
          causation_id INTEGER NULL
        )
        """
    )
    connection.execute("CREATE INDEX IF NOT EXISTS events_tail ON events(event_id DESC)")
    connection.commit()
    enforce_database_modes(state)
    return connection


def enforce_database_modes(state: State) -> None:
    for path in (state.database, Path(f"{state.database}-wal"), Path(f"{state.database}-shm")):
        if path.exists() and not path.is_symlink():
            os.chmod(path, 0o600)


def payload_with_receipt_diagnostic(payload: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """Validate a work receipt without treating an invalid one as Work."""
    extension = payload.get("changi")
    if not isinstance(extension, dict) or extension.get("kind") != "work_receipt":
        return payload, False
    receipt = payload.get("work_receipt")
    required = ("executor_id", "input_hash", "exit_status", "started_at")
    missing = [key for key in required if not isinstance(receipt, dict) or receipt.get(key) in (None, "")]
    has_finish = isinstance(receipt, dict) and (receipt.get("finished_at") not in (None, "") or receipt.get("duration_ms") not in (None, ""))
    has_proof = isinstance(receipt, dict) and (receipt.get("receipt_id") not in (None, "") or receipt.get("artifact_hash") not in (None, ""))
    valid = not missing and has_finish and has_proof
    normalized = json.loads(json.dumps(payload))
    normalized_extension = normalized.setdefault("changi", {})
    normalized_extension["receipt_valid"] = valid
    if not valid:
        details = missing + ([] if has_finish else ["finished_at_or_duration_ms"]) + ([] if has_proof else ["receipt_id_or_artifact_hash"])
        normalized_extension["receipt_error"] = "missing:" + ",".join(details)
    return normalized, valid


def append_event(
    connection: sqlite3.Connection,
    *,
    topic: str,
    producer_id: str,
    payload: dict[str, Any],
    causation_id: int | None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ChangiError("payload must be a JSON object")
    if not topic.strip() or not producer_id.strip():
        raise ChangiError("topic and producer_id must be non-empty")
    if causation_id is not None and causation_id <= 0:
        raise ChangiError("causation_id must be a positive event_id")
    normalized, _ = payload_with_receipt_diagnostic(payload)
    try:
        with connection:
            if causation_id is not None:
                exists = connection.execute("SELECT 1 FROM events WHERE event_id = ?", (causation_id,)).fetchone()
                if exists is None:
                    raise ChangiError(f"causation_id does not exist: {causation_id}")
            cursor = connection.execute(
                "INSERT INTO events(topic, producer_id, timestamp, schema_version, payload, causation_id) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    topic,
                    producer_id,
                    utc_now(),
                    "1.0",
                    json.dumps(normalized, sort_keys=True, separators=(",", ":")),
                    causation_id,
                ),
            )
    except sqlite3.Error as exc:
        raise ChangiError(f"record was not persisted: {exc}") from exc
    return event_by_id(connection, int(cursor.lastrowid))


def event_by_id(connection: sqlite3.Connection, event_id: int) -> dict[str, Any]:
    row = connection.execute(
        "SELECT event_id, topic, producer_id, timestamp, schema_version, payload, causation_id FROM events WHERE event_id = ?",
        (event_id,),
    ).fetchone()
    if row is None:
        raise ChangiError(f"event not found: {event_id}")
    return event_from_row(row)


def event_from_row(row: tuple[Any, ...]) -> dict[str, Any]:
    return {
        "event_id": row[0],
        "topic": row[1],
        "producer_id": row[2],
        "timestamp": row[3],
        "schema_version": row[4],
        "payload": json.loads(row[5]),
        "causation_id": row[6],
    }


def ledger_status(connection: sqlite3.Connection, *, pid: int, started_at: str, instance_id: str) -> dict[str, Any]:
    record_count = 0
    signals = {"pending": 0, "claimed": 0, "cleared": 0, "expired": 0}
    verified_work = 0
    in_flight_work = 0
    for (payload_text,) in connection.execute("SELECT payload FROM events"):
        record_count += 1
        payload = json.loads(payload_text)
        extension = payload.get("changi") if isinstance(payload, dict) else None
        if not isinstance(extension, dict):
            continue
        if extension.get("kind") == "signal":
            state = extension.get("state", "pending")
            if state in signals:
                signals[state] += 1
        if extension.get("kind") == "work_receipt" and extension.get("receipt_valid") is True:
            receipt = payload.get("work_receipt", {})
            if isinstance(receipt, dict) and receipt.get("state") == "in_flight":
                in_flight_work += 1
            else:
                verified_work += 1
    last = connection.execute("SELECT timestamp FROM events ORDER BY event_id DESC LIMIT 1").fetchone()
    return {
        "daemon": {"pid": pid, "instance_id": instance_id, "started_at": started_at, "protocol_version": PROTOCOL_VERSION},
        "record": {"count": record_count, "last_write_at": last[0] if last else None},
        "signal": {"pending_count": signals["pending"], "claimed_count": signals["claimed"], "cleared_count": signals["cleared"], "expired_count": signals["expired"]},
        "work": {"verified_count": verified_work, "in_flight_count": in_flight_work},
    }


def ledger_tail(connection: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
    if not 1 <= limit <= 500:
        raise ChangiError("log limit must be between 1 and 500")
    rows = connection.execute(
        "SELECT event_id, topic, producer_id, timestamp, schema_version, payload, causation_id FROM events ORDER BY event_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [event_from_row(row) for row in rows]


def encode_message(message: dict[str, Any]) -> bytes:
    encoded = json.dumps(message, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    if len(encoded) > MAX_MESSAGE_BYTES:
        raise ChangiError("control message exceeds the local protocol limit")
    return encoded


def decode_message(data: bytes) -> dict[str, Any]:
    if len(data) > MAX_MESSAGE_BYTES:
        raise ChangiError("control message exceeds the local protocol limit")
    try:
        decoded = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ChangiError("invalid local control message") from exc
    if not isinstance(decoded, dict):
        raise ChangiError("control message must be an object")
    return decoded


def socket_request(state: State, request: dict[str, Any], *, timeout: float = 0.75) -> dict[str, Any]:
    if state.socket.is_symlink():
        raise ChangiError("refusing symlinked control socket")
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        client.settimeout(timeout)
        client.connect(str(state.socket))
        client.sendall(encode_message({"protocol_version": PROTOCOL_VERSION, **request}))
        chunks = bytearray()
        while b"\n" not in chunks:
            part = client.recv(65536)
            if not part:
                break
            chunks.extend(part)
            if len(chunks) > MAX_MESSAGE_BYTES:
                raise ChangiError("local control response exceeds the protocol limit")
        response = decode_message(bytes(chunks.split(b"\n", 1)[0]))
    except (FileNotFoundError, ConnectionRefusedError, socket.timeout, OSError) as exc:
        raise ChangiError(f"daemon control connection failed: {exc}") from exc
    finally:
        client.close()
    if response.get("ok") is not True:
        raise ChangiError(str(response.get("error", "daemon rejected request")))
    return response


def valid_health(state: State) -> dict[str, Any] | None:
    try:
        health = socket_request(state, {"operation": "health"})
    except ChangiError:
        return None
    record = read_json_file(state.pid)
    if not live_record_process(record):
        return None
    if (
        health.get("pid") != record.get("pid")
        or str(health.get("start_identity")) != str(record.get("start_identity"))
        or health.get("instance_id") != record.get("instance_id")
        or health.get("protocol_version") != PROTOCOL_VERSION
    ):
        return None
    return health


def reclaim_stale_state(state: State) -> None:
    record = read_json_file(state.pid)
    if live_record_process(record):
        raise ChangiError("daemon is live but not responding; refusing to reclaim its state")
    unlink_owned(state.socket)
    unlink_owned(state.pid)


def daemon_script() -> Path:
    return Path(__file__).with_name("changid")


def ensure_daemon(workspace: str | Path) -> tuple[State, dict[str, Any]]:
    state = state_for(workspace)
    with bootstrap_lock(state):
        health = valid_health(state)
        if health is not None:
            return state, health
        reclaim_stale_state(state)
        subprocess.Popen(
            [sys.executable, str(daemon_script()), "--workspace", str(state.workspace)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            health = valid_health(state)
            if health is not None:
                return state, health
            time.sleep(0.05)
        reclaim_stale_state(state)
        raise ChangiError("daemon did not become healthy within five seconds")


def git_advisory(workspace: Path) -> None:
    try:
        inside = subprocess.run(
            ["git", "-C", str(workspace), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            text=True,
            timeout=1,
            check=False,
        )
        if inside.returncode != 0 or inside.stdout.strip() != "true":
            return
        ignored = subprocess.run(
            ["git", "-C", str(workspace), "check-ignore", "-q", STATE_DIRECTORY + "/"],
            capture_output=True,
            text=True,
            timeout=1,
            check=False,
        )
        if ignored.returncode != 0:
            print(
                "changi: notice: '.changi/' is not git-ignored; use 'changi init --git-ignore' to opt in.",
                file=sys.stderr,
            )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return


class Daemon:
    def __init__(self, workspace: str | Path) -> None:
        self.state = state_for(workspace)
        self.pid = os.getpid()
        self.start_identity = process_start_identity(self.pid)
        self.instance_id = uuid.uuid4().hex
        self.started_at = utc_now()
        self.last_activity = time.monotonic()
        self.idle_seconds = idle_seconds()
        self.connection = sqlite_connection(self.state)
        self.listener: socket.socket | None = None
        self.stopping = False

    def start(self) -> None:
        if self.state.socket.exists() or self.state.socket.is_symlink():
            raise ChangiError("control socket already exists")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self.state.socket))
        except OSError as exc:
            listener.close()
            raise ChangiError(f"cannot create workspace-local control socket: {exc}") from exc
        os.chmod(self.state.socket, 0o600)
        listener.listen(32)
        listener.setblocking(False)
        self.listener = listener
        write_owner_file(
            self.state.pid,
            json.dumps({"pid": self.pid, "start_identity": self.start_identity, "instance_id": self.instance_id}, sort_keys=True),
        )
        register_workspace(self.state, self.instance_id, self.pid, self.start_identity)

    def response_for(self, request: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        if request.get("protocol_version") != PROTOCOL_VERSION:
            return {"ok": False, "error": "unsupported protocol version"}, False
        operation = request.get("operation")
        if operation == "health":
            self.last_activity = time.monotonic()
            return {
                "ok": True,
                "pid": self.pid,
                "start_identity": self.start_identity,
                "instance_id": self.instance_id,
                "protocol_version": PROTOCOL_VERSION,
            }, False
        if operation == "status":
            self.last_activity = time.monotonic()
            return {"ok": True, "status": ledger_status(self.connection, pid=self.pid, started_at=self.started_at, instance_id=self.instance_id)}, False
        if operation == "log":
            self.last_activity = time.monotonic()
            return {"ok": True, "events": ledger_tail(self.connection, int(request.get("limit", 50)))}, False
        if operation == "emit":
            payload = request.get("payload")
            event = append_event(
                self.connection,
                topic=str(request.get("topic", "")),
                producer_id=str(request.get("producer_id", "")),
                payload=payload if isinstance(payload, dict) else {},
                causation_id=request.get("causation_id"),
            )
            self.last_activity = time.monotonic()
            enforce_database_modes(self.state)
            return {"ok": True, "event": event}, False
        if operation == "stop":
            self.last_activity = time.monotonic()
            return {"ok": True, "stopping": True}, True
        return {"ok": False, "error": "unknown operation"}, False

    def handle_connection(self, client: socket.socket) -> None:
        try:
            client.settimeout(0.5)
            data = bytearray()
            while b"\n" not in data:
                chunk = client.recv(65536)
                if not chunk:
                    return
                data.extend(chunk)
                if len(data) > MAX_MESSAGE_BYTES:
                    raise ChangiError("control message exceeds the local protocol limit")
            response, stop = self.response_for(decode_message(bytes(data.split(b"\n", 1)[0])))
            client.sendall(encode_message(response))
            self.stopping = self.stopping or stop
        except (ChangiError, sqlite3.Error, ValueError) as exc:
            try:
                client.sendall(encode_message({"ok": False, "error": str(exc)}))
            except OSError:
                pass
        finally:
            client.close()

    def serve(self) -> None:
        self.start()
        assert self.listener is not None
        selector = selectors.DefaultSelector()
        selector.register(self.listener, selectors.EVENT_READ)
        try:
            while not self.stopping:
                if time.monotonic() - self.last_activity >= self.idle_seconds:
                    self.stopping = True
                    continue
                for _key, _mask in selector.select(timeout=0.2):
                    client, _ = self.listener.accept()
                    self.handle_connection(client)
        finally:
            selector.close()
            self.close()

    def close(self) -> None:
        if self.listener is not None:
            self.listener.close()
        self.connection.close()
        unlink_owned(self.state.socket)
        record = read_json_file(self.state.pid)
        if record and record.get("pid") == self.pid and str(record.get("start_identity")) == str(self.start_identity):
            unlink_owned(self.state.pid)
        unregister_workspace(self.state)


def idle_seconds() -> float:
    raw = os.environ.get("CHANGI_IDLE_SECONDS")
    if raw is None:
        return float(DEFAULT_IDLE_SECONDS)
    try:
        value = float(raw)
    except ValueError as exc:
        raise ChangiError("CHANGI_IDLE_SECONDS must be a positive number") from exc
    if value <= 0:
        raise ChangiError("CHANGI_IDLE_SECONDS must be a positive number")
    return value


def daemon_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="changid", description="Changi workspace-local passive daemon")
    parser.add_argument("--version", action="version", version=f"changid {package_version()}")
    parser.add_argument("--workspace", default=os.getcwd())
    args = parser.parse_args(argv)
    try:
        daemon = Daemon(args.workspace)
        previous_term = signal.signal(signal.SIGTERM, lambda _number, _frame: setattr(daemon, "stopping", True))
        previous_int = signal.signal(signal.SIGINT, lambda _number, _frame: setattr(daemon, "stopping", True))
        try:
            daemon.serve()
        finally:
            signal.signal(signal.SIGTERM, previous_term)
            signal.signal(signal.SIGINT, previous_int)
    except (ChangiError, OSError) as exc:
        print(f"changid: error: {exc}", file=sys.stderr)
        return 2
    return 0


def parse_payload(value: str | None) -> dict[str, Any]:
    if value is None:
        return {}
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise ChangiError(f"payload is not valid JSON: {exc}") from exc
    if not isinstance(decoded, dict):
        raise ChangiError("payload must be a JSON object")
    return decoded


def stop_workspace(workspace: str | Path) -> bool:
    try:
        state = state_for(workspace, create=False)
    except ChangiError:
        return False
    health = valid_health(state)
    if health is not None:
        socket_request(state, {"operation": "stop"})
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if not state.socket.exists():
                return True
            time.sleep(0.05)
        raise ChangiError("daemon did not stop within five seconds")
    record = read_json_file(state.pid)
    if live_record_process(record):
        raise ChangiError("a process named in the PID file is live, but its Changi socket is unresponsive; refusing to signal it")
    reclaim_stale_state(state)
    unregister_workspace(state)
    return True


def git_root(workspace: Path) -> Path:
    result = subprocess.run(
        ["git", "-C", str(workspace), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        timeout=1,
        check=False,
    )
    if result.returncode != 0:
        raise ChangiError("workspace is not a Git worktree")
    return Path(result.stdout.strip()).resolve()


def init_ignore(workspace: Path, target: str, *, yes: bool, dry_run: bool) -> None:
    root = git_root(workspace)
    if target == "git-ignore":
        path = root / ".gitignore"
    else:
        result = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--git-path", "info/exclude"],
            capture_output=True,
            text=True,
            timeout=1,
            check=False,
        )
        if result.returncode != 0:
            raise ChangiError("could not locate Git exclude file")
        candidate = Path(result.stdout.strip())
        path = candidate if candidate.is_absolute() else root / candidate
    before = path.read_text(encoding="utf-8") if path.exists() else ""
    if STATE_DIRECTORY + "/" in before.splitlines() or STATE_DIRECTORY in before.splitlines():
        print(f"changi: {path} already contains {STATE_DIRECTORY}/")
        return
    suffix = "" if not before or before.endswith("\n") else "\n"
    after = before + suffix + STATE_DIRECTORY + "/\n"
    diff = "".join(difflib.unified_diff(before.splitlines(keepends=True), after.splitlines(keepends=True), fromfile=str(path), tofile=str(path)))
    print(diff, end="")
    if dry_run:
        return
    if not yes:
        raise ChangiError("review the diff, then rerun with --yes to make this explicit change")
    path.parent.mkdir(parents=True, exist_ok=True)
    write_owner_file(path, after)
    print(f"changi: updated {path}")


def monitor(state: State) -> int:
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        response = socket_request(state, {"operation": "status"})
        print(json.dumps(response["status"], sort_keys=True))
        print("changi: monitor unavailable without a TTY; printed status instead.", file=sys.stderr)
        return 0
    try:
        print("\x1b[?1049h\x1b[2J\x1b[H", end="", flush=True)
        while True:
            response = socket_request(state, {"operation": "status"})
            status_payload = response["status"]
            print("\x1b[H\x1b[2J", end="")
            print(f"[Changi] Workspace: {state.workspace}")
            print("Record", json.dumps(status_payload["record"], sort_keys=True))
            print("Signal", json.dumps(status_payload["signal"], sort_keys=True))
            print("Work  ", json.dumps(status_payload["work"], sort_keys=True))
            print("\nq or Ctrl-C: detach (daemon remains active)", flush=True)
            ready, _, _ = select_with_timeout(1.0)
            if ready and sys.stdin.read(1).lower() == "q":
                break
    except KeyboardInterrupt:
        pass
    finally:
        print("\x1b[?1049l", end="", flush=True)
    print("changi: detached. Background daemon remains active.", file=sys.stderr)
    return 0


def select_with_timeout(timeout: float) -> tuple[list[Any], list[Any], list[Any]]:
    import select

    return select.select([sys.stdin], [], [], timeout)


def cli_main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="changi", description="Changi local-first terminal event companion")
    parser.add_argument("--version", action="version", version=f"changi {package_version()}")
    parser.add_argument("--workspace", default=os.getcwd(), help="workspace root (default: current directory)")
    common_parser = argparse.ArgumentParser(add_help=False)
    common_parser.add_argument("--workspace", default=argparse.SUPPRESS, help="workspace root")
    subparsers = parser.add_subparsers(dest="command")
    status_parser = subparsers.add_parser("status", parents=[common_parser], help="show daemon and plane health")
    status_parser.add_argument("--json", action="store_true", help="emit machine-readable health")
    log_parser = subparsers.add_parser("log", parents=[common_parser], help="show newest-first Record entries")
    log_parser.add_argument("--limit", type=int, default=50)
    emit_parser = subparsers.add_parser("emit", parents=[common_parser], help="append one locally validated Record")
    emit_parser.add_argument("topic")
    emit_parser.add_argument("payload", nargs="?")
    emit_parser.add_argument("--producer-id", default="user")
    emit_parser.add_argument("--causation-id", type=int)
    stop_parser = subparsers.add_parser("stop", parents=[common_parser], help="stop a workspace-local daemon")
    stop_parser.add_argument("--all", action="store_true")
    stop_parser.add_argument("--yes", action="store_true")
    subparsers.add_parser("list-all", parents=[common_parser], help="list registered workspaces")
    init_parser = subparsers.add_parser("init", parents=[common_parser], help="explicitly configure a Git ignore rule")
    init_target = init_parser.add_mutually_exclusive_group(required=True)
    init_target.add_argument("--git-ignore", action="store_true")
    init_target.add_argument("--exclude", action="store_true")
    init_parser.add_argument("--yes", action="store_true")
    init_parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    try:
        workspace = canonical_workspace(args.workspace)
        if args.command == "list-all":
            print(json.dumps(load_registry(), sort_keys=True))
            return 0
        if args.command == "init":
            init_ignore(workspace, "git-ignore" if args.git_ignore else "exclude", yes=args.yes, dry_run=args.dry_run)
            return 0
        if args.command == "stop":
            if args.all:
                rows = load_registry()
                targets = [str(row["workspace"]) for row in rows if isinstance(row.get("workspace"), str)]
                if not args.yes:
                    print(json.dumps({"workspaces": targets}, sort_keys=True))
                    raise ChangiError("rerun with --yes to stop all listed workspace daemons")
                for target in targets:
                    stop_workspace(target)
                return 0
            return 0 if stop_workspace(workspace) else 1
        state, _ = ensure_daemon(workspace)
        git_advisory(workspace)
        if args.command == "status":
            response = socket_request(state, {"operation": "status"})
            if args.json:
                print(json.dumps(response["status"], sort_keys=True))
            else:
                health = response["status"]
                print(
                    f"daemon pid={health['daemon']['pid']} record={health['record']['count']} "
                    f"signal_pending={health['signal']['pending_count']} work_verified={health['work']['verified_count']}"
                )
            return 0
        if args.command == "log":
            response = socket_request(state, {"operation": "log", "limit": args.limit})
            for event in response["events"]:
                print(json.dumps(event, sort_keys=True))
            return 0
        if args.command == "emit":
            response = socket_request(
                state,
                {
                    "operation": "emit",
                    "topic": args.topic,
                    "payload": parse_payload(args.payload),
                    "producer_id": args.producer_id,
                    "causation_id": args.causation_id,
                },
            )
            print(json.dumps(response["event"], sort_keys=True))
            return 0
        return monitor(state)
    except ChangiError as exc:
        print(f"changi: error: {exc}", file=sys.stderr)
        return 2
