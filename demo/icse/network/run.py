"""Run a finite local network demonstration from an exact clean source commit."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import queue
import secrets
import socket
import subprocess
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from catalog.federation.control_plane_replication import ReplicaNode

from .provenance import verify_source

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = "fcp.icse-network-demo.v1"
CAPABILITIES = {
    "voter-b": ("icse-synthetic-readings", "demo.synthetic-readings"),
    "voter-c": ("icse-reading-viewer", "demo.reading-viewer"),
}


def utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def require(condition: bool, message: str) -> None:
    # Explicit checks remain active with python -O.
    if not condition:
        raise RuntimeError(message)


def source_identity(args: argparse.Namespace) -> dict:
    require(sys.version_info[:2] == (3, 12), "use Python 3.12 for the pinned reviewer environment")
    require(args.source_archive is None or sys.dont_write_bytecode, "archive mode requires Python -B")
    identity = verify_source(
        ROOT, args.source_sha, args.release_tag,
        source_archive=args.source_archive, archive_sha256=args.archive_sha256,
    )
    return {
        **identity,
        "python_version": sys.version.split()[0],
        "physical_acceptance": "NOT_EVALUATED",
    }


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def allocate_ports() -> list[int]:
    """Check nine distinct loopback ports; startup still fails if a port is stolen."""
    reservations: list[socket.socket] = []
    ports: list[int] = []
    try:
        for _ in range(3):
            for _attempt in range(200):
                first = socket.socket()
                first.bind(("127.0.0.1", 0))
                port = int(first.getsockname()[1])
                if port > 65533:
                    first.close()
                    continue
                attempt = [first]
                try:
                    for offset in (1, 2):
                        candidate = socket.socket()
                        attempt.append(candidate)
                        candidate.bind(("127.0.0.1", port + offset))
                except OSError:
                    for candidate in attempt:
                        candidate.close()
                    continue
                reservations.extend(attempt)
                ports.append(port)
                break
            else:
                raise RuntimeError("cannot allocate three consecutive loopback port triples")
        return ports
    finally:
        for reservation in reservations:
            reservation.close()


def provision(state: Path, args: argparse.Namespace) -> dict[str, Path]:
    from catalog.federation.control_plane_product import DEPLOYMENT_SCHEMA
    from catalog.node.identity import IdentityStore

    labels = ("voter-a", "voter-b", "voter-c", "reviewer")
    nodes = {}
    for label in labels:
        directory = state / label
        directory.mkdir(mode=0o700)
        identity = directory / "identity"
        credentials = IdentityStore(identity, display_name=label).create()
        nodes[label] = {
            "label": label,
            "node_id": credentials.identity.node_id,
            "public_key": credentials.identity.public_key,
            "identity": str(identity),
            "state": str(directory),
            "source_sha": args.source_sha,
            "source_tag": args.release_tag,
            "source_archive": str(args.source_archive.resolve()) if args.source_archive else None,
            "archive_sha256": args.archive_sha256,
        }
    secret_file = state / "control-plane.secret"
    secret_file.write_bytes(secrets.token_bytes(32))
    if os.name != "nt":
        secret_file.chmod(0o600)
    ports = allocate_ports()
    cluster_id = "icse-cluster-" + uuid.uuid4().hex
    peers = [
        {
            "voter_id": nodes[label]["node_id"],
            "public_key": nodes[label]["public_key"],
            "host": "127.0.0.1",
            "port": port,
            "display_name": label,
        }
        for label, port in zip(labels[:3], ports, strict=True)
    ]
    configs = {}
    for label, node in nodes.items():
        directory = Path(node["state"])
        if label != "reviewer":
            port = next(peer["port"] for peer in peers if peer["display_name"] == label)
            deployment = directory / "deployment.json"
            write_json(deployment, {
                "schema": DEPLOYMENT_SCHEMA,
                "cluster_id": cluster_id,
                "local_voter_id": node["node_id"],
                "identity_directory": node["identity"],
                "local_display_name": label,
                "replica_database": str(directory / "replica.sqlite3"),
                "replay_database": str(directory / "replay.sqlite3"),
                "coordinator_database": str(directory / "coordinator.sqlite3"),
                "transport_secret_file": str(secret_file),
                "listen": {"host": "127.0.0.1", "port": port},
                "peers": peers,
            })
            node["deployment"] = str(deployment)
        configs[label] = directory / "worker.json"
        write_json(configs[label], node)
    return configs


class Child:
    """Bounded process handle; never searches for or terminates unrelated PIDs."""

    def __init__(self, label: str, config: Path) -> None:
        self.label = label
        self.config = config
        self.sequence = 0
        self.intentional_stop = False
        self.inbox: queue.Queue = queue.Queue()
        self.stderr = (config.parent / "worker.stderr.log").open("a", encoding="utf-8")
        try:
            self.process = subprocess.Popen(
                [sys.executable, "-B", "-u", "-m", "demo.icse.network.worker", "--config", str(config)],
                cwd=ROOT,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=self.stderr,
                text=True,
                encoding="utf-8",
                bufsize=1,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
            )
        except BaseException:
            self.stderr.close()
            raise
        threading.Thread(target=self._read, daemon=True).start()
        try:
            message = self._next(45)
            require("startup" in message, f"{label} did not return startup metadata")
            self.startup = message["startup"]
        except BaseException:
            self.stop(force=True)
            raise

    def _read(self) -> None:
        try:
            for line in self.process.stdout:
                self.inbox.put(json.loads(line))
        except (ValueError, OSError):
            self.inbox.put({"pipe_error": True})
        finally:
            self.inbox.put({"process_ended": True})

    def _next(self, timeout: float) -> dict:
        try:
            result = self.inbox.get(timeout=timeout)
        except queue.Empty as error:
            raise RuntimeError(f"{self.label} command exceeded {timeout:g} seconds") from error
        require(not result.get("process_ended"), f"{self.label} exited; inspect its private worker.stderr.log")
        require(not result.get("pipe_error"), f"{self.label} returned malformed orchestration output")
        return result

    def rpc(self, operation: str, *, expect_error: bool = False, **values):
        require(self.process.poll() is None, f"{self.label} is no longer running")
        self.sequence += 1
        request = {"id": self.sequence, "operation": operation, **values}
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        response = self._next(45)
        require(response.get("id") == self.sequence, "orchestration response identity mismatch")
        if expect_error:
            require(response.get("ok") is False, f"{operation} unexpectedly succeeded")
            return response
        require(response.get("ok") is True, f"{self.label} {operation} failed: {response.get('error_type')}/{response.get('error_code')}")
        return response["result"]

    def stop(self, *, force: bool = False) -> None:
        shutdown_failed = False
        try:
            if self.process.poll() is not None:
                require(self.intentional_stop, f"{self.label} exited unexpectedly before shutdown")
                return
            self.intentional_stop = True
            if self.process.poll() is None and not force:
                try:
                    self.rpc("shutdown")
                    self.process.wait(timeout=20)
                except (RuntimeError, OSError, subprocess.TimeoutExpired):
                    shutdown_failed = True
                    force = True
            if self.process.poll() is None and force:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=10)
            require(not shutdown_failed, f"{self.label} required forced teardown after shutdown failure")
            require(force or self.process.returncode == 0, f"{self.label} returned a nonzero shutdown result")
        finally:
            for stream in (self.process.stdin, self.process.stdout, self.stderr):
                if stream and not stream.closed:
                    stream.close()


def wait_until(predicate, description: str, *, timeout: float = 60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.5)
    raise RuntimeError(description + " did not complete within the bounded wait")


def render_report(path: Path, summary: dict) -> None:
    """An offline rendering of recorded observations, not the product UI."""
    rows = []
    for event in summary["events"]:
        detail = html.escape(json.dumps(event["observation"], indent=2, sort_keys=True))
        rows.append(
            f"<article><h2>{html.escape(event['step'])}</h2>"
            f"<p>{html.escape(event['at'])}</p><pre>{detail}</pre></article>"
        )
    path.write_text(
        "<!doctype html><html lang='en'><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>Federation local network demonstration</title>"
        "<style>body{font:16px system-ui;margin:2rem auto;max-width:72rem;padding:0 1rem;"
        "background:#f4f7fa;color:#142536}article{background:white;padding:1.2rem;"
        "margin:1rem 0;border:1px solid #cad5df;border-radius:.5rem}"
        "pre{overflow:auto;font-size:13px;line-height:1.4}h1{font-size:2rem}</style>"
        "<h1>Federation membership survives one voter loss</h1>"
        "<p>Offline demonstration report generated from observed process output. "
        "This is not the product interface or physical acceptance evidence.</p>"
        f"<p>Result: <strong>{html.escape(summary['result'])}</strong><br>"
        f"Source: <code>{html.escape(summary['source_sha'])}</code></p>"
        + "".join(rows) + "</html>",
        encoding="utf-8",
    )


def campaign(args: argparse.Namespace) -> int:
    identity = source_identity(args)
    output = args.output.resolve()
    require(not output.is_relative_to(ROOT), "output must be outside the source checkout")
    require(not output.exists(), "output directory already exists; choose a fresh directory")
    output.mkdir(mode=0o700, parents=True)
    state = output / "private-state"
    state.mkdir(mode=0o700)
    public = output / "public"
    public.mkdir()
    summary = {
        "schema": SCHEMA,
        **identity,
        "run_id": "icse-network-" + uuid.uuid4().hex,
        "started_at": utc(),
        "result": "STARTED",
        "scope": "same-host independent processes; production authority and relay; illustrative payload",
        "events": [],
        "checks": {},
    }
    children: dict[str, Child] = {}

    def observe(step: str, value: dict) -> None:
        event = {"step": step, "at": utc(), "observation": value}
        summary["events"].append(event)
        with (public / "events.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
        write_json(public / "summary.json", summary)
        render_report(public / "operator-report.html", summary)
        print(f"{event['at']} {step}", flush=True)
        if args.step_pause:
            time.sleep(args.step_pause)

    def snapshot(labels: tuple[str, ...] | list[str]) -> dict:
        return {label: children[label].rpc("status") for label in labels}

    try:
        configs = provision(state, args)
        for label, config in configs.items():
            children[label] = Child(label, config)
        ids = {label: child.startup["node_id"] for label, child in children.items()}
        require(len({child.process.pid for child in children.values()}) == 4, "nodes do not have distinct process identities")
        require(all(child.startup["source_sha"] == identity["source_sha"] for child in children.values()), "mixed source identity")
        summary["checks"]["independent_processes"] = "PASS"
        observe("01 — Four independent local node processes", {
            label: {"pid": child.process.pid, "node_id": ids[label], "kind": "member" if label == "reviewer" else "voter and relay"}
            for label, child in children.items()
        })
        federation_id = "icse-federation-" + uuid.uuid4().hex
        session_id = "icse-session-" + uuid.uuid4().hex
        creator = children["voter-a"]
        creator.rpc("bootstrap", federation_id=federation_id, session_id=session_id)
        voters = ("voter-a", "voter-b", "voter-c")

        def all_ready():
            current = snapshot(voters)
            if all(item["control_plane"]["ready"] and item["control_plane"]["federation_id"] == federation_id for item in current.values()):
                return current
            return None

        initial = wait_until(all_ready, "three-voter bootstrap")
        summary["checks"]["authenticated_quorum_bootstrap"] = "PASS"
        observe("02 — One Federation committed by the voter quorum", initial)
        relay_url = creator.startup["relay_url"]
        for label in voters:
            children[label].rpc("connect", relay_url=relay_url)
        enrollment = creator.rpc("enrollment_token")
        children["reviewer"].rpc("connect", relay_url=relay_url, enrollment_token=enrollment["token"])
        invitation = creator.rpc("invite", session_id=session_id)
        joined = children["reviewer"].rpc("join", invitation=invitation["token"])
        require(joined["session_id"] == session_id, "reviewer joined another session")
        summary["checks"]["authenticated_enrollment_and_join"] = "PASS"
        observe("03 — Reviewer enrolls and joins using one-use credentials", {
            "reviewer_node_id": ids["reviewer"], "session_id": session_id,
            "credential_values": "omitted",
        })
        for label, (capability_id, capability_type) in CAPABILITIES.items():
            children[label].rpc("announce", session_id=session_id, capability_id=capability_id, capability_type=capability_type)
        rejected = children["reviewer"].rpc(
            "announce", expect_error=True, session_id=session_id,
            capability_id="icse-forged-owner", capability_type="demo.synthetic-readings",
            owner_node_id=ids["voter-b"],
        )
        require(rejected["error_code"] == "capability-node-mismatch", "ownership rejection had an unexpected cause")
        discovery = children["reviewer"].rpc("discover")
        capabilities = {item["capability_id"]: item for item in discovery["capabilities"]}
        for label, (capability_id, _capability_type) in CAPABILITIES.items():
            require(capabilities[capability_id]["node_id"] == ids[label], "discovery returned incorrect capability owner")
        require("icse-forged-owner" not in capabilities, "rejected capability became visible")
        summary["checks"]["discovery_and_owner_authorization"] = "PASS"
        observe("04 — Distinct capability owners are discoverable; forgery is rejected", {
            "authorized_status": discovery,
            "rejection_code": rejected["error_code"],
            "provider_activation": "NOT_DEMONSTRATED; declarations do not approve execution",
        })
        dataset = json.loads(Path(__file__).with_name("machine-readings.json").read_text(encoding="utf-8"))
        digest = hashlib.sha256(json.dumps(dataset, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

        def exchange(sequence: int) -> dict:
            payload = {"kind": "icse.synthetic-readings", "exchange": sequence, "dataset": dataset}
            delivery = children["voter-b"].rpc("send", session_id=session_id, target_node_id=ids["voter-c"], payload=payload)
            received = children["voter-c"].rpc("receive")
            require(delivery.get("delivered") is True, "relay did not confirm delivery")
            require(received["actor_node_id"] == ids["voter-b"], "received payload has the wrong authenticated sender")
            require(received["session_id"] == session_id, "received payload belongs to another session")
            require(received["payload"] == payload, "received dataset differs from sent dataset")
            return {"sender": ids["voter-b"], "recipient": ids["voter-c"], "dataset": dataset, "dataset_sha256": digest, "delivery_confirmed": True}

        observe("05 — Synthetic readings cross the authenticated relay", exchange(1))
        summary["checks"]["authenticated_payload_delivery"] = "PASS"

        def fully_replicated():
            current = snapshot(voters)
            for item in current.values():
                if item["memberships"].get(session_id, {}).get(ids["reviewer"]) is not True:
                    return None
                for label, (capability_id, _capability_type) in CAPABILITIES.items():
                    if item["capabilities"].get(session_id, {}).get(capability_id, {}).get("owner_node_id") != ids[label]:
                        return None
            return current

        before = wait_until(fully_replicated, "committed membership and capability replication")
        original = before["voter-a"]["control_plane"]
        require(original["role"] == ReplicaNode.LEADER, "original voter is no longer the leader before the fault")
        require(original["consensus_leader_id"] == ids["voter-a"], "original voter no longer owns consensus leadership")
        require(original["sessions"][0]["leader_node_id"] == ids["voter-a"], "original voter no longer owns session leadership")
        old_term = original["consensus_term"]
        old_leadership_term = original["sessions"][0]["leadership_term"]
        before_commit = original["commit_index"]
        creator.stop(force=True)
        observe("06 — The original leader process has actually exited", {
            "stopped_pid": creator.process.pid, "exit_code": creator.process.returncode,
            "fault_scope": "only this demonstration child process",
        })

        def successor_ready():
            current = snapshot(("voter-b", "voter-c"))
            leaders = [label for label, item in current.items() if item["control_plane"]["role"] == ReplicaNode.LEADER]
            if len(leaders) != 1:
                return None
            label = leaders[0]
            control = current[label]["control_plane"]
            sessions = control["sessions"]
            if not control["ready"] or control["consensus_term"] <= old_term or not sessions:
                return None
            if sessions[0]["leader_node_id"] != ids[label] or sessions[0]["leadership_term"] <= old_leadership_term:
                return None
            return label, current

        successor, after = wait_until(successor_ready, "automatic surviving-quorum election")
        for item in after.values():
            control = item["control_plane"]
            require(control["federation_id"] == federation_id, "failover changed Federation identity")
            require(control["commit_index"] >= before_commit, "failover lost the committed authority prefix")
            require(control["sessions"][0]["creator_node_id"] == ids["voter-a"], "failover changed creator provenance")
            require(item["memberships"][session_id].get(ids["reviewer"]) is True, "failover lost committed reviewer membership")
            for label, (capability_id, _capability_type) in CAPABILITIES.items():
                require(item["capabilities"][session_id][capability_id]["owner_node_id"] == ids[label], "failover changed capability ownership")
        # Fencing epochs are local durable counters. The cross-voter ordering
        # comes from consensus_term, already required to advance above.
        require(after[successor]["control_plane"]["fencing_epoch"] > before[successor]["control_plane"]["fencing_epoch"], "successor did not advance its local fencing epoch")
        summary["checks"]["automatic_quorum_failover_and_continuity"] = "PASS"
        observe("07 — Surviving voters retain the Federation and advance authority", after)
        successor_url = children[successor].startup["relay_url"]
        for label in ("voter-b", "voter-c", "reviewer"):
            children[label].rpc("connect", relay_url=successor_url)
        rediscovered = children["reviewer"].rpc("discover")
        for label, (capability_id, _capability_type) in CAPABILITIES.items():
            matches = [item for item in rediscovered["capabilities"] if item["capability_id"] == capability_id]
            require(len(matches) == 1 and matches[0]["node_id"] == ids[label], "successor relay lost capability discovery")
        observe("08 — Explicit reconnect restores the same authenticated exchange", exchange(2))
        summary["checks"]["successor_reconnect_and_delivery"] = "PASS"

        # Return the old process from its actual durable files, without copying a
        # database or forcing a role, election, materialization or term.
        latest_successor = children[successor].rpc("status")["control_plane"]
        require(latest_successor["role"] == ReplicaNode.LEADER, "successor lost leadership before former voter return")
        require(latest_successor["consensus_leader_id"] == ids[successor], "successor consensus identity changed before return")
        children["voter-a"] = Child("voter-a", configs["voter-a"])
        successor_term = latest_successor["consensus_term"]

        def returned_follower():
            item = children["voter-a"].rpc("status")
            control = item["control_plane"]
            if control["role"] == ReplicaNode.FOLLOWER and control["consensus_term"] >= successor_term and control["last_applied"] >= latest_successor["commit_index"]:
                return item
            return None

        returned = wait_until(returned_follower, "returning former leader convergence")
        require(returned["control_plane"]["federation_id"] == federation_id, "returning node created another Federation")
        require(returned["control_plane"]["sessions"][0]["leader_node_id"] == ids[successor], "returning node reclaimed authority")
        summary["checks"]["returning_leader_fenced"] = "PASS"
        observe("09 — Returning former leader catches up as a follower", returned)

        # Remove both peers of the current leader. A local administrative token
        # mutation must fail through the production quorum check, not a mock.
        for label in voters:
            if label != successor:
                children[label].stop(force=True)
        minority_before = children[successor].rpc("status")
        refusal = children[successor].rpc("enrollment_token", expect_error=True)
        require(refusal["error_type"] == "QuorumUnavailable" or refusal["error_code"] == "federation-quorum-leader-required", "minority write failed for a non-quorum reason")
        minority_after = children[successor].rpc("status")
        require(minority_after["control_plane"]["commit_index"] == minority_before["control_plane"]["commit_index"], "minority mutation advanced committed authority")
        summary["checks"]["minority_mutation_refused"] = "PASS"
        observe("10 — A single remaining voter cannot authorize enrollment", {
            "error_type": refusal["error_type"], "error_code": refusal["error_code"],
            "before": minority_before["control_plane"], "after": minority_after["control_plane"],
        })
        source_identity(args)
        summary["checks"]["source_identity_unchanged"] = "PASS"
        summary["result"] = "PASS"
        return 0
    except Exception as error:  # noqa: BLE001 - retain failure evidence, fail closed, and stop every child
        summary["result"] = "FAIL"
        summary["failure"] = {"type": type(error).__name__}
        with (state / "driver-failure.log").open("a", encoding="utf-8") as stream:
            traceback.print_exc(file=stream)
        print(f"Demonstration failed: {type(error).__name__}; details retained in private-state", file=sys.stderr)
        return 1
    finally:
        shutdown_errors = []
        for label, child in reversed(tuple(children.items())):
            try:
                child.stop()
            except (RuntimeError, OSError, subprocess.SubprocessError) as error:
                shutdown_errors.append({"label": label, "error_type": type(error).__name__})
                with (state / "driver-failure.log").open("a", encoding="utf-8") as stream:
                    traceback.print_exc(file=stream)
        if shutdown_errors:
            summary["result"] = "FAIL"
            summary["shutdown_errors"] = shutdown_errors
        summary["finished_at"] = utc()
        summary["all_owned_processes_stopped"] = all(child.process.poll() is not None for child in children.values())
        if not summary["all_owned_processes_stopped"]:
            summary["result"] = "FAIL"
        write_json(public / "summary.json", summary)
        render_report(public / "operator-report.html", summary)
        print(f"NETWORK_DEMO={summary['result']} SOURCE_SHA={identity['source_sha']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True, help="exact clean checked-out source commit")
    parser.add_argument("--release-tag", help="Git tag resolving to source SHA; archive mode checks only manifest artifact source_ref")
    parser.add_argument("--source-archive", type=Path, help="original publication ZIP for an extracted source tree")
    parser.add_argument("--archive-sha256", help="independently trusted SHA-256 of the complete publication ZIP")
    parser.add_argument("--output", required=True, type=Path, help="new directory outside the source checkout")
    parser.add_argument("--step-pause", type=float, default=0, help="0–15 seconds between recorded steps for narration")
    args = parser.parse_args()
    if not 0 <= args.step_pause <= 15:
        parser.error("step pause must be between 0 and 15 seconds")
    try:
        exit_code = campaign(args)
        # A teardown failure must also cause a nonzero command result.
        result = json.loads((args.output.resolve() / "public" / "summary.json").read_text(encoding="utf-8"))
        return exit_code if result["result"] == "PASS" else 1
    except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as error:
        print(f"Demonstration preflight refused: {type(error).__name__}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
