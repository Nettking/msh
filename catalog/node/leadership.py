"""One authenticated leadership handoff without taking over a managed socket."""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import ssl
from pathlib import Path

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from catalog.federation.errors import (
    FederationOperationError,
    FederationValidationError,
)
from catalog.federation.protocol import MAX_MESSAGE_BYTES, RelayEnvelope, utc_now
from catalog.node.client import RelayRemoteError, _validate_relay_url
from catalog.node.identity import IdentityStore
from catalog.relay.authentication import (
    authentication_message,
    leadership_transfer_command,
)


async def transfer_leadership(
    *,
    state_directory: Path | str,
    relay_url: str,
    actor_node_id: str,
    session_id: str,
    target_node_id: str,
    expected_term: int,
    request_id: str,
    timeout_seconds: float = 300,
    allow_insecure_local: bool = False,
    ssl_context: ssl.SSLContext | None = None,
) -> dict[str, object]:
    """Sign and submit once; never enroll, reconnect, retry or create identity.

    A timeout has an unknown outcome. The operator must inspect authoritative
    state before taking any further action. The expected term fences replay,
    including when the former leader later acquires leadership again.
    """
    _validate_relay_url(relay_url, allow_insecure_local=allow_insecure_local)
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 600
    ):
        raise ValueError("timeout_seconds must be finite, positive and at most 600")
    command = leadership_transfer_command({
        "session_id": session_id, "target_node_id": target_node_id,
        "expected_term": expected_term, "request_id": request_id,
    })
    credentials = IdentityStore(state_directory, display_name="Leadership operator").load()
    if credentials.identity.node_id != actor_node_id or actor_node_id == target_node_id:
        raise FederationValidationError(
            "leadership-identity-mismatch", "actor_node_id",
            "must match the retained identity and differ from the target",
        )

    async with asyncio.timeout(timeout_seconds):
        async with connect(
            relay_url, ssl=ssl_context, proxy=None,
            open_timeout=timeout_seconds, close_timeout=min(timeout_seconds, 5),
            max_size=MAX_MESSAGE_BYTES, max_queue=2,
            compression=None, ping_interval=None,
        ) as websocket:
            challenge = RelayEnvelope.from_json(await websocket.recv())
            if challenge.message_type != "auth.challenge":
                raise FederationOperationError("leadership-authentication-required", "expected relay challenge")
            nonce = challenge.payload.get("nonce")
            signed = authentication_message(
                challenge_id=challenge.request_id, nonce=nonce,
                node_id=actor_node_id, protocol_version=challenge.protocol_version,
                leadership_transfer=command,
            )
            request = RelayEnvelope(
                request_id=challenge.request_id,
                actor_node_id=actor_node_id,
                message_type="auth.response",
                authorization_context={"kind": "one-shot-leadership-transfer"},
                payload={"nonce": nonce, "signature": credentials.sign(signed),
                         "leadership_transfer": command},
                sent_at=utc_now(),
                protocol_version=challenge.protocol_version,
            )
            await websocket.send(request.to_json())
            response = RelayEnvelope.from_json(await websocket.recv())
            if response.message_type == "relay.error":
                error = response.payload.get("error", {})
                raise RelayRemoteError(
                    str(error.get("code", "leadership-transfer-rejected")),
                    "relay rejected the one-shot leadership handoff",
                )
            leadership = response.payload.get("leadership")
            if (
                response.message_type != "session.leader.transfer.accepted"
                or response.request_id != challenge.request_id
                or response.actor_node_id != challenge.actor_node_id
                or response.session_id != session_id
                or response.payload.get("request_id") != request_id
                or not isinstance(leadership, dict)
                or leadership.get("session_id") != session_id
                or leadership.get("leader_node_id") != target_node_id
                or leadership.get("term") != expected_term + 1
            ):
                raise FederationOperationError(
                    "leadership-receipt-invalid",
                    "handoff receipt did not match the explicit request; inspect authority once",
                )
            return dict(response.payload)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-directory", type=Path, required=True)
    parser.add_argument("--relay-url", required=True)
    parser.add_argument("--actor-node-id", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--target-node-id", required=True)
    parser.add_argument("--expected-term", type=int, required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    parser.add_argument("--allow-insecure-local", action="store_true")
    args = parser.parse_args(argv)
    try:
        receipt = asyncio.run(transfer_leadership(**vars(args)))
    except (FederationOperationError, FederationValidationError) as error:
        print(json.dumps({"result": "rejected-or-unconfirmed", "code": error.code,
                          "automatic_retry": False}))
        return 1
    except (OSError, TimeoutError, ConnectionClosed, ValueError) as error:
        print(json.dumps({"result": "unconfirmed", "code": type(error).__name__,
                          "automatic_retry": False,
                          "next_action": "inspect authoritative leadership once"}))
        return 1
    print(json.dumps({"result": "accepted", "receipt": receipt, "automatic_retry": False}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
