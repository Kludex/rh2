"""Drive the rh2 server codec with hyper-h2 acting as the client."""

from __future__ import annotations

import h2.config
import h2.connection
import h2.events

import rh2


def _pump(server: rh2.H2Connection, client: h2.connection.H2Connection) -> list:
    """Ferry bytes both ways until neither side has more to send."""
    events: list = []
    for _ in range(20):
        moved = False
        client_out = client.data_to_send()
        if client_out:
            events += server.receive_data(client_out)
            moved = True
        server_out = server.data_to_send()
        if server_out:
            events += client.receive_data(server_out)
            moved = True
        if not moved:
            break
    return events


def test_request_response_roundtrip() -> None:
    server = rh2.H2Connection()
    client = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
    client.initiate_connection()

    stream_id = client.get_next_available_stream_id()
    client.send_headers(
        stream_id,
        [
            (b":method", b"GET"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/hello"),
        ],
        end_stream=True,
    )

    events = _pump(server, client)
    reqs = [e for e in events if isinstance(e, rh2.RequestReceived)]
    assert len(reqs) == 1
    req = reqs[0]
    assert req.stream_id == stream_id
    assert req.stream_ended is True
    header_map = {bytes(k): bytes(v) for k, v in req.headers}
    assert header_map[b":method"] == b"GET"
    assert header_map[b":path"] == b"/hello"

    server.send_headers(stream_id, 200, [(b"content-type", b"text/plain")], end_stream=False)
    server.send_data(stream_id, b"hello world", end_stream=True)

    client_events = _pump(server, client)
    got_response = any(isinstance(e, h2.events.ResponseReceived) for e in client_events)
    got_data = b"".join(
        e.data for e in client_events if isinstance(e, h2.events.DataReceived)
    )
    assert got_response
    assert got_data == b"hello world"


def test_request_body_flow_control() -> None:
    server = rh2.H2Connection()
    client = h2.connection.H2Connection(config=h2.config.H2Configuration(client_side=True))
    client.initiate_connection()

    stream_id = client.get_next_available_stream_id()
    client.send_headers(
        stream_id,
        [
            (b":method", b"POST"),
            (b":scheme", b"http"),
            (b":authority", b"example.com"),
            (b":path", b"/upload"),
        ],
    )
    body = b"x" * (100 * 1024)  # larger than the default 64 KiB window
    # Send within the client's available window, then rely on WINDOW_UPDATE.
    offset = 0
    events: list = []
    while offset < len(body):
        window = client.local_flow_control_window(stream_id)
        if window == 0:
            events += _pump(server, client)
            continue
        chunk = body[offset : offset + min(window, 16384)]
        client.send_data(stream_id, chunk)
        offset += len(chunk)
    client.end_stream(stream_id)
    events += _pump(server, client)

    received = b"".join(
        bytes(e.data) for e in events if isinstance(e, rh2.DataReceived)
    )
    assert received == body
    assert any(isinstance(e, rh2.StreamEnded) for e in events)
