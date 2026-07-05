"""Rust-backed sans-IO HTTP/2 codec, wrapping the `h2` crate.

The API is deliberately close to `hyper-h2`: create a :class:`H2Connection`, feed
it bytes from the socket with :meth:`H2Connection.receive_data`, act on the
returned events, and write whatever :meth:`H2Connection.data_to_send` returns
back to the socket.
"""

from __future__ import annotations

from rh2._rh2 import (
    ConnectionTerminated,
    DataReceived,
    H2Connection,
    RequestReceived,
    StreamEnded,
    StreamReset,
)

__all__ = [
    "ConnectionTerminated",
    "DataReceived",
    "H2Connection",
    "RequestReceived",
    "StreamEnded",
    "StreamReset",
]
