# rh2

A Rust-backed, sans-IO HTTP/2 codec for Python, wrapping the
[`h2`](https://crates.io/crates/h2) crate (the HTTP/2 implementation behind
`hyper`/`reqwest`) via [PyO3](https://pyo3.rs).

The `h2` crate drives a connection over an async transport; `rh2` runs it
*sans-IO* by backing that transport with in-memory buffers and pumping the
state machine by hand. The Python API mirrors
[`hyper-h2`](https://python-hyper.org/projects/h2/):

```python
import rh2

conn = rh2.H2Connection()
events = conn.receive_data(data_from_socket)
for event in events:
    if isinstance(event, rh2.RequestReceived):
        conn.send_headers(event.stream_id, 200, [(b"content-type", b"text/plain")])
        conn.send_data(event.stream_id, b"hello", end_stream=True)
socket.sendall(conn.data_to_send())
```

## Status

Experimental. Server-side only.
