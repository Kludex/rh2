//! Sans-IO HTTP/2 codec for Python, wrapping the async `h2` crate.
//!
//! The `h2` crate drives an HTTP/2 connection over a `tokio` `AsyncRead + AsyncWrite`
//! transport. We want a *sans-IO* codec instead: Python feeds us the bytes it read
//! from the socket (`receive_data`) and drains the bytes we want to write
//! (`data_to_send`), and we surface protocol events. To bridge the two we back the
//! transport with in-memory buffers and drive `h2`'s futures by hand with a no-op
//! waker, pumping them to `Pending` after every interaction.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use bytes::Bytes;
use h2::server::{self, SendResponse};
use h2::{RecvStream, SendStream};
use http::{HeaderName, HeaderValue, Response, StatusCode};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// --- No-op waker -----------------------------------------------------------
// We poll manually and re-poll after feeding input, so wakeups are irrelevant.

fn noop_waker() -> Waker {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

// --- In-memory duplex transport -------------------------------------------

#[derive(Default)]
struct IoInner {
    // Contiguous buffer + read cursor so poll_read can bulk-copy instead of
    // popping a byte at a time.
    inbound: Vec<u8>,
    inbound_pos: usize,
    outbound: Vec<u8>,
    inbound_closed: bool,
}

impl IoInner {
    fn push_inbound(&mut self, data: &[u8]) {
        // Drop already-consumed bytes before growing, keeping the buffer small.
        if self.inbound_pos > 0 {
            self.inbound.drain(0..self.inbound_pos);
            self.inbound_pos = 0;
        }
        self.inbound.extend_from_slice(data);
    }
}

#[derive(Clone)]
struct SharedIo(Rc<RefCell<IoInner>>);

impl SharedIo {
    fn new() -> Self {
        SharedIo(Rc::new(RefCell::new(IoInner::default())))
    }
}

impl AsyncRead for SharedIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut inner = self.0.borrow_mut();
        let avail = inner.inbound.len() - inner.inbound_pos;
        if avail == 0 {
            // EOF once the peer half-closed and we've drained everything.
            if inner.inbound_closed {
                return Poll::Ready(Ok(()));
            }
            return Poll::Pending;
        }
        let n = std::cmp::min(buf.remaining(), avail);
        let start = inner.inbound_pos;
        buf.put_slice(&inner.inbound[start..start + n]);
        inner.inbound_pos += n;
        if inner.inbound_pos == inner.inbound.len() {
            inner.inbound.clear();
            inner.inbound_pos = 0;
        }
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for SharedIo {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.0.borrow_mut().outbound.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

// --- Events ----------------------------------------------------------------

#[pyclass(module = "rh2._rh2")]
struct RequestReceived {
    #[pyo3(get)]
    stream_id: u32,
    headers: Py<PyList>,
    #[pyo3(get)]
    stream_ended: bool,
}

#[pymethods]
impl RequestReceived {
    #[getter]
    fn headers(&self, py: Python<'_>) -> Py<PyList> {
        self.headers.clone_ref(py)
    }

    fn __repr__(&self) -> String {
        format!("RequestReceived(stream_id={}, stream_ended={})", self.stream_id, self.stream_ended)
    }
}

#[pyclass(module = "rh2._rh2")]
struct DataReceived {
    #[pyo3(get)]
    stream_id: u32,
    data: Py<PyBytes>,
    #[pyo3(get)]
    stream_ended: bool,
}

#[pymethods]
impl DataReceived {
    #[getter]
    fn data(&self, py: Python<'_>) -> Py<PyBytes> {
        self.data.clone_ref(py)
    }

    fn __repr__(&self) -> String {
        format!("DataReceived(stream_id={})", self.stream_id)
    }
}

#[pyclass(module = "rh2._rh2")]
struct StreamEnded {
    #[pyo3(get)]
    stream_id: u32,
}

#[pymethods]
impl StreamEnded {
    fn __repr__(&self) -> String {
        format!("StreamEnded(stream_id={})", self.stream_id)
    }
}

#[pyclass(module = "rh2._rh2")]
struct StreamReset {
    #[pyo3(get)]
    stream_id: u32,
    #[pyo3(get)]
    error_code: u32,
}

#[pymethods]
impl StreamReset {
    fn __repr__(&self) -> String {
        format!("StreamReset(stream_id={}, error_code={})", self.stream_id, self.error_code)
    }
}

#[pyclass(module = "rh2._rh2")]
struct ConnectionTerminated {}

#[pymethods]
impl ConnectionTerminated {
    fn __repr__(&self) -> String {
        "ConnectionTerminated()".to_string()
    }
}

// --- Connection ------------------------------------------------------------

type HandshakeFut =
    Pin<Box<dyn Future<Output = Result<server::Connection<SharedIo, Bytes>, h2::Error>>>>;

enum State {
    Handshaking(HandshakeFut),
    Ready(server::Connection<SharedIo, Bytes>),
    Closed,
}

#[pyclass(module = "rh2._rh2", unsendable)]
struct H2Connection {
    io: SharedIo,
    state: State,
    recv_streams: HashMap<u32, RecvStream>,
    responders: HashMap<u32, SendResponse<Bytes>>,
    send_streams: HashMap<u32, SendStream<Bytes>>,
    // Body bytes queued by the app, flushed as the peer's flow-control window allows.
    pending_send: HashMap<u32, VecDeque<(Bytes, bool)>>,
    events: VecDeque<PyObject>,
}

impl H2Connection {
    fn pump(&mut self, py: Python<'_>) -> PyResult<()> {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        // 1. Complete the handshake if we're still waiting on the client preface.
        if let State::Handshaking(fut) = &mut self.state {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(conn)) => self.state = State::Ready(conn),
                Poll::Ready(Err(e)) => {
                    self.state = State::Closed;
                    return Err(PyValueError::new_err(format!("handshake failed: {e}")));
                }
                Poll::Pending => return Ok(()),
            }
        }

        if !matches!(self.state, State::Ready(_)) {
            return Ok(());
        }

        // Drive the connection until it stops making progress. `h2` only writes
        // buffered frames to the transport while the connection is being polled,
        // so a final no-progress pass is what flushes the last response bytes.
        for _ in 0..64 {
            let mut progress = self.drive(py, &mut cx)?;
            if self.flush_pending(&mut cx) {
                progress = true;
            }
            if !progress {
                break;
            }
        }
        Ok(())
    }

    fn drive(&mut self, py: Python<'_>, cx: &mut Context<'_>) -> PyResult<bool> {
        // Progress is measured by whether the drive surfaced any new events;
        // capacity-only advances are handled by `flush_pending` in the caller.
        let before = self.events.len();
        let conn = match &mut self.state {
            State::Ready(conn) => conn,
            _ => return Ok(false),
        };

        // 2. Accept any newly-arrived streams.
        loop {
            match conn.poll_accept(cx) {
                Poll::Ready(Some(Ok((request, respond)))) => {
                    let stream_id = respond.stream_id().as_u32();
                    let (parts, recv) = request.into_parts();

                    // Reconstruct the HTTP/2 pseudo-headers the app expects, then
                    // the regular headers, as a list of (name, value) byte tuples.
                    let headers = PyList::empty_bound(py);
                    let mut push = |name: &[u8], value: &[u8]| {
                        let pair = (PyBytes::new_bound(py, name), PyBytes::new_bound(py, value));
                        headers.append(pair).unwrap();
                    };
                    push(b":method", parts.method.as_str().as_bytes());
                    if let Some(scheme) = parts.uri.scheme_str() {
                        push(b":scheme", scheme.as_bytes());
                    }
                    if let Some(authority) = parts.uri.authority() {
                        push(b":authority", authority.as_str().as_bytes());
                    }
                    let path = parts
                        .uri
                        .path_and_query()
                        .map(|pq| pq.as_str())
                        .unwrap_or("/");
                    push(b":path", path.as_bytes());
                    for (name, value) in parts.headers.iter() {
                        push(name.as_str().as_bytes(), value.as_bytes());
                    }
                    let headers: Py<PyList> = headers.into();

                    let stream_ended = recv.is_end_stream();
                    self.recv_streams.insert(stream_id, recv);
                    self.responders.insert(stream_id, respond);
                    let event = RequestReceived { stream_id, headers, stream_ended };
                    self.events.push_back(event.into_py(py));
                }
                Poll::Ready(Some(Err(e))) => {
                    self.state = State::Closed;
                    let event = ConnectionTerminated {};
                    self.events.push_back(event.into_py(py));
                    let _ = e;
                    return Ok(self.events.len() != before);
                }
                Poll::Ready(None) => {
                    let event = ConnectionTerminated {};
                    self.events.push_back(event.into_py(py));
                    break;
                }
                Poll::Pending => break,
            }
        }

        // 3. Read body data from each open receive stream.
        let ids: Vec<u32> = self.recv_streams.keys().copied().collect();
        for stream_id in ids {
            loop {
                let recv = match self.recv_streams.get_mut(&stream_id) {
                    Some(r) => r,
                    None => break,
                };
                match recv.poll_data(cx) {
                    Poll::Ready(Some(Ok(chunk))) => {
                        // Immediately release the connection-level window; the app
                        // acknowledges stream-level capacity via `acknowledge_data`.
                        let _ = recv.flow_control().release_capacity(chunk.len());
                        let stream_ended = recv.is_end_stream();
                        let event = DataReceived {
                            stream_id,
                            data: PyBytes::new_bound(py, &chunk).into(),
                            stream_ended,
                        };
                        self.events.push_back(event.into_py(py));
                    }
                    Poll::Ready(Some(Err(e))) => {
                        let error_code =
                            e.reason().map(|r| u32::from(r)).unwrap_or(0);
                        self.recv_streams.remove(&stream_id);
                        let event = StreamReset { stream_id, error_code };
                        self.events.push_back(event.into_py(py));
                        break;
                    }
                    Poll::Ready(None) => {
                        self.recv_streams.remove(&stream_id);
                        let event = StreamEnded { stream_id };
                        self.events.push_back(event.into_py(py));
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }

        Ok(self.events.len() != before)
    }

    fn flush_pending(&mut self, cx: &mut Context<'_>) -> bool {
        let mut sent = false;
        let ids: Vec<u32> = self.pending_send.keys().copied().collect();
        for stream_id in ids {
            loop {
                let queue = match self.pending_send.get_mut(&stream_id) {
                    Some(q) if !q.is_empty() => q,
                    _ => break,
                };
                let send = match self.send_streams.get_mut(&stream_id) {
                    Some(s) => s,
                    None => break,
                };
                let (chunk, end_stream) = queue.front().cloned().unwrap();
                if chunk.is_empty() {
                    let _ = send.send_data(Bytes::new(), end_stream);
                    queue.pop_front();
                    sent = true;
                    continue;
                }
                send.reserve_capacity(chunk.len());
                match send.poll_capacity(cx) {
                    Poll::Ready(Some(Ok(cap))) if cap > 0 => {
                        let take = std::cmp::min(cap, chunk.len());
                        let to_send = chunk.slice(0..take);
                        let last = take == chunk.len();
                        let _ = send.send_data(to_send, last && end_stream);
                        if last {
                            queue.pop_front();
                        } else {
                            queue[0] = (chunk.slice(take..), end_stream);
                        }
                        sent = true;
                    }
                    Poll::Ready(Some(Ok(_))) => break,
                    Poll::Ready(Some(Err(_))) | Poll::Ready(None) => {
                        queue.clear();
                        break;
                    }
                    Poll::Pending => break,
                }
            }
        }
        sent
    }
}

#[pymethods]
impl H2Connection {
    #[new]
    fn new() -> Self {
        let io = SharedIo::new();
        let fut: HandshakeFut = Box::pin(server::handshake(io.clone()));
        H2Connection {
            io,
            state: State::Handshaking(fut),
            recv_streams: HashMap::new(),
            responders: HashMap::new(),
            send_streams: HashMap::new(),
            pending_send: HashMap::new(),
            events: VecDeque::new(),
        }
    }

    /// Feed bytes received from the peer; returns the events they produced.
    fn receive_data(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<Vec<PyObject>> {
        self.io.0.borrow_mut().push_inbound(data);
        self.pump(py)?;
        Ok(self.events.drain(..).collect())
    }

    /// Signal that the peer closed its side of the connection (EOF).
    fn receive_eof(&mut self, py: Python<'_>) -> PyResult<Vec<PyObject>> {
        self.io.0.borrow_mut().inbound_closed = true;
        self.pump(py)?;
        Ok(self.events.drain(..).collect())
    }

    /// Drain bytes that must be written to the peer.
    ///
    /// The state-changing calls (`send_headers`, `send_data`, ...) only mutate
    /// local state; the connection is driven here, so a burst of them coalesces
    /// into a single pump instead of re-driving the whole machine per call.
    fn data_to_send<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.pump(py)?;
        let mut inner = self.io.0.borrow_mut();
        let out = std::mem::take(&mut inner.outbound);
        Ok(PyBytes::new_bound(py, &out))
    }

    /// Send response headers on a stream. `headers` is a list of (name, value) byte pairs;
    /// the `:status` pseudo-header supplies the status code.
    #[pyo3(signature = (stream_id, status, headers, end_stream=false))]
    fn send_headers(
        &mut self,
        stream_id: u32,
        status: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>,
        end_stream: bool,
    ) -> PyResult<()> {
        let respond = self
            .responders
            .remove(&stream_id)
            .ok_or_else(|| PyValueError::new_err(format!("unknown stream {stream_id}")))?;
        let mut response = Response::builder()
            .status(StatusCode::from_u16(status).map_err(|e| PyValueError::new_err(e.to_string()))?);
        {
            let hdrs = response.headers_mut().unwrap();
            for (name, value) in headers {
                if name.first() == Some(&b':') {
                    continue; // pseudo-headers are set from `status`
                }
                let hn = HeaderName::from_bytes(&name)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                let hv = HeaderValue::from_bytes(&value)
                    .map_err(|e| PyValueError::new_err(e.to_string()))?;
                hdrs.append(hn, hv);
            }
        }
        let response = response
            .body(())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let mut respond = respond;
        let send = respond
            .send_response(response, end_stream)
            .map_err(|e| PyValueError::new_err(format!("send_response failed: {e}")))?;
        if !end_stream {
            self.send_streams.insert(stream_id, send);
        }
        Ok(())
    }

    #[pyo3(signature = (stream_id, data, end_stream=false))]
    fn send_data(&mut self, stream_id: u32, data: &[u8], end_stream: bool) -> PyResult<()> {
        // Take `&[u8]` (buffer protocol, zero-copy view) rather than `Vec<u8>`,
        // which PyO3 would fill element-by-element - O(n) Python int conversions.
        self.pending_send
            .entry(stream_id)
            .or_default()
            .push_back((Bytes::copy_from_slice(data), end_stream));
        Ok(())
    }

    #[pyo3(signature = (stream_id, error_code=8))]
    fn reset_stream(&mut self, stream_id: u32, error_code: u32) -> PyResult<()> {
        if let Some(mut send) = self.send_streams.remove(&stream_id) {
            send.send_reset(h2::Reason::from(error_code));
        }
        self.responders.remove(&stream_id);
        self.recv_streams.remove(&stream_id);
        self.pending_send.remove(&stream_id);
        Ok(())
    }

    /// Acknowledge that the app consumed `size` bytes of a request body, refilling
    /// the stream-level flow-control window.
    fn acknowledge_data(&mut self, stream_id: u32, size: usize) -> PyResult<()> {
        if let Some(recv) = self.recv_streams.get_mut(&stream_id) {
            let _ = recv.flow_control().release_capacity(size);
        }
        Ok(())
    }

    /// Begin a graceful shutdown (send GOAWAY).
    fn close_connection(&mut self) -> PyResult<()> {
        if let State::Ready(conn) = &mut self.state {
            conn.graceful_shutdown();
        }
        Ok(())
    }
}

#[pymodule]
fn _rh2(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<H2Connection>()?;
    m.add_class::<RequestReceived>()?;
    m.add_class::<DataReceived>()?;
    m.add_class::<StreamEnded>()?;
    m.add_class::<StreamReset>()?;
    m.add_class::<ConnectionTerminated>()?;
    Ok(())
}
