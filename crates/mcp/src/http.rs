//! Streamable HTTP transport (`/mcp`) for [`Server`].
//!
//! Implements the MCP Streamable HTTP transport (`POST /mcp`, `DELETE /mcp`, `OPTIONS /mcp`) and a
//! lightweight `GET /health` probe using standard-library sockets. By default (`shared_session:
//! true`), all requests share one persistent studio [`Server`] on the engine thread so cloud AI
//! orchestrators that re-initialize per tool call keep full document, selection and undo state.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError, sync_channel};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

use crate::backend::Backend;
use crate::server::{MAX_LINE_BYTES, Server, read_line_capped};

const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30 * 60);
const DEFAULT_MAX_SESSIONS: usize = 64;
const MAX_CONCURRENT_CONNS: usize = 32;
const MAX_HEADERS: usize = 64;
const MAX_HEADER_LINE: usize = 8 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(120);
const JOB_QUEUE_CAP: usize = 128;
const STATELESS_SESSION_KEY: &str = "stateless-default";
const SHARED_SESSION_KEY: &str = "studio-shared";

static NEXT_SESSION_SEQ: AtomicU64 = AtomicU64::new(1);

/// Configuration for the Streamable HTTP MCP server.
#[derive(Clone, Debug)]
pub struct HttpConfig {
    /// Optional Bearer token required on `/mcp` requests.
    pub auth_token: Option<String>,
    /// Idle duration before an inactive isolated `Mcp-Session-Id` is evicted.
    pub idle_timeout: Duration,
    /// Maximum concurrent isolated sessions kept in memory.
    pub max_sessions: usize,
    /// Share one persistent studio session across all HTTP handshakes and tool calls.
    pub shared_session: bool,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self { auth_token: None, idle_timeout: DEFAULT_IDLE_TIMEOUT, max_sessions: DEFAULT_MAX_SESSIONS, shared_session: true }
    }
}

enum EngineJob {
    Post { session_id: Option<String>, body: String, reply: SyncSender<HttpReply> },
    Delete { session_id: String, reply: SyncSender<HttpReply> },
}

struct HttpError {
    status: u16,
    reason: &'static str,
    msg: &'static str,
}

impl HttpError {
    const fn new(status: u16, reason: &'static str, msg: &'static str) -> Self {
        Self { status, reason, msg }
    }
}

struct HttpReply {
    status: u16,
    reason: &'static str,
    content_type: Option<&'static str>,
    session_id: Option<String>,
    allow: Option<&'static str>,
    body: Vec<u8>,
}

impl HttpReply {
    fn json(status: u16, reason: &'static str, session_id: Option<String>, body: String) -> Self {
        Self { status, reason, content_type: Some("application/json"), session_id, allow: None, body: body.into_bytes() }
    }

    fn empty(status: u16, reason: &'static str, session_id: Option<String>) -> Self {
        Self { status, reason, content_type: None, session_id, allow: None, body: Vec::new() }
    }

    fn method_not_allowed(allow: &'static str) -> Self {
        Self { status: 405, reason: "Method Not Allowed", content_type: None, session_id: None, allow: Some(allow), body: Vec::new() }
    }
}

struct ParsedRequest {
    method: String,
    path: String,
    auth: Option<String>,
    session_id: Option<String>,
    body: String,
}

struct SessionEntry {
    server: Server,
    last_used: Instant,
}

/// Bind `addr` and serve MCP over Streamable HTTP until an accept error stops the listener.
pub fn serve<F>(addr: &str, config: HttpConfig, make_backend: F) -> std::io::Result<()>
where
    F: FnMut() -> Result<Box<dyn Backend>, String>,
{
    let listener = TcpListener::bind(addr)?;
    serve_listener(listener, config, make_backend)
}

/// Serve MCP over Streamable HTTP on an already-bound [`TcpListener`].
pub fn serve_listener<F>(listener: TcpListener, config: HttpConfig, make_backend: F) -> std::io::Result<()>
where
    F: FnMut() -> Result<Box<dyn Backend>, String>,
{
    let stop = Arc::new(AtomicBool::new(false));
    serve_until(listener, config, stop, make_backend)
}

/// Serve MCP over Streamable HTTP until `stop` is set (checked on each connection / wake).
pub fn serve_until<F>(listener: TcpListener, config: HttpConfig, stop: Arc<AtomicBool>, mut make_backend: F) -> std::io::Result<()>
where
    F: FnMut() -> Result<Box<dyn Backend>, String>,
{
    let (job_tx, job_rx) = sync_channel::<EngineJob>(JOB_QUEUE_CAP);
    let idle_timeout = config.idle_timeout;
    let max_sessions = config.max_sessions.max(1);
    let shared_session = config.shared_session;
    let shared_cfg = Arc::new(config);
    let active_conns = Arc::new(AtomicUsize::new(0));
    let accept_stop = Arc::clone(&stop);

    std::thread::scope(|scope| {
        let acceptor = scope.spawn(move || accept_loop(listener, shared_cfg, active_conns, accept_stop, job_tx));
        let mut sessions: HashMap<String, SessionEntry> = HashMap::new();
        while let Ok(job) = job_rx.recv() {
            if !shared_session {
                evict_expired(&mut sessions, idle_timeout);
            }
            run_engine_job(&mut sessions, max_sessions, shared_session, &mut make_backend, job);
        }
        acceptor.join().unwrap_or_else(|_| Err(std::io::Error::other("HTTP accept thread panicked")))
    })
}

fn accept_loop(
    listener: TcpListener,
    config: Arc<HttpConfig>,
    active: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    job_tx: SyncSender<EngineJob>,
) -> std::io::Result<()> {
    for incoming in listener.incoming() {
        if stop.load(Ordering::Acquire) {
            break;
        }
        let mut stream = match incoming {
            Ok(s) => s,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if active.load(Ordering::Acquire) >= MAX_CONCURRENT_CONNS {
            let busy = HttpReply::json(503, "Service Unavailable", None, "{\"error\":\"too many concurrent connections\"}".into());
            let _ = write_http_reply(&mut stream, &busy);
            continue;
        }
        active.fetch_add(1, Ordering::AcqRel);
        let cfg = Arc::clone(&config);
        let counter = Arc::clone(&active);
        let tx = job_tx.clone();
        std::thread::spawn(move || {
            let _ = handle_connection(&mut stream, &cfg, &tx);
            counter.fetch_sub(1, Ordering::AcqRel);
        });
    }
    Ok(())
}

fn handle_connection(stream: &mut TcpStream, config: &HttpConfig, job_tx: &SyncSender<EngineJob>) -> std::io::Result<()> {
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let _ = stream.set_write_timeout(Some(WRITE_TIMEOUT));
    let req = match read_http_request(stream) {
        Ok(Some(r)) => r,
        Ok(None) => return Ok(()),
        Err(err) => {
            let body = serde_json::json!({ "error": err.msg }).to_string();
            return write_http_reply(stream, &HttpReply::json(err.status, err.reason, None, body));
        }
    };
    let reply = dispatch_http_request(req, config, job_tx);
    write_http_reply(stream, &reply)
}

fn dispatch_http_request(req: ParsedRequest, config: &HttpConfig, job_tx: &SyncSender<EngineJob>) -> HttpReply {
    let path = req.path.split('?').next().unwrap_or(req.path.as_str());
    if req.method == "OPTIONS" {
        return HttpReply::empty(204, "No Content", None);
    }
    if req.method == "GET" && (path == "/health" || path == "/healthz") {
        return HttpReply::json(200, "OK", None, "{\"ok\":true,\"service\":\"vectorcraft-mcp\"}".into());
    }
    if path != "/mcp" && path != "/" {
        return HttpReply::json(404, "Not Found", None, "{\"error\":\"not found; use /mcp or /health\"}".into());
    }
    if let Some(expected) = config.auth_token.as_deref()
        && !bearer_matches(req.auth.as_deref(), expected)
    {
        return HttpReply::json(401, "Unauthorized", None, "{\"error\":\"missing or invalid Bearer token\"}".into());
    }
    match req.method.as_str() {
        "GET" => HttpReply::method_not_allowed("POST, DELETE, OPTIONS"),
        "DELETE" => match req.session_id {
            Some(session_id) if is_valid_session_id(&session_id) => send_job(job_tx, |reply| EngineJob::Delete { session_id, reply }),
            _ => HttpReply::json(400, "Bad Request", None, "{\"error\":\"missing or invalid Mcp-Session-Id header\"}".into()),
        },
        "POST" => {
            let clean_sid = req.session_id.filter(|s| is_valid_session_id(s));
            send_job(job_tx, |reply| EngineJob::Post { session_id: clean_sid, body: req.body, reply })
        }
        _ => HttpReply::method_not_allowed("POST, DELETE, OPTIONS"),
    }
}

fn send_job(job_tx: &SyncSender<EngineJob>, build: impl FnOnce(SyncSender<HttpReply>) -> EngineJob) -> HttpReply {
    let (tx, rx) = sync_channel(1);
    match job_tx.try_send(build(tx)) {
        Ok(()) => rx.recv().unwrap_or_else(|_| HttpReply::json(500, "Internal Server Error", None, "{\"error\":\"engine closed\"}".into())),
        Err(TrySendError::Full(_)) => HttpReply::json(503, "Service Unavailable", None, "{\"error\":\"server queue full\"}".into()),
        Err(TrySendError::Disconnected(_)) => HttpReply::json(503, "Service Unavailable", None, "{\"error\":\"server shutting down\"}".into()),
    }
}

fn run_engine_job<F>(
    sessions: &mut HashMap<String, SessionEntry>,
    max_sessions: usize,
    shared_session: bool,
    make_backend: &mut F,
    job: EngineJob,
) where
    F: FnMut() -> Result<Box<dyn Backend>, String>,
{
    match job {
        EngineJob::Delete { session_id, reply } => {
            if !shared_session {
                sessions.remove(&session_id);
            }
            let _ = reply.send(HttpReply::empty(204, "No Content", None));
        }
        EngineJob::Post { session_id, body, reply } => {
            let http_reply = handle_post(sessions, max_sessions, shared_session, make_backend, session_id, &body);
            let _ = reply.send(http_reply);
        }
    }
}

fn handle_post<F>(
    sessions: &mut HashMap<String, SessionEntry>,
    max_sessions: usize,
    shared_session: bool,
    make_backend: &mut F,
    asked_sid: Option<String>,
    body: &str,
) -> HttpReply
where
    F: FnMut() -> Result<Box<dyn Backend>, String>,
{
    let is_init = is_initialize_request(body);
    let (store_key, header_sid) = if shared_session {
        let echoed = match (is_init, asked_sid) {
            (_, Some(id)) => Some(id),
            (true, None) => Some(new_session_id()),
            (false, None) => None,
        };
        (SHARED_SESSION_KEY.to_string(), echoed)
    } else {
        let sid = match (is_init, asked_sid) {
            (true, None) => new_session_id(),
            (_, Some(id)) => id,
            (false, None) => STATELESS_SESSION_KEY.to_string(),
        };
        if is_init {
            sessions.remove(&sid);
        }
        let echoed = (sid != STATELESS_SESSION_KEY).then(|| sid.clone());
        (sid, echoed)
    };

    if !sessions.contains_key(&store_key) {
        if !shared_session && sessions.len() >= max_sessions {
            evict_oldest(sessions);
        }
        let backend = match make_backend() {
            Ok(b) => b,
            Err(e) => {
                let err = serde_json::json!({"jsonrpc": "2.0", "id": Value::Null, "error": {"code": -32603, "message": e}}).to_string();
                return HttpReply::json(500, "Internal Server Error", None, err);
            }
        };
        sessions.insert(store_key.clone(), SessionEntry { server: Server::new(backend), last_used: Instant::now() });
    }
    let Some(entry) = sessions.get_mut(&store_key) else {
        return HttpReply::json(500, "Internal Server Error", None, "{\"error\":\"session missing\"}".into());
    };
    entry.last_used = Instant::now();
    match entry.server.handle_line(body) {
        Some(line) => HttpReply::json(200, "OK", header_sid, line),
        None => HttpReply::empty(202, "Accepted", header_sid),
    }
}

fn evict_expired(sessions: &mut HashMap<String, SessionEntry>, timeout: Duration) {
    sessions.retain(|_, entry| entry.last_used.elapsed() < timeout);
}

fn evict_oldest(sessions: &mut HashMap<String, SessionEntry>) {
    let oldest = sessions.iter().min_by_key(|(_, e)| e.last_used).map(|(k, _)| k.clone());
    if let Some(key) = oldest {
        sessions.remove(&key);
    }
}

fn is_initialize_request(body: &str) -> bool {
    let Ok(val) = serde_json::from_str::<Value>(body) else { return false };
    val.get("method").and_then(Value::as_str) == Some("initialize")
}

fn is_valid_session_id(s: &str) -> bool {
    !s.is_empty() && s.len() <= 128 && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn new_session_id() -> String {
    let seq = NEXT_SESSION_SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(seq);
    hasher.write_u128(nanos);
    let rand_part = hasher.finish();
    format!("vc-{seq:x}-{rand_part:016x}")
}

fn bearer_matches(auth_header: Option<&str>, expected_token: &str) -> bool {
    let Some(raw) = auth_header else { return false };
    let Some((scheme, token)) = raw.trim().split_once(' ') else { return false };
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return false;
    }
    let given = token.trim().as_bytes();
    let expected = expected_token.as_bytes();
    if given.len() != expected.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&a, &b) in given.iter().zip(expected.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

fn read_http_request(stream: &mut TcpStream) -> Result<Option<ParsedRequest>, HttpError> {
    let cloned = stream.try_clone().map_err(|_| HttpError::new(500, "Internal Server Error", "stream clone failed"))?;
    let mut reader = BufReader::new(cloned);
    let mut line_buf = Vec::new();
    let bad_req_line = |_| HttpError::new(400, "Bad Request", "failed reading request line");
    let has_first = read_line_capped(&mut reader, &mut line_buf, MAX_HEADER_LINE).map_err(bad_req_line)?;
    if !has_first {
        return Ok(None);
    }
    if line_buf.len() > MAX_HEADER_LINE {
        return Err(HttpError::new(431, "Request Header Fields Too Large", "request line too long"));
    }
    let req_line = std::str::from_utf8(&line_buf).map_err(|_| HttpError::new(400, "Bad Request", "invalid UTF-8 in request line"))?;
    let mut parts = req_line.split_whitespace();
    let method = parts.next().ok_or_else(|| HttpError::new(400, "Bad Request", "missing HTTP method"))?.to_string();
    let path = parts.next().ok_or_else(|| HttpError::new(400, "Bad Request", "missing request target"))?.to_string();

    let mut content_len: Option<usize> = None;
    let mut chunked = false;
    let mut auth: Option<String> = None;
    let mut session_id: Option<String> = None;

    for _ in 0..MAX_HEADERS {
        let bad_hdr = |_| HttpError::new(400, "Bad Request", "malformed headers");
        let ok = read_line_capped(&mut reader, &mut line_buf, MAX_HEADER_LINE).map_err(bad_hdr)?;
        if !ok || line_buf.is_empty() {
            let body = read_http_body(&mut reader, content_len, chunked)?;
            return Ok(Some(ParsedRequest { method, path, auth, session_id, body }));
        }
        if line_buf.len() > MAX_HEADER_LINE {
            return Err(HttpError::new(431, "Request Header Fields Too Large", "header too long"));
        }
        let header = std::str::from_utf8(&line_buf).map_err(|_| HttpError::new(400, "Bad Request", "invalid header encoding"))?;
        let Some((name, val)) = header.split_once(':') else {
            return Err(HttpError::new(400, "Bad Request", "malformed header line"));
        };
        let val = val.trim();
        if name.eq_ignore_ascii_case("content-length") {
            let n = val.parse::<usize>().map_err(|_| HttpError::new(400, "Bad Request", "invalid Content-Length"))?;
            if n > MAX_LINE_BYTES {
                return Err(HttpError::new(413, "Payload Too Large", "request body exceeds 16 MiB limit"));
            }
            content_len = Some(n);
        } else if name.eq_ignore_ascii_case("transfer-encoding") && val.to_ascii_lowercase().contains("chunked") {
            chunked = true;
        } else if name.eq_ignore_ascii_case("authorization") {
            auth = Some(val.to_string());
        } else if name.eq_ignore_ascii_case("mcp-session-id") {
            session_id = Some(val.to_string());
        }
    }
    Err(HttpError::new(431, "Request Header Fields Too Large", "too many HTTP headers"))
}

fn read_http_body(reader: &mut impl BufRead, content_len: Option<usize>, chunked: bool) -> Result<String, HttpError> {
    let bytes = if chunked {
        read_chunked_body(reader)?
    } else if let Some(len) = content_len {
        let mut buf = vec![0u8; len];
        std::io::Read::read_exact(reader, &mut buf).map_err(|_| HttpError::new(400, "Bad Request", "truncated request body"))?;
        buf
    } else {
        Vec::new()
    };
    String::from_utf8(bytes).map_err(|_| HttpError::new(400, "Bad Request", "request body must be UTF-8"))
}

fn read_chunked_body(reader: &mut impl BufRead) -> Result<Vec<u8>, HttpError> {
    let mut out = Vec::new();
    let mut size_line = Vec::new();
    loop {
        let ok = read_line_capped(reader, &mut size_line, 128).map_err(|_| HttpError::new(400, "Bad Request", "invalid chunk header"))?;
        if !ok || size_line.len() > 128 {
            return Err(HttpError::new(400, "Bad Request", "invalid chunk size line"));
        }
        let raw = std::str::from_utf8(&size_line).map_err(|_| HttpError::new(400, "Bad Request", "invalid chunk size encoding"))?;
        let hex = raw.split(';').next().unwrap_or("").trim();
        let chunk_len = usize::from_str_radix(hex, 16).map_err(|_| HttpError::new(400, "Bad Request", "bad chunk hex length"))?;
        if chunk_len == 0 {
            let _ = read_line_capped(reader, &mut size_line, MAX_HEADER_LINE);
            return Ok(out);
        }
        if out.len().saturating_add(chunk_len) > MAX_LINE_BYTES {
            return Err(HttpError::new(413, "Payload Too Large", "chunked body exceeds 16 MiB limit"));
        }
        let start = out.len();
        out.resize(start + chunk_len, 0);
        if let Some(slice) = out.get_mut(start..) {
            std::io::Read::read_exact(slice_reader(reader), slice).map_err(|_| HttpError::new(400, "Bad Request", "truncated chunk payload"))?;
        }
        let mut crlf = [0u8; 2];
        std::io::Read::read_exact(reader, &mut crlf).map_err(|_| HttpError::new(400, "Bad Request", "missing chunk CRLF"))?;
    }
}

fn slice_reader<R: BufRead>(reader: &mut R) -> &mut R {
    reader
}

fn write_http_reply(stream: &mut impl Write, reply: &HttpReply) -> std::io::Result<()> {
    write!(stream, "HTTP/1.1 {} {}\r\n", reply.status, reply.reason)?;
    write!(stream, "Connection: close\r\n")?;
    write!(stream, "Access-Control-Allow-Origin: *\r\n")?;
    write!(stream, "Access-Control-Allow-Methods: POST, GET, DELETE, OPTIONS\r\n")?;
    write!(stream, "Access-Control-Allow-Headers: Content-Type, Accept, Authorization, Mcp-Session-Id, MCP-Protocol-Version\r\n")?;
    write!(stream, "Access-Control-Expose-Headers: Mcp-Session-Id\r\n")?;
    if let Some(ct) = reply.content_type {
        write!(stream, "Content-Type: {ct}\r\n")?;
    }
    if let Some(allow) = reply.allow {
        write!(stream, "Allow: {allow}\r\n")?;
    }
    if let Some(sid) = &reply.session_id {
        write!(stream, "Mcp-Session-Id: {sid}\r\n")?;
    }
    write!(stream, "Content-Length: {}\r\n\r\n", reply.body.len())?;
    if !reply.body.is_empty() {
        stream.write_all(&reply.body)?;
    }
    stream.flush()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use serde_json::{Value, json};

    use crate::headless::Headless;

    use super::{HttpConfig, serve_until};

    fn send_raw(addr: std::net::SocketAddr, raw_req: &str) -> (u16, HashMap<String, String>, String) {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        stream.write_all(raw_req.as_bytes()).unwrap();
        stream.flush().unwrap();
        let mut resp = String::new();
        stream.read_to_string(&mut resp).unwrap();
        let (head, body) = resp.split_once("\r\n\r\n").unwrap();
        let mut lines = head.lines();
        let status_line = lines.next().unwrap();
        let status: u16 = status_line.split_whitespace().nth(1).unwrap().parse().unwrap();
        let mut headers = HashMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
        (status, headers, body.to_string())
    }

    #[test]
    fn shared_studio_session_persists_across_reinitialize_and_delete() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = Arc::clone(&stop);
        let cfg = HttpConfig { auth_token: Some("secret-token".into()), ..Default::default() };

        let handle = std::thread::spawn(move || serve_until(listener, cfg, stop_bg, || Ok(Box::new(Headless::with_document()))));

        let (status, _, body) = send_raw(addr, "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n");
        assert_eq!(status, 200);
        assert!(body.contains("\"ok\":true"));

        let init_body = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18"}}).to_string();
        let unauth_req = format!("POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{init_body}", init_body.len());
        let (status, _, _) = send_raw(addr, &unauth_req);
        assert_eq!(status, 401);

        let auth_init = format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token\r\nContent-Length: {}\r\n\r\n{init_body}",
            init_body.len()
        );
        let (status, headers, body) = send_raw(addr, &auth_init);
        assert_eq!(status, 200);
        let sid1 = headers.get("mcp-session-id").unwrap().clone();
        let parsed: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["result"]["serverInfo"]["name"], "vectorcraft");

        let draw = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "draw_shape", "arguments": {"shape": "rectangle", "x": 20.0, "y": 30.0, "width": 120.0, "height": 80.0, "fill": "#ff0000", "name": "PersistBox"}}
        })
        .to_string();
        let draw_req = format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token\r\nMcp-Session-Id: {sid1}\r\nContent-Length: {}\r\n\r\n{draw}",
            draw.len()
        );
        let (status, _, _) = send_raw(addr, &draw_req);
        assert_eq!(status, 200);

        let del_req = format!("DELETE /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token\r\nMcp-Session-Id: {sid1}\r\n\r\n");
        let (status, _, _) = send_raw(addr, &del_req);
        assert_eq!(status, 204);

        // Simulate a cloud AI client opening a brand-new initialize handshake for the next tool call.
        let (status, headers2, _) = send_raw(addr, &auth_init);
        assert_eq!(status, 200);
        let sid2 = headers2.get("mcp-session-id").unwrap().clone();
        assert_ne!(sid1, sid2);

        let inspect = json!({"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "doc_inspect", "arguments": {}}}).to_string();
        let inspect_req = format!(
            "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer secret-token\r\nMcp-Session-Id: {sid2}\r\nContent-Length: {}\r\n\r\n{inspect}",
            inspect.len()
        );
        let (status, _, body) = send_raw(addr, &inspect_req);
        assert_eq!(status, 200);
        assert!(body.contains("PersistBox"));

        stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(addr);
        handle.join().unwrap().unwrap();
    }
}
