//! files:write and files:write_batch: the API's body limits, the Native upload
//! admission, and the writes the sandbox replays by the granted guest key.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::StreamExt as _;
use pplx_space_guest_process::{GuestUserName, IdempotencyKey};
use pplx_space_native_files::limits::{MAX_FILE_WRITE_BYTES, WRITE_FILE_BODY_LIMIT_BYTES};
use pplx_space_native_files::{
    BatchEntry, FileOperation, decode_write_chunks, write_batch_fingerprint, write_fingerprint,
};
use pplx_space_sbx_mgr_client::operations::{MkdirResponse, WriteBatchResponse, WriteResponse};
use pplx_space_sbx_mgr_client::proto::{SpacedWriteRequest, spaced_write_request};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::{UploadAdmissionConfig, UploadShareConfig};

/// What the guest key for `operation`'s request under `key` is, as the API
/// derives it.
fn guest_key(operation: FileOperation, key: &str, fingerprint: &str) -> String {
    pplx_space_native_files::guest_key(
        operation.route(),
        &IdempotencyKey::parse(key).unwrap(),
        &pplx_space_public_api::RequestFingerprint::parse(fingerprint.to_owned()).unwrap(),
    )
    .unwrap()
    .as_str()
    .to_owned()
}

/// A write stream's open message and its data.
fn written(
    messages: &[SpacedWriteRequest],
) -> (pplx_space_sbx_mgr_client::proto::SpacedWriteOpen, Vec<u8>) {
    let mut open = None;
    let mut data = Vec::new();
    for message in messages {
        match message.operation.clone().unwrap().message.unwrap() {
            spaced_write_request::Message::Open(opened) => open = Some(opened),
            spaced_write_request::Message::Data(chunk) => data.extend_from_slice(&chunk),
        }
    }
    (open.expect("a write opens its file"), data)
}

impl Harness {
    fn fingerprints(&self) -> Vec<Option<String>> {
        self.api
            .calls()
            .into_iter()
            .filter(|call| call.rpc == "prepare")
            .map(|call| call.fingerprint)
            .collect()
    }
}

#[tokio::test]
async fn writes_answer_through_the_granted_route_in_the_apis_shapes() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    harness
        .worker
        .reply(Reply::Write(Ok(WriteResponse { bytes_written: 5 })));
    harness
        .worker
        .reply(Reply::WriteBatch(Ok(WriteBatchResponse {
            files_written: 2,
        })));
    let data = BASE64.encode(b"hello");
    let (status, _, body) = read_body(
        harness
            .send(harness.post_keyed(
                "write",
                "k-write",
                json!({"path": "/workspace/a", "data_base64": data, "user": "sandbox"}),
            ))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"bytes_written": 5})
    );
    let (status, _, body) = read_body(
        harness
            .send(harness.post_keyed(
                "write_batch",
                "k-batch",
                json!({"entries": [
                    {"path": "/a", "data_base64": BASE64.encode(b"1")},
                    {"path": "/b", "data_base64": BASE64.encode(b"22"), "append": true},
                ]}),
            ))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        json!({"files_written": 2})
    );

    // Prepared with the fingerprints the API's handlers compute.
    let user = GuestUserName::parse("sandbox").unwrap();
    let chunks = decode_write_chunks(&data).unwrap();
    let write = write_fingerprint("/workspace/a", false, false, Some(&user), &chunks).unwrap();
    let batch = write_batch_fingerprint(
        false,
        None,
        [
            BatchEntry {
                path: "/a",
                append: false,
                data: b"1",
            },
            BatchEntry {
                path: "/b",
                append: true,
                data: b"22",
            },
        ]
        .into_iter(),
    )
    .unwrap();
    assert_eq!(
        harness.fingerprints(),
        [
            Some(write.as_str().to_owned()),
            Some(batch.as_str().to_owned())
        ]
    );

    // The sandbox got the API's requests, under the guest keys derived from
    // the bound keys and those fingerprints.
    let calls = harness.worker.calls();
    let sandbox_id = harness.api.sandbox_id.to_string();
    match &calls[0].request {
        WorkerRequest::Write(messages) => {
            assert!(
                messages
                    .iter()
                    .all(|message| message.sandbox_id == sandbox_id)
            );
            let (open, data) = written(messages);
            assert_eq!(open.path, "/workspace/a");
            assert!(!open.append);
            assert_eq!(open.user.as_deref(), Some("sandbox"));
            assert_eq!(
                open.idempotency_key.as_deref(),
                Some(guest_key(FileOperation::Write, "k-write", write.as_str()).as_str())
            );
            assert_eq!(data, b"hello");
        }
        other => panic!("expected a write, got {other:?}"),
    }
    match &calls[1].request {
        WorkerRequest::WriteBatch(request) => {
            assert_eq!(request.sandbox_id, sandbox_id);
            let operation = request.operation.as_ref().unwrap();
            assert_eq!(
                operation.idempotency_key.as_deref(),
                Some(guest_key(FileOperation::WriteBatch, "k-batch", batch.as_str()).as_str())
            );
            let entries: Vec<_> = operation
                .entries
                .iter()
                .map(|entry| (entry.path.as_str(), entry.data.as_slice(), entry.append))
                .collect();
            assert_eq!(
                entries,
                [
                    ("/a", b"1".as_slice(), false),
                    ("/b", b"22".as_slice(), true)
                ]
            );
        }
        other => panic!("expected a batch, got {other:?}"),
    }
    for call in &calls {
        assert_eq!(call.route_token.as_deref(), Some(ROUTE_TOKEN));
    }
}

#[tokio::test]
async fn a_parents_write_creates_its_directories_once_the_guest_misses_them() {
    if !test_process() {
        return;
    }
    let missing = || {
        pplx_space_common_grpc::mark_guest_path_not_found(Status::not_found("no such directory"))
    };
    let harness = Harness::start().await;
    harness.worker.reply(Reply::Write(Err(missing())));
    harness
        .worker
        .reply(Reply::Mkdir(Ok(MkdirResponse::default())));
    harness
        .worker
        .reply(Reply::Write(Ok(WriteResponse { bytes_written: 1 })));
    harness.worker.reply(Reply::WriteBatch(Err(missing())));
    harness
        .worker
        .reply(Reply::Mkdir(Ok(MkdirResponse::default())));
    harness
        .worker
        .reply(Reply::Mkdir(Ok(MkdirResponse::default())));
    harness
        .worker
        .reply(Reply::WriteBatch(Ok(WriteBatchResponse {
            files_written: 2,
        })));
    let one = BASE64.encode(b"1");
    let response = harness
        .send(harness.post_keyed(
            "write",
            "k-1",
            json!({"path": "/workspace/new/a", "data_base64": one, "parents": true}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let response = harness
        .send(harness.post_keyed(
            "write_batch",
            "k-2",
            json!({"parents": true, "entries": [
                {"path": "/x/a", "data_base64": one},
                {"path": "/y/b", "data_base64": one},
            ]}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::OK);

    let fingerprints = harness.fingerprints();
    let write_key = guest_key(
        FileOperation::Write,
        "k-1",
        fingerprints[0].as_deref().unwrap(),
    );
    let batch_key = guest_key(
        FileOperation::WriteBatch,
        "k-2",
        fingerprints[1].as_deref().unwrap(),
    );
    let seen: Vec<String> = harness
        .worker
        .calls()
        .iter()
        .map(|call| match &call.request {
            WorkerRequest::Write(messages) => {
                format!("write {}", written(messages).0.idempotency_key.unwrap())
            }
            WorkerRequest::WriteBatch(request) => format!(
                "batch {}",
                request.operation.clone().unwrap().idempotency_key.unwrap()
            ),
            WorkerRequest::Mkdir(request) => {
                let operation = request.operation.clone().unwrap();
                assert!(operation.parents && operation.exist_ok);
                format!("mkdir {}", operation.path)
            }
            other => panic!("unexpected {other:?}"),
        })
        .collect();
    // Directories only after the guest reported the path missing, then the
    // write again under the derived retry key.
    assert_eq!(
        seen,
        [
            format!("write {write_key}"),
            "mkdir /workspace/new".to_owned(),
            format!("write {write_key}:parents"),
            format!("batch {batch_key}"),
            "mkdir /x".to_owned(),
            "mkdir /y".to_owned(),
            format!("batch {batch_key}:parents"),
        ]
    );
}

/// One upload at a time per sandbox, and no wait for a slot.
fn one_upload_per_sandbox() -> UploadAdmissionConfig {
    UploadAdmissionConfig {
        wait: Duration::ZERO,
        per_sandbox: UploadShareConfig {
            max_concurrent_uploads: NonZeroUsize::new(1).unwrap(),
            max_in_flight_bytes: NonZeroU64::new(256 << 20).unwrap(),
        },
        ..UploadAdmissionConfig::default()
    }
}

#[tokio::test]
async fn a_write_is_refused_before_its_body_is_read() {
    if !test_process() {
        return;
    }
    let harness = Harness::start_with(Some(one_upload_per_sandbox())).await;
    let watched = |read: &Arc<AtomicBool>, length: Option<usize>| {
        let mut request = Request::post(harness.path("write"))
            .header(header::AUTHORIZATION, CLIENT_KEY)
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", "k-watched");
        if let Some(length) = length {
            request = request.header(header::CONTENT_LENGTH, length);
        }
        request.body(watched_body(read)).unwrap()
    };

    // Without credentials.
    harness.api.authorize(Answer::Refuse(rendered(
        401,
        &[("content-type", "application/json")],
        br#"{"code":"unauthorized","message":"invalid API key","request_id":null,"retryable":false,"details":[]}"#,
    )));
    let read = Arc::new(AtomicBool::new(false));
    let response = harness.send(watched(&read, None)).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(!read.load(Ordering::SeqCst));

    // Declared larger than the route's limit: the API's 413, unread.
    let read = Arc::new(AtomicBool::new(false));
    let (status, _, body) = read_body(
        harness
            .send(watched(&read, Some(WRITE_FILE_BODY_LIMIT_BYTES + 1)))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope.message, "invalid request");
    assert!(!read.load(Ordering::SeqCst));

    // Over admission: a write to the same sandbox while one holds its only
    // slot is refused with the Native 429.
    let (chunks, received) = mpsc::channel::<Result<Bytes, std::convert::Infallible>>(1);
    let (started, reading) = oneshot::channel();
    let mut started = Some(started);
    let held = Body::from_stream(futures_util::stream::unfold(
        received,
        move |mut received| {
            if let Some(started) = started.take() {
                let _ = started.send(());
            }
            async move { received.recv().await.map(|chunk| (chunk, received)) }
        },
    ));
    harness
        .worker
        .reply(Reply::Write(Ok(WriteResponse { bytes_written: 1 })));
    let state = harness.state.clone();
    let first = tokio::spawn(tower::ServiceExt::oneshot(
        crate::router(state),
        Request::post(harness.path("write"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", "k-held")
            .body(held)
            .unwrap(),
    ));
    // Its body is read only once it holds the slot.
    reading.await.unwrap();
    let prepared = harness.fingerprints().len();
    let read = Arc::new(AtomicBool::new(false));
    let (status, headers, body) = read_body(harness.send(watched(&read, Some(14))).await).await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
    let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        serde_json::to_value(&envelope.code).unwrap(),
        "rate_limited"
    );
    assert!(envelope.retryable);
    let retry_after: u64 = headers[header::RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=5).contains(&retry_after), "{retry_after}");
    assert!(!read.load(Ordering::SeqCst));
    assert_eq!(harness.fingerprints().len(), prepared);

    // The held write finishes, and its slot is free again.
    chunks
        .send(Ok(Bytes::from(
            json!({"path": "/a", "data_base64": BASE64.encode(b"x")}).to_string(),
        )))
        .await
        .unwrap();
    drop(chunks);
    assert_eq!(first.await.unwrap().unwrap().status(), StatusCode::OK);
    harness
        .worker
        .reply(Reply::Write(Ok(WriteResponse { bytes_written: 1 })));
    let response = harness
        .send(harness.post_keyed(
            "write",
            "k-after",
            json!({"path": "/b", "data_base64": BASE64.encode(b"y")}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_write_takes_up_to_the_apis_limits() {
    if !test_process() {
        return;
    }
    let harness = Harness::start().await;
    // The largest payload the API takes, in a body far above the 2 MiB
    // default.
    harness.worker.reply(Reply::Write(Ok(WriteResponse {
        bytes_written: MAX_FILE_WRITE_BYTES as u64,
    })));
    let at_cap = BASE64.encode(vec![7u8; MAX_FILE_WRITE_BYTES]);
    let response = harness
        .send(harness.post_keyed(
            "write",
            "k-cap",
            json!({"path": "/a", "data_base64": at_cap}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    match &harness.worker.calls()[0].request {
        WorkerRequest::Write(messages) => {
            assert_eq!(written(messages).1.len(), MAX_FILE_WRITE_BYTES)
        }
        other => panic!("expected a write, got {other:?}"),
    }

    let above = BASE64.encode(vec![7u8; MAX_FILE_WRITE_BYTES + 1]);
    let (status, _, body) = read_body(
        harness
            .send(harness.post_keyed(
                "write",
                "k-above",
                json!({"path": "/a", "data_base64": above}),
            ))
            .await,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope.message, "file write payload is too large");

    // A body past the route's limit that never said how long it is.
    let oversized = Request::post(harness.path("write"))
        .header(header::CONTENT_TYPE, "application/json")
        .header("idempotency-key", "k-chunked")
        .body(Body::from_stream(futures_util::stream::iter(
            (0..=WRITE_FILE_BODY_LIMIT_BYTES / (1 << 20))
                .map(|_| Ok::<_, std::convert::Infallible>(Bytes::from(vec![b' '; 1 << 20]))),
        )))
        .unwrap();
    assert_eq!(
        harness.send(oversized).await.status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );

    for (body, message) in [
        (
            json!({"entries": [{"path": "/a", "data_base64": BASE64.encode(vec![1u8; 3 * 1024 * 1024])}]}),
            "file write batch is too large: data and paths exceed 3145728 bytes",
        ),
        (
            json!({"parents": true, "entries": [{"path": "/a", "data_base64": "", "append": true}]}),
            "parents cannot be combined with append entries",
        ),
    ] {
        let (status, _, body) = read_body(
            harness
                .send(harness.post_keyed("write_batch", "k-b", body))
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
        assert_eq!(envelope.message, message);
    }
    assert_eq!(harness.worker.calls().len(), 1);
}

#[tokio::test]
async fn a_batch_reconnects_when_the_node_api_drains_and_a_write_stream_does_not() {
    if !test_process() {
        return;
    }
    let draining = || Status::unavailable(pplx_space_node_api_client::NODE_API_DRAINING_MESSAGE);
    let harness = Harness::start().await;
    harness.worker.reply(Reply::WriteBatch(Err(draining())));
    harness
        .worker
        .reply(Reply::WriteBatch(Ok(WriteBatchResponse {
            files_written: 1,
        })));
    let one = BASE64.encode(b"1");
    let response = harness
        .send(harness.post_keyed(
            "write_batch",
            "k-1",
            json!({"entries": [{"path": "/a", "data_base64": one}]}),
        ))
        .await;
    assert_eq!(response.status(), StatusCode::OK);
    let fingerprints = harness.fingerprints();
    assert_eq!(fingerprints.len(), 2);
    assert_eq!(fingerprints[0], fingerprints[1]);

    harness.worker.reply(Reply::Write(Err(draining())));
    let response = harness
        .send(harness.post_keyed("write", "k-2", json!({"path": "/a", "data_base64": one})))
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(harness.fingerprints().len(), 3);
    assert_eq!(harness.worker.calls().len(), 3);
}

/// Native writes take the data plane's upload budgets but none of ECW's
/// transfer limits: neither its org byte-rate bucket nor its minimum body
/// rate refuses or ends one.
#[tokio::test]
async fn a_write_is_held_to_no_transfer_rate() {
    if !test_process() {
        return;
    }
    let harness = Harness::start_with(Some(UploadAdmissionConfig {
        org_transfer_rate: Some(crate::ByteRateConfig {
            bytes_per_second: NonZeroU64::new(1).unwrap(),
            burst_bytes: NonZeroU64::new(1).unwrap(),
        }),
        min_body_rate: Some(crate::MinBodyRateConfig {
            bytes_per_second: NonZeroU64::new(1 << 20).unwrap(),
            window: Duration::from_millis(50),
        }),
        ..UploadAdmissionConfig::default()
    }))
    .await;
    let body = json!({"path": "/a", "data_base64": BASE64.encode(b"slow")}).to_string();
    for key in ["k-1", "k-2"] {
        harness
            .worker
            .reply(Reply::Write(Ok(WriteResponse { bytes_written: 4 })));
        // A body that stalls far longer than the minimum rate allows, from
        // an org already far past its byte rate after the first.
        let (head, tail) = body.split_at(body.len() / 2);
        let chunks = [Bytes::from(head.to_owned()), Bytes::from(tail.to_owned())];
        let slow = Body::from_stream(futures_util::stream::iter(chunks).then(|chunk| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok::<_, std::convert::Infallible>(chunk)
        }));
        let request = Request::post(harness.path("write"))
            .header(header::CONTENT_TYPE, "application/json")
            .header("idempotency-key", key)
            .body(slow)
            .unwrap();
        let (status, _, body) = read_body(harness.send(request).await).await;
        assert_eq!(status, StatusCode::OK, "{key}: {body:?}");
    }
}
