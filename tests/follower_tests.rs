use std::{future::Future, time::Duration};

use bytes::Bytes;
use futures::stream;
use sundae_sync_v2::worker::Follower;
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tonic::{transport::Server, Request, Response, Status};
use utxorpc_spec::utxorpc::v1::{
    cardano::{Block, BlockBody, BlockHeader, Tx, TxCommon},
    sync::{
        any_chain_block::Chain,
        follow_tip_response::Action,
        sync_service_server::{SyncService, SyncServiceServer},
        AnyChainBlock, BlockRef, DumpHistoryRequest, DumpHistoryResponse, FetchBlockRequest,
        FetchBlockResponse, FollowTipRequest, FollowTipResponse, ReadTipRequest, ReadTipResponse,
    },
};

// Bound every network await so a broken client fails rather than hanging the suite.
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(10), future)
        .await
        .expect("local sync RPC timed out")
}

struct TestSync {
    events: Vec<Result<FollowTipResponse, Status>>,
    requests: mpsc::UnboundedSender<Request<FollowTipRequest>>,
}

#[tonic::async_trait]
impl SyncService for TestSync {
    type FollowTipStream = stream::Iter<std::vec::IntoIter<Result<FollowTipResponse, Status>>>;

    async fn follow_tip(
        &self,
        request: Request<FollowTipRequest>,
    ) -> Result<Response<Self::FollowTipStream>, Status> {
        self.requests.send(request).unwrap();
        Ok(Response::new(stream::iter(self.events.clone())))
    }

    async fn fetch_block(
        &self,
        _: Request<FetchBlockRequest>,
    ) -> Result<Response<FetchBlockResponse>, Status> {
        Err(Status::unimplemented("not used by follower"))
    }

    async fn dump_history(
        &self,
        _: Request<DumpHistoryRequest>,
    ) -> Result<Response<DumpHistoryResponse>, Status> {
        Err(Status::unimplemented("not used by follower"))
    }

    async fn read_tip(
        &self,
        _: Request<ReadTipRequest>,
    ) -> Result<Response<ReadTipResponse>, Status> {
        Err(Status::unimplemented("not used by follower"))
    }
}

struct LocalSync {
    uri: String,
    requests: mpsc::UnboundedReceiver<Request<FollowTipRequest>>,
    task: JoinHandle<()>,
}

impl LocalSync {
    async fn start(events: Vec<Result<FollowTipResponse, Status>>) -> Self {
        // Match the binary's process-level TLS initialization. Dependencies
        // enable both providers, so Rustls cannot choose one automatically.
        let _ = rustls::crypto::CryptoProvider::install_default(
            rustls::crypto::ring::default_provider(),
        );
        // Keep the listener bound from address allocation through serving: no port race
        // and no sleeps or external services are needed.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        let incoming = stream::unfold(listener, |listener| async {
            let connection = listener.accept().await.map(|(socket, _)| socket);
            Some((connection, listener))
        });
        let (requests, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            Server::builder()
                .add_service(SyncServiceServer::new(TestSync { events, requests }))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        Self {
            uri,
            requests: receiver,
            task,
        }
    }

    async fn follower(&self) -> Follower {
        bounded(Follower::new(&self.uri, &None, vec![]))
            .await
            .unwrap()
    }
}

impl Drop for LocalSync {
    fn drop(&mut self) {
        // Also cleans up when assertions panic.
        self.task.abort();
    }
}

fn event(action: Action) -> FollowTipResponse {
    FollowTipResponse {
        action: Some(action),
        ..Default::default()
    }
}

fn block() -> Block {
    let child = TxCommon {
        hash: Bytes::from_static(b"\x00\xffchild"),
        ..Default::default()
    };
    Block {
        header: Some(BlockHeader {
            slot: 1234,
            height: 567,
            hash: Bytes::from_static(b"\x00\xffheader"),
            ..Default::default()
        }),
        body: Some(BlockBody {
            tx: vec![Tx {
                common: Some(TxCommon {
                    hash: Bytes::from_static(b"parent"),
                    ..Default::default()
                }),
                successful: true,
                sub_transactions: vec![child, TxCommon::default()],
                ..Default::default()
            }],
            ..Default::default()
        }),
        timestamp: 123456789,
    }
}

fn chain_block(block: Block, native_bytes: Bytes) -> AnyChainBlock {
    AnyChainBlock {
        chain: Some(Chain::Cardano(block)),
        native_bytes,
    }
}

#[tokio::test]
async fn apply_and_undo_preserve_nested_transactions_and_native_bytes() {
    let expected = block();
    let apply_bytes = Bytes::from_static(b"\x00\xff\x80apply");
    let undo_bytes = Bytes::from_static(b"\x81\x00\xffundo");
    let server = LocalSync::start(vec![
        Ok(event(Action::Apply(chain_block(
            expected.clone(),
            apply_bytes.clone(),
        )))),
        Ok(event(Action::Undo(chain_block(
            expected.clone(),
            undo_bytes.clone(),
        )))),
    ])
    .await;
    let mut follower = server.follower().await;
    for (forward, native) in [(true, apply_bytes), (false, undo_bytes)] {
        let (actual_forward, bytes, parsed, header) = bounded(follower.next_event()).await.unwrap();
        assert_eq!(actual_forward, forward);
        assert_eq!(bytes, native);
        // Whole-message equality includes common fields and sub-transactions,
        // their order, bytes fields, and the separately returned header.
        assert_eq!(parsed, expected);
        assert_eq!(header, expected.header.clone().unwrap());
    }
}

#[tokio::test]
async fn resets_are_skipped_before_apply_and_undo() {
    let expected = block();
    let reset = event(Action::Reset(BlockRef::default()));
    let server = LocalSync::start(vec![
        Ok(reset.clone()),
        Ok(reset.clone()),
        Ok(event(Action::Apply(chain_block(
            expected.clone(),
            Bytes::new(),
        )))),
        Ok(reset),
        Ok(event(Action::Undo(chain_block(
            expected.clone(),
            Bytes::new(),
        )))),
    ])
    .await;
    let mut follower = server.follower().await;
    for forward in [true, false] {
        let actual = bounded(follower.next_event()).await.unwrap();
        assert_eq!(actual.0, forward);
        assert_eq!(actual.2, expected);
    }
}

#[tokio::test]
async fn closed_stream_including_after_reset_returns_error() {
    for events in [vec![], vec![Ok(event(Action::Reset(BlockRef::default())))]] {
        let server = LocalSync::start(events).await;
        let mut follower = server.follower().await;
        assert_eq!(
            bounded(follower.next_event())
                .await
                .unwrap_err()
                .to_string(),
            "Stream closed"
        );
    }
}

#[tokio::test]
async fn apply_and_undo_without_parsed_block_return_error() {
    for action in [
        Action::Apply(AnyChainBlock::default()),
        Action::Undo(AnyChainBlock::default()),
    ] {
        let server = LocalSync::start(vec![Ok(event(action))]).await;
        let mut follower = server.follower().await;
        assert_eq!(
            bounded(follower.next_event())
                .await
                .unwrap_err()
                .to_string(),
            "expected parsed Cardano block"
        );
    }
}

#[tokio::test]
async fn missing_action_and_header_return_errors() {
    for (response, expected) in [
        (FollowTipResponse::default(), "missing follow-tip action"),
        (
            event(Action::Apply(chain_block(Block::default(), Bytes::new()))),
            "must include header",
        ),
    ] {
        let server = LocalSync::start(vec![Ok(response)]).await;
        let mut follower = server.follower().await;
        assert_eq!(
            bounded(follower.next_event())
                .await
                .unwrap_err()
                .to_string(),
            expected
        );
    }
}

#[tokio::test]
async fn api_metadata_and_intersection_are_preserved_on_wire() {
    let points = vec![
        BlockRef {
            slot: 42,
            hash: Bytes::from_static(b"\x00\xfffirst"),
            height: 12,
            timestamp: 987654,
        },
        BlockRef {
            slot: 7,
            hash: Bytes::from_static(b"second"),
            height: 3,
            timestamp: 123456,
        },
    ];
    for api_key in [Some("test-api-key".to_owned()), None] {
        let mut server = LocalSync::start(vec![]).await;
        let _follower = bounded(Follower::new(&server.uri, &api_key, points.clone()))
            .await
            .unwrap();
        let request = bounded(server.requests.recv()).await.unwrap();
        assert_eq!(
            request
                .metadata()
                .get("dmtr-api-key")
                .map(|v| v.to_str().unwrap()),
            api_key.as_deref()
        );
        assert_eq!(
            request.into_inner(),
            FollowTipRequest {
                intersect: points.clone(),
                field_mask: None,
            }
        );
    }
}

#[tokio::test]
async fn invalid_api_metadata_returns_contextual_error_without_rpc() {
    let mut server = LocalSync::start(vec![]).await;
    let result = bounded(Follower::new(
        &server.uri,
        &Some("invalid\nmetadata".to_owned()),
        vec![],
    ))
    .await;
    let error = match result {
        Ok(_) => panic!("invalid metadata unexpectedly accepted"),
        Err(error) => error,
    };
    assert_eq!(error.to_string(), "invalid UTxO RPC API key metadata");
    assert!(matches!(
        server.requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
#[ignore = "requires UTXO_RPC_TEST_URI serving the Musashi fixture and its predecessor"]
async fn live_v1_fetch_and_follow_preserve_musashi_subtransaction() {
    use pallas::interop::utxorpc::v1::Mapper;
    use sundae_sync_v2::archive::NoContext;
    use tonic::transport::Endpoint;
    use utxorpc_spec::utxorpc::v1::sync::sync_service_client::SyncServiceClient;

    let uri = std::env::var("UTXO_RPC_TEST_URI").expect("set UTXO_RPC_TEST_URI explicitly");
    let _ =
        rustls::crypto::CryptoProvider::install_default(rustls::crypto::ring::default_provider());
    let channel = bounded(Endpoint::from_shared(uri.clone()).unwrap().connect())
        .await
        .unwrap();
    let mut client = SyncServiceClient::new(channel).max_decoding_message_size(usize::MAX);
    let raw = hex::decode(
        include_str!(
            "fixtures/5ba7b23a04aadcd104db62d67a840fa208342fdad749ee138a4579236579a5d7.cbor.hex"
        )
        .trim(),
    )
    .unwrap();
    let expected = Mapper::new(NoContext).map_block_cbor(&raw);
    let expected_header = expected.header.as_ref().unwrap();
    // v1 FetchBlock accepts a single optional reference, not a repeated field.
    let fetched = bounded(client.fetch_block(FetchBlockRequest {
        r#ref: Some(BlockRef {
            hash: expected_header.hash.clone(),
            ..Default::default()
        }),
        field_mask: None,
    }))
    .await
    .unwrap()
    .into_inner()
    .block
    .unwrap();
    assert_eq!(fetched.native_bytes.as_ref(), raw);
    let Some(Chain::Cardano(parsed)) = fetched.chain else {
        panic!("expected Cardano block")
    };
    assert_eq!(parsed.header, expected.header);
    let parent = &parsed.body.as_ref().unwrap().tx[123];
    assert!(parent.successful);
    assert_eq!(parent.sub_transactions.len(), 1);
    let sub = &parent.sub_transactions[0];
    assert_eq!(
        hex::encode(&sub.hash),
        "ede49f69c8e99d253d82b0b06d024be6435168cfeb1b960761370fe1bf5f8377"
    );
    assert_eq!(
        sub.outputs,
        expected.body.as_ref().unwrap().tx[123].sub_transactions[0].outputs
    );

    let previous = bounded(client.fetch_block(FetchBlockRequest {
        r#ref: Some(BlockRef {
            height: expected_header.height - 1,
            ..Default::default()
        }),
        field_mask: None,
    }))
    .await
    .unwrap()
    .into_inner()
    .block
    .unwrap();
    let Some(Chain::Cardano(previous)) = previous.chain else {
        panic!("expected predecessor")
    };
    let previous = previous.header.unwrap();
    let point = BlockRef {
        hash: previous.hash,
        slot: previous.slot,
        height: previous.height,
        timestamp: 0,
    };
    let mut follower = bounded(Follower::new(&uri, &None, vec![point]))
        .await
        .unwrap();
    let (forward, native, followed, header) = bounded(follower.next_event()).await.unwrap();
    assert!(forward);
    assert_eq!(&header, expected_header);
    assert_eq!(native.as_ref(), raw);
    assert_eq!(followed, parsed);
}
