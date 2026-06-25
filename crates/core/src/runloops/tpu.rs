use std::{
    net::UdpSocket,
    sync::{Arc, RwLock},
    thread::JoinHandle,
};

use crossbeam_channel::Sender;
use solana_keypair::Keypair;
use solana_perf::packet::PacketBatch;
use solana_streamer::{
    nonblocking::swqos::SwQosConfig,
    quic::{QuicStreamerConfig, SpawnServerResult, spawn_stake_wighted_qos_server},
    streamer::StakedNodes,
};
use solana_transaction::versioned::VersionedTransaction;
use surfpool_types::{RpcConfig, SimnetCommand, SimnetEvent};
use tokio_util::sync::CancellationToken;

/// Accept transactions on the QUIC TPU port advertised by `getClusterNodes` and
/// route them into the same pipeline as `sendTransaction`. Clients like the
/// Anchor program deployer send buffer-write transactions straight to TPU over
/// QUIC; without a listener those packets are dropped and the deploy never lands.
pub fn start_tpu_ingest_runloop(
    rpc_config: &RpcConfig,
    simnet_commands_tx: Sender<SimnetCommand>,
    simnet_events_tx: Sender<SimnetEvent>,
    cancel: CancellationToken,
) -> Result<JoinHandle<()>, String> {
    let addr = format!("{}:{}", rpc_config.bind_host, rpc_config.tpu_quic_port);
    let socket =
        UdpSocket::bind(&addr).map_err(|e| format!("failed to bind TPU QUIC socket {addr}: {e}"))?;

    let (packet_tx, packet_rx) = crossbeam_channel::unbounded::<PacketBatch>();

    let server: SpawnServerResult = spawn_stake_wighted_qos_server(
        "surfpoolTpu",
        "surfpool_tpu_quic",
        [socket],
        &Keypair::new(),
        packet_tx,
        Arc::new(RwLock::new(StakedNodes::default())),
        QuicStreamerConfig::default(),
        SwQosConfig::default(),
        cancel,
    )
    .map_err(|e| format!("failed to start TPU QUIC server: {e}"))?;

    let _ = simnet_events_tx.send(SimnetEvent::info(format!("TPU QUIC ingest listening on {addr}")));

    hiro_system_kit::thread_named("TPU Ingest")
        .spawn(move || {
            let _server = server;
            for batch in packet_rx.iter() {
                for packet in batch.iter() {
                    let Some(bytes) = packet.data(..) else {
                        continue;
                    };
                    let Ok(transaction) = bincode::deserialize::<VersionedTransaction>(bytes) else {
                        continue;
                    };
                    let (status_tx, _status_rx) = crossbeam_channel::unbounded();
                    let _ = simnet_commands_tx.send(SimnetCommand::ProcessTransaction(
                        None,
                        transaction,
                        status_tx,
                        true,
                        None,
                    ));
                }
            }
        })
        .map_err(|e| format!("failed to spawn TPU Ingest thread: {e}"))
}
