use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UdpSocket,
    sync::{mpsc, oneshot},
};

use crate::ptcp::{PTCPBody, PTCPEvent, PTCPPayload, PTCPSession, PTCP};

/**
 * Read data from the channel and write it back to the client
 */
pub async fn process_writer(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    while let Some(data) = rx.recv().await {
        if writer.write_all(&data).await.is_err() {
            println!("Writer: Socket closed by peer.");
            break;
        }
    }
}

/**
 * Read data from the client and send it to the channel
 */
pub async fn process_reader(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    realm_id: u32,
    dh_tx: mpsc::Sender<PTCPEvent>,
) {
    let mut buf = [0u8; 4096];

    loop {
        let n = match reader.read(&mut buf).await {
            Ok(n) => {
                if n == 0 {
                    println!("Reader: Socket closed by peer.");
                    dh_tx.send(PTCPEvent::Disconnect(realm_id)).await.unwrap();
                    break;
                }

                n
            }
            Err(e) => {
                println!("Reader: {}", e);
                dh_tx.send(PTCPEvent::Disconnect(realm_id)).await.unwrap();
                break;
            }
        };

        dh_tx
            .send(PTCPEvent::Data(realm_id, buf[0..n].to_vec()))
            .await
            .unwrap();
    }
}

/**
* Read data from client and send it to devices
*/
pub async fn dh_writer(
    session: Arc<Mutex<PTCPSession>>,
    socket: Arc<UdpSocket>,
    mut dh_rx: mpsc::Receiver<PTCPEvent>,
    remote_port: u32,
) {
    loop {
        let ev = dh_rx.recv().await.unwrap();

        match ev {
            PTCPEvent::Heartbeat => {
                let p = session.lock().unwrap().send(PTCPBody::Heartbeat);
                socket.ptcp_request(p).await;
            }
            PTCPEvent::Connect(realm) => {
                let p = session
                    .lock()
                    .unwrap()
                    .send(PTCPBody::Bind(realm, remote_port));
                socket.ptcp_request(p).await;
            }
            PTCPEvent::Disconnect(realm) => {
                let p = session
                    .lock()
                    .unwrap()
                    .send(PTCPBody::Status(realm, "DISC".to_string()));
                socket.ptcp_request(p).await;
            }
            PTCPEvent::Data(realm, data) => {
                let p = session
                    .lock()
                    .unwrap()
                    .send(PTCPBody::Payload(PTCPPayload { realm, data }));
                socket.ptcp_request(p).await;
            }
        }
    }
}

/**
 * Read data from devices and send it to clients
 */
pub async fn dh_reader(
    session: Arc<Mutex<PTCPSession>>,
    socket: Arc<UdpSocket>,
    channels: Arc<Mutex<HashMap<u32, mpsc::UnboundedSender<Vec<u8>>>>>,
    conn_channels: Arc<Mutex<HashMap<u32, oneshot::Sender<bool>>>>,
) {
    loop {
        let packet = socket.ptcp_read().await;
        let packets = session.lock().unwrap().recv_ordered(packet);

        // Always ACK (with the current in-order offset) so the device can
        // retransmit anything we are missing.
        let needs_ack = packets.iter().all(|p| !matches!(p.body, PTCPBody::Empty));
        if needs_ack {
            let p = session.lock().unwrap().send(PTCPBody::Empty);
            socket.ptcp_request(p).await;
        }

        for packet in packets {
            match packet.body {
                PTCPBody::Status(realm, status) => {
                    if status == "CONN" {
                        if let Some(tx) = conn_channels.lock().unwrap().remove(&realm) {
                            let _ = tx.send(true);
                        }
                    } else if status == "DISC" {
                        channels.lock().unwrap().remove(&realm);
                    }
                }
                PTCPBody::Payload(p) => {
                    // Never block here: this task ACKs packets for every realm, so a
                    // slow or closed client must not stall the whole P2P session.
                    let mut map = channels.lock().unwrap();
                    if let Some(tx) = map.get(&p.realm) {
                        if tx.send(p.data).is_err() {
                            map.remove(&p.realm);
                        }
                    }
                }
                _ => {}
            }
        }
    }
}
