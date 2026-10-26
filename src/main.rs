use clap::Parser;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::{
    net::{TcpListener, UdpSocket},
    sync::{mpsc, oneshot},
};

use crate::{
    dh::p2p_handshake,
    process::{dh_reader, dh_writer, process_reader, process_writer},
    ptcp::PTCPEvent,
};

mod dh;
mod process;
mod ptcp;

#[derive(Parser)]
#[command(about = "A PoC implementation of TCP tunneling over Dahua P2P protocol.", long_about = None)]
struct Cli {
    /// Bind address, port and remote port. Default: 127.0.0.1:1554:554
    #[arg(short, long, value_name = "[bind_address:]port:remote_port")]
    port: Option<String>,
    /// Relay mode (experimental)
    #[arg(short, long)]
    relay: bool,
    /// Serial number of the camera
    serial: String,
}

/// Consecutive connects the device ignored; a dead P2P session shows up this way.
static CONNECT_FAILS: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let serial = args.serial;
    let port = args.port.unwrap_or("127.0.0.1:1554:554".to_string());

    let parts: Vec<&str> = port.split(':').collect();
    let (bind_address, bind_port, remote_port): (&str, u16, u16) = match parts.len() {
        2 => (
            "127.0.0.1",
            parts[0].parse().unwrap(),
            parts[1].parse().unwrap(),
        ),
        3 => (
            parts[0],
            parts[1].parse().unwrap(),
            parts[2].parse().unwrap(),
        ),
        _ => panic!("Invalid port specification"),
    };

    // Bind the listener to the address
    let listener = TcpListener::bind(format!("{}:{}", bind_address, bind_port))
        .await
        .unwrap();

    let socket = UdpSocket::bind("0.0.0.0:0").await.unwrap();

    let (socket, session) = p2p_handshake(socket, serial, args.relay).await;

    let (dh_tx, dh_rx) = mpsc::channel::<PTCPEvent>(128);
    let session = Arc::new(Mutex::new(session));

    let channels = Arc::new(Mutex::new(HashMap::<u32, mpsc::UnboundedSender<Vec<u8>>>::new()));
    let conn_channels = Arc::new(Mutex::new(HashMap::<u32, oneshot::Sender<bool>>::new()));

    println!("PTCP session established");

    /*
     * Clone the handles
     */

    let reader = Arc::new(socket);
    let writer = reader.clone();

    let session2 = session.clone();
    let channels2 = channels.clone();
    let conn_channels2 = conn_channels.clone();

    let hb_tx = dh_tx.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            hb_tx.send(PTCPEvent::Heartbeat).await.unwrap();
        }
    });

    tokio::spawn(async move {
        dh_writer(session, writer, dh_rx, remote_port.into()).await;
    });

    tokio::spawn(async move {
        dh_reader(session2, reader, channels, conn_channels).await;
    });

    println!("Ready to connect!");
    if remote_port == 554 {
        println!(
            "RTSP URL: rtsp://127.0.0.1{}/cam/realmonitor?channel=1&subtype=0",
            if bind_port != 554 {
                format!(":{}", bind_port)
            } else {
                String::new()
            }
        );
    }

    loop {
        // The second item contains the IP and port of the new connection.
        let (client, addr) = listener.accept().await.unwrap();
        println!("Accepted connection from {}", addr);

        // Create a channel for the client
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (conn_tx, conn_rx) = oneshot::channel::<bool>();
        let dh_tx = dh_tx.clone();

        let realm_id = rand::random::<u32>();

        // Store the channel in the map
        channels2.lock().unwrap().insert(realm_id, tx);
        conn_channels2.lock().unwrap().insert(realm_id, conn_tx);

        let channels3 = channels2.clone();
        let conn_channels3 = conn_channels2.clone();
        tokio::spawn(async move {
            if dh_tx.send(PTCPEvent::Connect(realm_id)).await.is_err() {
                return;
            }
            let ok = matches!(
                tokio::time::timeout(tokio::time::Duration::from_secs(15), conn_rx).await,
                Ok(Ok(true))
            );
            if !ok {
                println!("Realm {:08x}: device did not accept connection", realm_id);
                if CONNECT_FAILS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1 >= 2 {
                    println!("P2P session unusable; exiting so the supervisor reconnects");
                    std::process::exit(1);
                }
                channels3.lock().unwrap().remove(&realm_id);
                conn_channels3.lock().unwrap().remove(&realm_id);
                return;
            }

            CONNECT_FAILS.store(0, std::sync::atomic::Ordering::Relaxed);
            let (reader, writer) = client.into_split();
            tokio::spawn(process_writer(writer, rx));
            process_reader(reader, realm_id, dh_tx).await;
            channels3.lock().unwrap().remove(&realm_id);
        });
    }
}
