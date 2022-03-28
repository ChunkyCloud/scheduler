mod endpoint;
mod scheduler;
mod util;

mod args;
mod backend;

use std::borrow::Borrow;
use log::*;
use std::net::SocketAddr;
use std::time::Duration;
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::try_join;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite;
use tungstenite::handshake::server::{Request, Response};
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::backend::Backend;
use util::error::{Error, Result};
use crate::scheduler::heap_scheduler::HeapSchedulerFactory;
use crate::util::queue;
use crate::util::websocket::{MessageWsStream, MessageWsStreamHandler};

async fn accept_connection(peer: SocketAddr, stream: TcpStream, backend: Backend) {
    if let Err(e) = handle_connection(peer, stream, backend).await {
        match e {
            Error::Ws(e) => {
                match e.borrow() {
                    tungstenite::Error::ConnectionClosed => (),
                    tungstenite::Error::Protocol(_) => (),
                    tungstenite::Error::Utf8 => (),
                    e => error!("Error processing connection: {:?}", e),
                }
            },
            Error::Serde(e) => {
                error!("Error while (de)serializing: {:?}", e);
            },
            Error::Queue(e) => {
                match e.borrow() {
                    queue::Error::Closed => (),
                    queue::Error::Full => error!("Full queue."),
                }
            }
            Error::Mongo(e) => {
                error!("MongoDB error: {:?}", e);
            }
            Error::Generic(e) => {
                error!("Error: {}", e);
            }
        }
    }
}

async fn handle_connection(peer: SocketAddr, stream: TcpStream, backend: Backend) -> Result<()> {
    let mut uri: Option<Uri> = None;

    match accept_hdr_async(stream, |req: &Request, response: Response| {
        uri = Some(req.uri().clone());
        Ok(response)
    }).await {
        Ok(ws_stream) => {
            let peer_id = Uuid::new_v4();
            let peer_target = peer_id.to_string();
            info!(target: &peer_target, "New WebSocket connection: {}", peer);

            let handler = MessageWsStreamHandler::new(ws_stream, &peer_target, &Duration::from_secs(30));
            match try_join!(handle_endpoint(peer_id, handler.message_stream(), uri, backend), handler.handle()) {
                Err(Error::Ws(e)) => {
                    info!(target: &peer_target, "Websocket error: {:?}", e);
                    return Err(Error::Ws(e));
                },
                Err(Error::Serde(e)) => {
                    error!(target: &peer_target, "Uncaught (de)serialization error: {:?}", e);
                    return Err(Error::Serde(e));
                }
                _ => {},
            }
        }
        Err(_) => {
            info!("Failed to accept connection for: {}", peer);
        }
    }
    Ok(())
}

async fn handle_endpoint(peer_id: Uuid, stream: MessageWsStream, uri: Option<Uri>, backend: Backend) -> Result<()> {
    match uri {
        None => {
            info!(target: stream.target(), "Connection had no path.");
            endpoint::invalid::accept(stream).await?;
        }
        Some(uri) => {
            let path = uri.path();
            info!(target: &peer_id.to_string(), "Connection to: {}", path);
            match path {
                "/apiserver" => endpoint::api_server::accept(peer_id, stream, uri, backend).await?,
                "/rendernode" => endpoint::rendernode::accept(peer_id, stream, uri, backend).await?,
                _ => endpoint::invalid::accept(stream).await?,
            }
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let cli: args::Args = args::Args::parse();

    pretty_env_logger::formatted_builder()
        .filter_level(match cli.verbose {
            0 => LevelFilter::Info,
            1 => LevelFilter::Debug,
            _ => LevelFilter::Trace,
        })
        .init();

    if cli.admin_key.is_none() {
        warn!("Admin key is empty!");
    }

    if cli.mongo.is_none() {
        warn!("Mongo URL is empty!");
    }

    let backend = Backend::new(cli.admin_key, cli.mongo,
                               HeapSchedulerFactory {}).await;

    let addr = "127.0.0.1:5700";
    let listener = TcpListener::bind(&addr)
        .await
        .expect(&*format!("Could not listen on: {}", addr));
    info!("Listening on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        match stream.peer_addr() {
            Ok(peer) => {
                tokio::spawn(accept_connection(peer, stream, backend.clone()));
            }
            Err(_) => {
                error!("Connected streams should have a peer address.");
            }
        }
    }
}
