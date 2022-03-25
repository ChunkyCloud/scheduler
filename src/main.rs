mod rendernode;
mod invalid_endpoint;
mod message;
mod args;
mod state;
mod scheduler;
mod queue;
mod api_server;
mod error;

use std::borrow::Borrow;
use log::*;
use std::net::SocketAddr;
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_hdr_async, WebSocketStream};
use tokio_tungstenite::tungstenite;
use tungstenite::handshake::server::{Request, Response};
use tungstenite::http::Uri;
use uuid::Uuid;
use crate::state::Backend;
use crate::error::{Result, Error};

async fn accept_connection(peer: SocketAddr, stream: TcpStream, backend: Backend) {
    if let Err(e) = handle_connection(peer, stream, backend).await {
        match e {
            Error::WsError(e) => {
                match e.borrow() {
                    tungstenite::Error::ConnectionClosed => (),
                    tungstenite::Error::Protocol(_) => (),
                    tungstenite::Error::Utf8 => (),
                    e => error!("Error processing connection: {:?}", e),
                }
            },
            Error::SerdeError(e) => {
                error!("Error while (de)serializing: {:?}", e);
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

            match handle_endpoint(peer_id, peer_target.clone(), uri, ws_stream, backend).await {
                Err(Error::WsError(e)) => {
                    info!(target: &peer_target, "Websocket error: {:?}", e);
                    return Err(Error::WsError(e));
                },
                Err(Error::SerdeError(e)) => {
                    error!(target: &peer_target, "Uncaught (de)serialization error: {:?}", e);
                    return Err(Error::SerdeError(e));
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

async fn handle_endpoint(peer_id: Uuid, peer_target: String, uri: Option<Uri>, stream: WebSocketStream<TcpStream>, backend: Backend) -> Result<()> {
    match uri {
        None => {
            info!(target: &peer_target, "Connection had no path.");
            invalid_endpoint::accept(stream).await?;
        }
        Some(uri) => {
            let path = uri.path();
            info!(target: &peer_target, "Connection to: {}", path);
            match path {
                "/apiserver" => api_server::accept(peer_id, stream, uri, backend).await?,
                "/rendernode" => rendernode::accept(peer_id, stream, uri, backend).await?,
                _ => invalid_endpoint::accept(stream).await?,
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

    let backend = Backend::new(cli.admin_key);

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
