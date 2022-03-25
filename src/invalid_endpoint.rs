use futures_util::{SinkExt};
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;
use crate::message;
use crate::error::Result;

pub async fn accept(mut stream: WebSocketStream<TcpStream>) -> Result<()> {
    let m = message::Message::error_message("Invalid Endpoint")
        .to_ws_message()?;
    stream.send(m).await?;
    Ok(())
}
