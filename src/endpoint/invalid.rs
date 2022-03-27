use crate::MessageWsStream;
use crate::util::error::Result;

pub async fn accept(stream: MessageWsStream) -> Result<()> {
    stream.send(crate::scheduler::message::Message::error_message("Invalid endpoint.")).await?;
    stream.close().await?;
    Ok(())
}
