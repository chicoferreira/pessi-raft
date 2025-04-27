use maelstrom::MaelstromBody;
use std::time::Duration;

mod maelstrom;
mod raft;
mod transport;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = maelstrom::MaelstromServer::new();
    let (node_id, node_ids) = server.init().await?;

    let mut node = raft::Node::new(node_id, node_ids);
    let mut ticker = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            message = server.receive_message() => {
                let Ok(message) = message else {
                    eprintln!("Error receiving message");
                    continue;
                };

                let result = match message.body {
                    MaelstromBody::Raft(node_message) => {
                        node.handle_message(&server, node_message).await
                    }
                    MaelstromBody::Read { key, msg_id} => {
                        let request = transport::ClientRequest::Read { key, msg_id };
                        node.append_to_log(&server, message.src, request).await
                    }
                    MaelstromBody::Write { key, value, msg_id } => {
                        let request = transport::ClientRequest::Write { key, value, msg_id };
                        node.append_to_log(&server, message.src, request).await
                    }
                    MaelstromBody::Cas { key, from, to, msg_id } => {
                        let entry = transport::ClientRequest::Cas { key, from, to, msg_id };
                        node.append_to_log(&server, message.src, entry).await
                    }
                    _ => {
                        eprintln!("Unexpected message type: {:?}", message.body);
                        continue;
                    }
                };

                if let Err(e) = result {
                    eprintln!("Error handling message: {:?}", e);
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = node.tick_periodically(&server).await {
                    eprintln!("Error handling timeout: {:?}", e);
                }
            }
        }
    }
}
