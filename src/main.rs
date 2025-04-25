use crate::server::MessageSender;
use std::time::Duration;

mod maelstrom;
mod node;
mod server;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let server = maelstrom::MaelstromServer::new();
    let (node_id, node_ids) = server.init().await?;

    let mut node = node::Node::new(node_id, node_ids);
    let mut ticker = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            message = server.receive_message() => {
                let Ok(message) = message else {
                    eprintln!("Error receiving message");
                    continue;
                };

                match message.body {
                    maelstrom::MaelstromMessageBody::Node(node_message) => {
                        if let Err(e) = node.handle_message(&server, node_message).await {
                            eprintln!("Error handling message: {:?}", e);
                        }
                    }
                    _ => {
                        eprintln!("Unexpected message type: {:?}", message.body);
                    }
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
