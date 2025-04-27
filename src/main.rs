use maelstrom::MaelstromMessageBody;
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

                let result = match message.body {
                    MaelstromMessageBody::Node(node_message) => {
                        node.handle_message(&server, node_message).await
                    }
                    MaelstromMessageBody::Read { key, msg_id} => {
                        let entry = server::LogEntryType::Read {
                            key,
                            msg_id,
                        };

                        node.append_to_log(&server, message.src, entry).await
                    }
                    MaelstromMessageBody::Write { key, value, msg_id } => {
                        let entry = server::LogEntryType::Write {
                            key,
                            value,
                            msg_id,
                        };

                        node.append_to_log(&server, message.src, entry).await
                    }
                    MaelstromMessageBody::Cas { key, from, to, msg_id } => {
                        let entry = server::LogEntryType::Cas {
                            key,
                            from,
                            to,
                            msg_id,
                        };

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
