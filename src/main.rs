use std::time::Duration;

mod maelstrom;
mod raft;
mod transport;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut server = maelstrom::MaelstromServer::new();
    let (node_id, node_ids) = server.init().await?;

    let mut node = raft::Node::new(node_id, node_ids);
    let mut ticker = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            msg = server.receive_message() => {
                if let Err(e) = server.handle_message(msg, &mut node).await {
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
